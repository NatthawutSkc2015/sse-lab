package main

import (
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//go:embed web
var webFS embed.FS

const maxNameLen = 24
const maxMessageLen = 500

// chatEvent is the JSON payload pushed down the SSE stream. Type is one of
// "chat", "join", or "leave" so the frontend can render each differently.
type chatEvent struct {
	Type    string `json:"type"`
	User    string `json:"user"`
	Message string `json:"message,omitempty"`
	Time    string `json:"time"`
	Clients int    `json:"clients"`
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// sanitizeName trims and length-caps a display name. It stays plain text —
// the frontend renders it with textContent, never innerHTML, so no
// HTML-escaping is needed here.
func sanitizeName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "Anonymous"
	}
	if len(s) > maxNameLen {
		s = s[:maxNameLen]
	}
	return s
}

// Client is one connected SSE subscriber: their outbound message queue plus
// the display name they joined with.
type Client struct {
	ch   chan string
	user string
}

// Hub keeps track of every connected SSE client and fans out broadcasts to
// all of them. A per-client buffered channel plus a non-blocking send keeps
// one slow reader from stalling the rest of the pool, which is what lets
// this scale past 100 concurrent connections on a single goroutine-per-conn
// model.
type Hub struct {
	mu      sync.RWMutex
	clients map[uint64]*Client
	nextID  uint64
}

func newHub() *Hub {
	return &Hub{clients: make(map[uint64]*Client)}
}

func (h *Hub) register(user string) (uint64, chan string) {
	id := atomic.AddUint64(&h.nextID, 1)
	ch := make(chan string, 16)
	h.mu.Lock()
	h.clients[id] = &Client{ch: ch, user: user}
	h.mu.Unlock()
	return id, ch
}

func (h *Hub) unregister(id uint64) {
	h.mu.Lock()
	if c, ok := h.clients[id]; ok {
		delete(h.clients, id)
		close(c.ch)
	}
	h.mu.Unlock()
}

func (h *Hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func (h *Hub) broadcast(msg string) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for id, c := range h.clients {
		select {
		case c.ch <- msg:
		default:
			log.Printf("client %d is slow, dropping a message", id)
		}
	}
}

func (h *Hub) eventsHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	user := sanitizeName(r.URL.Query().Get("user"))
	id, ch := h.register(user)
	log.Printf("client %d (%s) connected (total=%d)", id, user, h.count())

	defer func() {
		h.unregister(id)
		log.Printf("client %d (%s) disconnected (total=%d)", id, user, h.count())
		h.broadcast(mustJSON(chatEvent{
			Type:    "leave",
			User:    user,
			Time:    time.Now().Format(time.RFC3339),
			Clients: h.count(),
		}))
	}()

	fmt.Fprintf(w, "event: connected\ndata: %s\n\n", mustJSON(map[string]any{"id": id, "user": user}))
	flusher.Flush()

	h.broadcast(mustJSON(chatEvent{
		Type:    "join",
		User:    user,
		Time:    time.Now().Format(time.RFC3339),
		Clients: h.count(),
	}))

	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "data: %s\n\n", msg)
			flusher.Flush()
		case <-heartbeat.C:
			fmt.Fprint(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}

type broadcastReq struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

func (h *Hub) broadcastHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req broadcastReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid body: expected {\"user\": \"...\", \"message\": \"...\"}", http.StatusBadRequest)
		return
	}
	message := strings.TrimSpace(req.Message)
	if message == "" {
		http.Error(w, "message must not be empty", http.StatusBadRequest)
		return
	}
	if len(message) > maxMessageLen {
		message = message[:maxMessageLen]
	}

	h.broadcast(mustJSON(chatEvent{
		Type:    "chat",
		User:    sanitizeName(req.User),
		Message: message,
		Time:    time.Now().Format(time.RFC3339),
		Clients: h.count(),
	}))
	w.WriteHeader(http.StatusNoContent)
}

func (h *Hub) onlineUsers() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	users := make([]string, 0, len(h.clients))
	for _, c := range h.clients {
		users = append(users, c.user)
	}
	return users
}

func (h *Hub) statsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(map[string]any{
		"clients": h.count(),
		"users":   h.onlineUsers(),
	})
}

// withCORS lets the frontend be served from a different origin/port than
// the API (e.g. a static file server for web/ while this binary listens on
// :9000). It answers preflight OPTIONS requests and tags every response
// with Access-Control-Allow-Origin.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	flag.Parse()

	hub := newHub()

	webRoot, err := fs.Sub(webFS, "web")
	if err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/events", hub.eventsHandler)
	mux.HandleFunc("/broadcast", hub.broadcastHandler)
	mux.HandleFunc("/stats", hub.statsHandler)
	mux.Handle("/", http.FileServer(http.FS(webRoot)))

	srv := &http.Server{
		Addr:              *addr,
		Handler:           withCORS(mux),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: SSE connections are meant to stay open.
	}

	log.Printf("sse-lab listening on %s", *addr)
	log.Fatal(srv.ListenAndServe())
}
