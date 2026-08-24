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

// chatEvent คือ payload แบบ JSON ที่ส่งลงไปตาม SSE stream โดย Type จะเป็นค่าใดค่าหนึ่ง
// จาก "chat", "join", หรือ "leave" เพื่อให้ฝั่ง frontend เรนเดอร์แต่ละแบบต่างกันได้
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

// sanitizeName ตัดช่องว่างและจำกัดความยาวของชื่อที่แสดง โดยยังคงเป็นข้อความล้วน —
// ฝั่ง frontend เรนเดอร์ด้วย textContent ไม่ใช่ innerHTML ดังนั้นจึงไม่จำเป็นต้อง
// escape HTML ที่นี่
func sanitizeName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "ไม่ระบุชื่อ"
	}
	if len(s) > maxNameLen {
		s = s[:maxNameLen]
	}
	return s
}

// Client คือผู้สมัครสมาชิก SSE ที่เชื่อมต่ออยู่หนึ่งราย ประกอบด้วยคิวข้อความขาออก
// ของตัวเองและชื่อที่แสดงตอนเข้าร่วม
type Client struct {
	ch   chan string
	user string
}

// Hub คอยติดตามไคลเอนต์ SSE ที่เชื่อมต่ออยู่ทั้งหมดและกระจายข้อความ broadcast ไปยัง
// ทุกคน การใช้ channel แบบ buffered ต่อไคลเอนต์หนึ่งตัวร่วมกับการส่งแบบ non-blocking
// ช่วยไม่ให้ผู้อ่านที่ช้ารายหนึ่งทำให้พูลทั้งหมดสะดุด ซึ่งเป็นสิ่งที่ทำให้ระบบนี้
// รองรับได้เกิน 100 การเชื่อมต่อพร้อมกันบนโมเดลหนึ่ง goroutine ต่อหนึ่งการเชื่อมต่อ
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
			log.Printf("ไคลเอนต์ %d ทำงานช้า กำลังทิ้งข้อความ", id)
		}
	}
}

func (h *Hub) eventsHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "ไม่รองรับการสตรีม", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // ปิดการ buffer ของ proxy (nginx)
	w.Header().Set("Access-Control-Allow-Origin", "*")

	user := sanitizeName(r.URL.Query().Get("user"))
	id, ch := h.register(user)
	log.Printf("ไคลเอนต์ %d (%s) เชื่อมต่อแล้ว (รวม=%d)", id, user, h.count())

	defer func() {
		h.unregister(id)
		log.Printf("ไคลเอนต์ %d (%s) ตัดการเชื่อมต่อแล้ว (รวม=%d)", id, user, h.count())
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
		http.Error(w, "ไม่อนุญาตให้ใช้ method นี้", http.StatusMethodNotAllowed)
		return
	}
	var req broadcastReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "รูปแบบข้อมูลไม่ถูกต้อง: ต้องการ {\"user\": \"...\", \"message\": \"...\"}", http.StatusBadRequest)
		return
	}
	message := strings.TrimSpace(req.Message)
	if message == "" {
		http.Error(w, "ข้อความต้องไม่เป็นค่าว่าง", http.StatusBadRequest)
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

// withCORS ทำให้ frontend ถูกเสิร์ฟจาก origin/port ที่ต่างจาก API ได้ (เช่น
// static file server สำหรับ web/ ในขณะที่ไบนารีนี้ฟังที่ :9000) มันตอบคำขอ
// preflight OPTIONS และแนบ Access-Control-Allow-Origin ในทุกการตอบกลับ
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
	addr := flag.String("addr", ":9000", "ที่อยู่ที่จะให้เซิร์ฟเวอร์รับฟัง")
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
		// ไม่ตั้ง WriteTimeout: การเชื่อมต่อ SSE ถูกออกแบบให้ค้างไว้ได้ตลอด
	}

	log.Printf("sse-lab กำลังรับฟังที่ %s", *addr)
	log.Fatal(srv.ListenAndServe())
}
