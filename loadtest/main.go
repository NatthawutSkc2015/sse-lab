// Command loadtest opens N concurrent SSE connections against the sse-lab
// server and reports how many connected successfully, how long the first
// event took to arrive, and how many events each client received during the
// run. Use it to confirm the server holds up under 100+ simultaneous users.
//
//	go run ./loadtest -n 150 -url http://localhost:8080/events -duration 20s
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	connected   bool
	firstEvent  time.Duration
	eventsRecvd int64
	err         error
}

func runClient(ctx context.Context, url string, id int) result {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return result{err: err}
	}
	req.Header.Set("Accept", "text/event-stream")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return result{err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return result{err: fmt.Errorf("status %d", resp.StatusCode)}
	}

	res := result{connected: true}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	first := true
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			atomic.AddInt64(&res.eventsRecvd, 1)
			if first {
				res.firstEvent = time.Since(start)
				first = false
			}
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		res.err = err
	}
	return res
}

func main() {
	url := flag.String("url", "http://localhost:9000/events", "SSE endpoint to hit")
	n := flag.Int("n", 100, "number of concurrent simulated users")
	duration := flag.Duration("duration", 15*time.Second, "how long each client stays connected")
	rampMs := flag.Int("ramp-ms", 5, "delay between spawning each client, in milliseconds")
	flag.Parse()

	fmt.Printf("spawning %d clients against %s for %s\n", *n, *url, *duration)

	ctx, cancel := context.WithTimeout(context.Background(), *duration+5*time.Second)
	defer cancel()

	results := make([]result, *n)
	var wg sync.WaitGroup
	var connectedSoFar int64

	runCtx, stop := context.WithTimeout(ctx, *duration)
	defer stop()

	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = runClient(runCtx, *url, i)
			if results[i].connected {
				c := atomic.AddInt64(&connectedSoFar, 1)
				if c%25 == 0 {
					log.Printf("%d/%d connected so far", c, *n)
				}
			}
		}(i)
		if *rampMs > 0 {
			time.Sleep(time.Duration(*rampMs) * time.Millisecond)
		}
	}

	wg.Wait()

	var connected, failed int
	var totalEvents int64
	var maxFirst time.Duration
	for _, r := range results {
		if r.connected {
			connected++
			totalEvents += r.eventsRecvd
			if r.firstEvent > maxFirst {
				maxFirst = r.firstEvent
			}
		} else {
			failed++
			if r.err != nil {
				log.Printf("client failed: %v", r.err)
			}
		}
	}

	fmt.Println("\n--- results ---")
	fmt.Printf("connected:        %d/%d\n", connected, *n)
	fmt.Printf("failed:           %d\n", failed)
	fmt.Printf("total events:     %d\n", totalEvents)
	fmt.Printf("slowest first byte: %s\n", maxFirst)
}
