// คำสั่ง loadtest จะเปิดการเชื่อมต่อ SSE พร้อมกัน N รายการไปยังเซิร์ฟเวอร์ sse-lab
// และรายงานว่ามีการเชื่อมต่อสำเร็จกี่รายการ ใช้เวลานานเท่าไหร่กว่าอีเวนต์แรกจะมาถึง
// และแต่ละไคลเอนต์ได้รับอีเวนต์กี่รายการระหว่างการทำงาน ใช้เพื่อยืนยันว่าเซิร์ฟเวอร์
// รองรับผู้ใช้พร้อมกัน 100 คนขึ้นไปได้ นอกจากนี้ยังสามารถให้แต่ละไคลเอนต์จำลอง
// การส่งข้อความแชทไปที่ /broadcast ระหว่างที่เชื่อมต่ออยู่ได้ด้วย -send-every
//
//	go run ./loadtest -n 150 -url http://localhost:8080/events -duration 20s
//	go run ./loadtest -n 150 -url http://localhost:8080/events -duration 20s -send-every 2s
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	sent        int64
	sendErrs    int64
	err         error
}

// broadcastReq ต้องมีโครงสร้างตรงกับ broadcastReq ฝั่งเซิร์ฟเวอร์ (main.go)
type broadcastReq struct {
	User    string `json:"user"`
	Message string `json:"message"`
}

// sendMessages ส่งข้อความแชทปลอมไปที่ broadcastURL ทุกช่วง interval จนกว่า ctx
// จะถูกยกเลิก ใช้จำลองพฤติกรรมผู้ใช้จริงที่พิมพ์ข้อความระหว่างเชื่อมต่อ SSE อยู่
func sendMessages(ctx context.Context, broadcastURL string, id int, interval time.Duration, res *result) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	n := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n++
			body, _ := json.Marshal(broadcastReq{
				User:    fmt.Sprintf("loadtest-%d", id),
				Message: fmt.Sprintf("ข้อความทดสอบจากไคลเอนต์ %d #%d", id, n),
			})
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, broadcastURL, bytes.NewReader(body))
			if err != nil {
				atomic.AddInt64(&res.sendErrs, 1)
				continue
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				atomic.AddInt64(&res.sendErrs, 1)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				atomic.AddInt64(&res.sent, 1)
			} else {
				atomic.AddInt64(&res.sendErrs, 1)
			}
		}
	}
}

func runClient(ctx context.Context, url, broadcastURL string, id int, sendEvery time.Duration) result {
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

	var wg sync.WaitGroup
	if sendEvery > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sendMessages(ctx, broadcastURL, id, sendEvery, &res)
		}()
	}

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

	wg.Wait()
	return res
}

func main() {
	url := flag.String("url", "http://localhost:9000/events", "endpoint SSE ที่จะยิงเข้าไป")
	n := flag.Int("n", 10, "จำนวนผู้ใช้จำลองที่ทำงานพร้อมกัน")
	duration := flag.Duration("duration", 15*time.Second, "ระยะเวลาที่แต่ละไคลเอนต์เชื่อมต่อค้างไว้")
	rampMs := flag.Int("ramp-ms", 5, "หน่วงเวลาระหว่างการสร้างไคลเอนต์แต่ละตัว หน่วยเป็นมิลลิวินาที")
	sendEvery := flag.Duration("send-every", 0, "ถ้ามากกว่า 0 แต่ละไคลเอนต์จะส่งข้อความแชทไปที่ broadcast endpoint ด้วยความถี่นี้ (0 = ปิดใช้งาน)")
	broadcastURL := flag.String("broadcast-url", "", "endpoint สำหรับส่งข้อความแชท (ค่าเริ่มต้น: แทนที่ /events ด้วย /broadcast ใน -url)")
	flag.Parse()

	bURL := *broadcastURL
	if bURL == "" {
		bURL = strings.Replace(*url, "/events", "/broadcast", 1)
	}

	fmt.Printf("กำลังสร้างไคลเอนต์ %d ตัว ยิงไปที่ %s เป็นเวลา %s\n", *n, *url, *duration)
	if *sendEvery > 0 {
		fmt.Printf("แต่ละไคลเอนต์จะส่งข้อความไปที่ %s ทุก %s\n", bURL, *sendEvery)
	}

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
			results[i] = runClient(runCtx, *url, bURL, i, *sendEvery)
			if results[i].connected {
				c := atomic.AddInt64(&connectedSoFar, 1)
				if c%25 == 0 {
					log.Printf("เชื่อมต่อแล้ว %d/%d", c, *n)
				}
			}
		}(i)
		if *rampMs > 0 {
			time.Sleep(time.Duration(*rampMs) * time.Millisecond)
		}
	}

	wg.Wait()

	var connected, failed int
	var totalEvents, totalSent, totalSendErrs int64
	var maxFirst time.Duration
	for _, r := range results {
		if r.connected {
			connected++
			totalEvents += r.eventsRecvd
			totalSent += r.sent
			totalSendErrs += r.sendErrs
			if r.firstEvent > maxFirst {
				maxFirst = r.firstEvent
			}
		} else {
			failed++
			if r.err != nil {
				log.Printf("ไคลเอนต์ล้มเหลว: %v", r.err)
			}
		}
	}

	fmt.Println("\n--- ผลลัพธ์ ---")
	fmt.Printf("เชื่อมต่อสำเร็จ:      %d/%d\n", connected, *n)
	fmt.Printf("ล้มเหลว:             %d\n", failed)
	fmt.Printf("อีเวนต์ทั้งหมด:       %d\n", totalEvents)
	fmt.Printf("ไบต์แรกที่ช้าที่สุด:   %s\n", maxFirst)
	if *sendEvery > 0 {
		fmt.Printf("ข้อความที่ส่งสำเร็จ:  %d\n", totalSent)
		fmt.Printf("ข้อความที่ส่งล้มเหลว: %d\n", totalSendErrs)
	}
}
