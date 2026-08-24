# SSE Chat Lab

ห้องแชทสาธิตแบบเรียลไทม์ผ่าน Server-Sent Events (SSE) — ฝั่งเซิร์ฟเวอร์เขียนด้วย Go
ฝั่งหน้าเว็บเป็น HTML/CSS/JavaScript ล้วน ๆ (ไม่มี framework) ออกแบบมาให้รองรับ
ผู้ใช้เชื่อมต่อพร้อมกันได้ 100+ คนบนอินสแตนซ์เดียวสบาย ๆ

## วิธีรัน (แยกรัน backend กับ frontend คนละโปรเซส)

หน้าเว็บ (`web/`) ถูกฮาร์ดโค้ดให้คุยกับ API ที่ `http://localhost:9000`
และถูก serve เป็นไฟล์ static จาก **root ของโปรเจกต์** (เพื่อให้ path
`/web/style.css` และ `/web/app.js` resolve ได้ถูกต้อง) — ไม่ใช่จากข้างใน
โฟลเดอร์ `web/` เอง ดังนั้นต้องเปิด 2 เทอร์มินัล:

```bash
# เทอร์มินัลที่ 1 — backend API
go run . -addr :9000

# เทอร์มินัลที่ 2 — static file server สำหรับ frontend รันจาก root ของโปรเจกต์
python3 -m http.server 8000
```

เปิด http://localhost:8000/web/index.html แล้วตั้งชื่อผู้ใช้เพื่อเข้าห้องแชท
หน้าเว็บจะเชื่อมต่อผ่าน `EventSource` ไปยัง `localhost:9000` แสดงจำนวนคน
ออนไลน์แบบสด ๆ และมีข้อความแจ้งเตือนเมื่อมีคนเข้า/ออกห้อง ทุกข้อความที่พิมพ์
ส่งจะถูกกระจาย (broadcast) ไปยังทุกคนที่เชื่อมต่ออยู่ทันที

เนื่องจาก backend กับ frontend อยู่คนละ origin (คนละพอร์ต) เซิร์ฟเวอร์ Go จึงส่ง
`Access-Control-Allow-Origin: *` ในทุก response และตอบ CORS preflight
`OPTIONS` ให้ด้วย (ดู `withCORS` ใน `main.go`) — จำเป็นสำหรับให้
`fetch`/`EventSource` ทำงานข้าม origin ได้

ถ้าอยากรันทุกอย่างจากไบนารีเดียวบนพอร์ตเดียว ให้แก้ค่า `BACKEND_URL` ใน
`web/app.js` กลับเป็น same-origin (`""`) แล้วเปิด `http://localhost:9000/`
ได้เลย — หน้าเว็บถูก embed เข้าไปในไบนารี Go ผ่าน `go:embed` และ serve ที่ `/`
อยู่แล้ว

## หลักการที่ทำให้รองรับผู้ใช้ 100+ คนได้

- ใช้ 1 goroutine ต่อ 1 การเชื่อมต่อ SSE (`eventsHandler` ใน `main.go`) —
  goroutine ของ Go มีต้นทุนต่ำมาก (ไม่กี่ KB ต่ออัน) การเชื่อมต่อค้างไว้เฉย ๆ
  หลายร้อยตัวจึงแทบไม่เปลืองทรัพยากร
- `Hub` เก็บรายชื่อ client ทั้งหมดไว้ใน map ที่ป้องกันด้วย `sync.RWMutex`
  และกระจายข้อความไปยัง channel แบบ buffered ของแต่ละ client
- การส่งเข้า channel ของ client เป็นแบบ **non-blocking** (`select` พร้อม
  `default`) — ถ้า client คนไหนอ่านช้าหรือค้าง ข้อความของคนนั้นจะถูก drop
  ไปแทนที่จะทำให้การ broadcast ของทุกคนหยุดชะงัก
- มี heartbeat comment ทุก 15 วินาที เพื่อกันไม่ให้การเชื่อมต่อที่ idle
  (รวมถึง proxy/load balancer ด้านหน้า) ตัดการเชื่อมต่อเพราะ timeout
- ไม่ตั้งค่า `WriteTimeout` ให้ `http.Server` เพราะการเชื่อมต่อ SSE ตั้งใจ
  ให้เปิดค้างไว้ตลอด

## Endpoints

| Method | Path         | หน้าที่                                                        |
|--------|--------------|-----------------------------------------------------------------|
| GET    | `/events`    | SSE stream — เชื่อมต่อที่นี่ (รับ query `?user=ชื่อผู้ใช้`)         |
| POST   | `/broadcast` | `{"user": "...", "message": "..."}` → กระจายไปยังทุก client       |
| GET    | `/stats`     | `{"clients": N, "users": [...]}` จำนวนและรายชื่อผู้ใช้ที่ออนไลน์อยู่ |

แต่ละ event ที่ส่งผ่าน `/events` เป็น JSON รูปแบบ
`{"type": "join"|"leave"|"chat", "user", "message", "time", "clients"}`
ให้ฝั่งหน้าเว็บนำไปแสดงผลตาม `type`

## Load test: พิสูจน์ว่ารองรับผู้ใช้พร้อมกัน 100+ คนได้จริง

```bash
go run ./loadtest -n 150 -duration 20s
# ค่าเริ่มต้นชี้ไปที่ http://localhost:9000/events ใส่ -url เพื่อเปลี่ยนปลายทาง
```

คำสั่งนี้จะสร้าง client จริง 150 ตัว เปิดการเชื่อมต่อ `/events` ค้างไว้
20 วินาที แล้วรายงานว่าเชื่อมต่อสำเร็จกี่ตัว แต่ละตัวได้รับ event กี่ครั้ง
และเวลาที่ช้าที่สุดกว่าจะได้รับ event แรก ปรับแต่งได้ด้วย `-n`, `-duration`,
และ `-ramp-ms` (หน่วงเวลาระหว่างการสร้างแต่ละ client)

บน macOS/Linux ถ้าดันจำนวนการเชื่อมต่อพร้อมกันเกินสองสามร้อย อาจต้องเพิ่ม
ลิมิตของ open-file สำหรับโปรเซส load test:

```bash
ulimit -n 4096
```

## โครงสร้างโปรเจกต์

```
sse-chat-lab/
├── main.go           # HTTP server, SSE hub, endpoint สำหรับ broadcast/stats
├── web/               # หน้าเว็บ static (ถูก embed เข้าไปในไบนารี)
│   ├── index.html
│   ├── style.css
│   └── app.js
└── loadtest/
    └── main.go        # load test สำหรับทดสอบการเชื่อมต่อพร้อมกันจำนวนมาก
```
