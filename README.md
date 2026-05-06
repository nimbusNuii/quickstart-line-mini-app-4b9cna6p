# LINE Mini App — Personalized Broadcast

ส่ง LINE Push Message พร้อมลิ้งเฉพาะบุคคลให้ผู้ใช้แต่ละคน  
รองรับผู้ใช้หลักล้านคนด้วย **SQLite + parallel batch sending + background job**

---

## Architecture

```
LIFF (index.html/js)
  └─ POST /register ──────────────────────────────► server.js
                                                        │
LINE Webhook (follow/message/unfollow)                  │
  └─ POST /webhook ───────────────────────────────►  SQLite (users.db)
                                                        │
Admin triggers broadcast                               │
  └─ POST /broadcast ─────────────────────────────► background job
       returns jobId immediately                        │
                                                 parallel batches (50/batch)
                                                        │
       GET /broadcast/status/:jobId ◄──────────── progress tracking
```

**ทำไมถึงรองรับหลักล้านคน:**
- **SQLite + WAL mode**: query/insert เร็ว, ไม่โหลดทุก row เข้า RAM
- **Streaming iterator**: วนผ่าน users ทีละ chunk ไม่ allocate memory ครั้งเดียวทั้งหมด
- **Parallel batches**: ส่ง 50 requests พร้อมกัน (ปรับได้), ไม่รอทีละคน
- **Background job**: `/broadcast` ตอบกลับทันที, ติดตาม progress ผ่าน `/broadcast/status/:jobId`

---

## วิธี Setup

### 1. ติดตั้ง dependencies

```bash
npm install
```

### 2. ตั้งค่า Environment Variables

```bash
cp .env.example .env
```

แก้ไข `.env`:

```
LINE_CHANNEL_SECRET=<Channel Secret จาก LINE Developers>
LINE_CHANNEL_ACCESS_TOKEN=<Channel Access Token>
LIFF_ID=<LIFF ID>
BASE_URL=https://your-server-domain.com
PORT=3000
```

### 3. รัน Server

```bash
npm start
```

### 4. ตั้ง Webhook URL ใน LINE Developers Console

```
https://your-server-domain.com/webhook
```

---

## API Endpoints

| Method | Path | คำอธิบาย |
|--------|------|-----------|
| `POST` | `/webhook` | รับ events จาก LINE (follow, message, unfollow) |
| `POST` | `/register` | ลงทะเบียน userId จาก LIFF และรับลิ้งส่วนตัว |
| `POST` | `/broadcast` | เริ่ม broadcast job (ตอบกลับทันที พร้อม jobId) |
| `GET`  | `/broadcast/status/:jobId` | ตรวจสอบ progress ของ broadcast job |
| `GET`  | `/users?limit=100&offset=0` | ดูรายชื่อผู้ใช้แบบ paginated |
| `GET`  | `/landing?ref=&uid=` | หน้าปลายทางของลิ้งเฉพาะบุคคล |

### ตัวอย่าง: ส่ง Broadcast

```bash
curl -X POST https://your-server-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "baseUrl": "https://your-server-domain.com",
    "message": "สวัสดี! นี่คือลิ้งเฉพาะสำหรับคุณ: {link}",
    "concurrency": 50
  }'
```

Response (ทันที):
```json
{
  "jobId": "uuid-...",
  "total": 1500000,
  "message": "Broadcast started for 1,500,000 users",
  "statusUrl": "/broadcast/status/uuid-..."
}
```

### ตรวจสอบ Progress

```bash
curl https://your-server-domain.com/broadcast/status/<jobId>
```

```json
{
  "jobId": "uuid-...",
  "status": "completed",
  "total": 1500000,
  "sent": 1499873,
  "failed": 127,
  "startedAt": "2026-05-06T10:00:00.000Z",
  "finishedAt": "2026-05-06T10:47:22.000Z"
}
```

`{link}` จะถูกแทนด้วยลิ้งเฉพาะของแต่ละคน เช่น  
`https://your-server-domain.com/landing?ref=<uuid>&uid=<userId>`

---

## โครงสร้างไฟล์

```
├── server.js        # Express backend
├── index.html       # LIFF frontend
├── index.js         # LIFF JavaScript
├── style.css        # Styles
├── users.db         # SQLite database (ถูก gitignore)
├── .env.example     # Template ตัวแปรสภาพแวดล้อม
└── package.json
```


[Edit in StackBlitz next generation editor ⚡️](https://stackblitz.com/~/github.com/nimbusNuii/quickstart-line-mini-app-4b9cna6p)