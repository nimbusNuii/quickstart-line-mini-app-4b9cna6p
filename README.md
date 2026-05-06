# LINE Mini App — Personalized Broadcast

ส่ง LINE message พร้อมลิ้งเฉพาะบุคคลให้ผู้ใช้แต่ละคน  
รองรับผู้ใช้หลักล้านคนด้วย **SQLite + background jobs**

---

## เปรียบเทียบ 3 Mode

| | `push` | `multicast` | `broadcast` ⭐ |
|---|---|---|---|
| LINE API calls (1M คน) | **1,000,000** | **2,000** | **1** |
| เวลาโดยประมาณ | ~67 นาที | ~8 วิ | **ทันที** |
| ส่งหา | เฉพาะ registered | เฉพาะ registered | **ทุก follower** |
| ลิ้งใน message | ✅ unique ต่อคน | ❌ URL เดียวกัน | ❌ URL เดียวกัน |
| Personalize | ใน message | via LIFF landing | via LIFF landing |

**แนะนำ:** ใช้ `broadcast` — 1 call เดียว ทุกคนรับ URL เดียวกัน พอคลิกเข้า landing page แล้ว LIFF จะ `getProfile()` เพื่อแสดงข้อมูลเฉพาะบุคคลจาก DB

---

## Architecture

```
LIFF (index.html/js)
  └─ POST /register ──────────────────────────────► server.js
                                                        │
LINE Webhook (follow/message/unfollow)                  │
  └─ POST /webhook ───────────────────────────────►  SQLite (users.db)
                                                        │
Admin triggers broadcast                                │
  └─ POST /broadcast { mode } ───────────────────► background job
       returns jobId immediately                        │
                              broadcast: 1 call ────────┤
                              multicast: 2K calls ──────┤
                              push: 1M calls ───────────┘

User clicks link → /landing → liff.getProfile() → GET /my-link → personalized content
```

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
| `POST` | `/register` | ลงทะเบียน userId จาก LIFF |
| `GET`  | `/my-link?userId=` | ดึงลิ้งเฉพาะบุคคล (เรียกจาก landing page) |
| `POST` | `/broadcast` | เริ่ม broadcast job (ตอบกลับทันที พร้อม jobId) |
| `GET`  | `/broadcast/status/:jobId` | ตรวจสอบ progress |
| `GET`  | `/broadcast/status` | ดู 20 jobs ล่าสุด |
| `GET`  | `/users?limit=&offset=` | ดูรายชื่อผู้ใช้แบบ paginated |
| `GET`  | `/landing` | หน้า personalized (LIFF) |

### ตัวอย่าง: ส่ง Broadcast (แนะนำ — 1 API call)

```bash
curl -X POST https://your-server-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "broadcast",
    "message": "สวัสดี! แตะลิ้งนี้เพื่อดูข้อมูลเฉพาะของคุณ: {link}"
  }'
```

### ตัวอย่าง: ส่ง Multicast (เฉพาะ registered users)

```bash
curl -X POST https://your-server-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "multicast",
    "concurrency": 50
  }'
```

### ตัวอย่าง: ส่ง Push (ลิ้ง unique ใน message)

```bash
curl -X POST https://your-server-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "push",
    "message": "ลิ้งเฉพาะของคุณ: {link}"
  }'
```

Response (ทันที):
```json
{
  "jobId": "uuid-...",
  "mode": "broadcast",
  "total": 1500000,
  "apiCalls": 1,
  "message": "Broadcast started — 1 broadcast call (all followers)",
  "statusUrl": "/broadcast/status/uuid-..."
}
```

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