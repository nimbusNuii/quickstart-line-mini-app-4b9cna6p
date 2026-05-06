# LINE Mini App — Personalized Broadcast

ส่ง LINE message พร้อมลิ้งเฉพาะบุคคลให้ผู้ใช้แต่ละคน  
รองรับผู้ใช้ **13 ล้านคน** ด้วย SQLite + background jobs

---

## เปรียบเทียบ 4 Mode (สำหรับ 13 ล้านคน)

| | `broadcast` ⭐ | `narrowcast` | `multicast` | `push` |
|---|---|---|---|---|
| **API calls** | **1** | **9** | **26,000** | **13,000,000** |
| **เวลาโดยประมาณ** | **ทันที** | ~2-5 นาที* | ~2 นาที | หลายชั่วโมง |
| **ส่งหา** | ทุก follower | registered เท่านั้น | registered เท่านั้น | registered เท่านั้น |
| **ลิ้ง unique ใน message** | ❌ | ❌ | ❌ | ✅ |
| **Personalize** | via LIFF | via LIFF | via LIFF | ใน message |
| **Demographic filter** | ❌ | ✅ | ❌ | ❌ |
| **Server load** | ต่ำมาก | ต่ำ (LINE ส่งให้) | ปานกลาง | สูงมาก |

\* narrowcast: รวมเวลา upload audience + รอ LINE process (~1-3 นาที) + 9 narrowcast calls

**สรุปแนะนำ:**
- ต้องการส่งถึง **ทุก follower** และ personalize via LIFF → ใช้ **`broadcast`**
- ต้องการ **demographic filter** หรือส่งเฉพาะ registered users → ใช้ **`narrowcast`**
- ต้องการ **ลิ้ง unique ใน message เลย** (ไม่ผ่าน LIFF) → ใช้ **`push`** (ช้า)

---

## ทำไม Narrowcast ถึงดีสำหรับ 13 ล้าน

```
13M users ÷ 1,500,000 per audience = 9 audience groups

Upload phase: 13M ÷ 10,000 per batch = 1,300 API calls
              → parallel 50 concurrent = ~5 seconds

Wait phase:   LINE processes audiences async → ~1-3 minutes

Narrowcast:   9 sequential calls → ~18 seconds

Total: ~2-5 minutes  vs  Push: หลายชั่วโมง
```

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
                         broadcast:   1 call ───────────┤
                         narrowcast:  9 calls ──────────┤ (13M users)
                         multicast: 26K calls ──────────┤
                         push:       13M calls ─────────┘

User clicks link → /landing → liff.getProfile() → GET /my-link → personalized content
```

---

## วิธี Setup

```bash
npm install
cp .env.example .env   # แก้ไข credentials
npm start
```

ตั้ง Webhook URL ใน LINE Developers Console:
```
https://your-server-domain.com/webhook
```

---

## API Endpoints

| Method | Path | คำอธิบาย |
|--------|------|-----------|
| `POST` | `/webhook` | รับ events จาก LINE |
| `POST` | `/register` | ลงทะเบียน userId จาก LIFF |
| `GET`  | `/my-link?userId=` | ดึงลิ้งเฉพาะบุคคล (เรียกจาก landing page) |
| `POST` | `/broadcast` | เริ่ม broadcast job (ตอบกลับทันที) |
| `GET`  | `/broadcast/status/:jobId` | ตรวจสอบ progress |
| `GET`  | `/broadcast/status` | ดู 20 jobs ล่าสุด |
| `GET`  | `/users?limit=&offset=` | ดูรายชื่อผู้ใช้ (paginated) |
| `GET`  | `/landing` | หน้า personalized (LIFF) |

### ตัวอย่าง: Broadcast (แนะนำ — 1 call)
```bash
curl -X POST https://your-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{"mode":"broadcast","message":"สวัสดี! ดูข้อมูลของคุณ: {link}"}'
```

### ตัวอย่าง: Narrowcast (registered users + LINE-side delivery)
```bash
curl -X POST https://your-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{"mode":"narrowcast","concurrency":50}'
```

### ตัวอย่าง: Push (ลิ้ง unique ใน message)
```bash
curl -X POST https://your-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{"mode":"push","message":"ลิ้งเฉพาะของคุณ: {link}"}'
```

Response (ทันที):
```json
{
  "jobId": "uuid-...",
  "mode": "narrowcast",
  "total": 13000000,
  "apiCalls": 9,
  "message": "Broadcast started — 9 narrowcast calls (via 9 audience groups)",
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
├── users.db         # SQLite database (gitignored)
├── .env.example     # Template credentials
└── package.json
```


[Edit in StackBlitz next generation editor ⚡️](https://stackblitz.com/~/github.com/nimbusNuii/quickstart-line-mini-app-4b9cna6p)