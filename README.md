# LINE Mini App — Personalized Broadcast

ส่ง LINE message พร้อมลิ้งเฉพาะบุคคลให้ผู้ใช้แต่ละคน  
รองรับผู้ใช้ **13 ล้านคน** ด้วย SQLite + background jobs

---

## เปรียบเทียบ 4 Mode (สำหรับ 13 ล้านคน)

| | `broadcast` | `narrowcast` (filter only) ⭐ | `narrowcast` (audience) | `multicast` | `push` |
|---|---|---|---|---|---|
| **API calls** | **1** | **1** | **9** | **26,000** | **13,000,000** |
| **เวลา** | ทันที | **ทันที** | ~2-5 นาที | ~2 นาที | หลายชั่วโมง |
| **ส่งหา** | ทุก follower | ทุก follower + filter | registered + filter | registered เท่านั้น | registered เท่านั้น |
| **demographic filter** | ❌ | ✅ gender/age/OS | ✅ gender/age/OS | ❌ | ❌ |
| **Personalize** | via LIFF | via LIFF | via LIFF | via LIFF | ใน message |

**⭐ แนะนำ: `narrowcast` + `filter`** — เร็วเท่า broadcast (1 call) แต่ filter ได้ด้วย

---

## Filter Fields (สำหรับ narrowcast)

| Field | Values |
|---|---|
| `gender` | `"male"` \| `"female"` |
| `ageMin` | `"age_15"` \| `"age_20"` \| `"age_25"` \| `"age_30"` \| `"age_35"` \| `"age_40"` \| `"age_45"` \| `"age_50"` |
| `ageMax` | เหมือน ageMin (exclusive upper bound) |
| `os` | `"ios"` \| `"android"` |

ใส่หลาย field = **AND** กันทั้งหมด

---

## ตัวอย่าง: เร็วสุด + filter ได้ (narrowcast, 1 call)

```bash
# ส่งเฉพาะผู้ชาย อายุ 20-35 ใช้ iOS
curl -X POST https://your-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "mode": "narrowcast",
    "message": "สวัสดี! ดูข้อมูลของคุณได้ที่นี่: {link}",
    "filter": {
      "gender": "male",
      "ageMin": "age_20",
      "ageMax": "age_35",
      "os": "ios"
    }
  }'
```

Response (ทันที — 1 API call):
```json
{
  "jobId": "uuid-...",
  "mode": "narrowcast",
  "total": 0,
  "apiCalls": 1,
  "filter": { "gender": "male", "ageMin": "age_20", "ageMax": "age_35", "os": "ios" },
  "message": "Broadcast started — 1 narrowcast call (demographic filter, instant)"
}
```

---

## ตัวอย่างอื่น

### Broadcast ทุกคน (เร็วสุด ไม่ filter)
```bash
curl -X POST https://your-domain.com/broadcast \
  -d '{"mode":"broadcast","message":"สวัสดี! ลิ้งของคุณ: {link}"}'
```

### Narrowcast ผู้ใช้ที่ register + กรอง iOS เท่านั้น (13M users → 9 calls + filter)
```bash
curl -X POST https://your-domain.com/broadcast \
  -d '{"mode":"narrowcast","filter":{"os":"ios"}}'
```

### Push (ลิ้ง unique อยู่ใน message เลย — ช้า)
```bash
curl -X POST https://your-domain.com/broadcast \
  -d '{"mode":"push","message":"ลิ้งเฉพาะของคุณ: {link}"}'
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
POST /broadcast { mode, filter } ───────────────► background job
       returns jobId immediately                        │
                    broadcast:              1 call ─────┤ (all followers)
                    narrowcast filter-only: 1 call ─────┤ ⭐ fast + filterable
                    narrowcast + audience:  9 calls ────┤ (13M, LINE delivers)
                    multicast:          26K calls ───────┤
                    push:               13M calls ───────┘

User clicks link → /landing → liff.getProfile() → GET /my-link → personalized content
```

---

## วิธี Setup

```bash
npm install
cp .env.example .env   # ใส่ credentials
npm start
```

ตั้ง Webhook URL: `https://your-domain.com/webhook`

---

## API Endpoints

| Method | Path | คำอธิบาย |
|--------|------|-----------|
| `POST` | `/webhook` | รับ events จาก LINE |
| `POST` | `/register` | ลงทะเบียน userId จาก LIFF |
| `GET`  | `/my-link?userId=` | ดึงลิ้งเฉพาะบุคคล |
| `POST` | `/broadcast` | เริ่ม broadcast job |
| `GET`  | `/broadcast/status/:jobId` | ตรวจสอบ progress |
| `GET`  | `/broadcast/status` | ดู 20 jobs ล่าสุด |
| `GET`  | `/users?limit=&offset=` | ดูรายชื่อผู้ใช้ (paginated) |
| `GET`  | `/landing` | หน้า personalized (LIFF) |

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