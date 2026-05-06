# LINE Mini App — Personalized Broadcast

ส่ง LINE Push Message พร้อมลิ้งเฉพาะบุคคลให้ผู้ใช้แต่ละคน

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
| `POST` | `/broadcast` | ส่ง push message พร้อมลิ้งเฉพาะบุคคลให้ทุกคน |
| `GET`  | `/users` | ดูรายชื่อผู้ใช้และลิ้งของแต่ละคน |
| `GET`  | `/landing?ref=&uid=` | หน้าปลายทางของลิ้งเฉพาะบุคคล |

### ตัวอย่าง: ส่ง Broadcast

```bash
curl -X POST https://your-server-domain.com/broadcast \
  -H "Content-Type: application/json" \
  -d '{
    "baseUrl": "https://your-server-domain.com",
    "message": "สวัสดี! นี่คือลิ้งเฉพาะสำหรับคุณ: {link}"
  }'
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
├── users.json       # ที่เก็บ userId (ถูก gitignore)
├── .env.example     # Template ตัวแปรสภาพแวดล้อม
└── package.json
```


[Edit in StackBlitz next generation editor ⚡️](https://stackblitz.com/~/github.com/nimbusNuii/quickstart-line-mini-app-4b9cna6p)