require('dotenv').config();
const express = require('express');
const line = require('@line/bot-sdk');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// LINE client config
const lineConfig = {
  channelSecret: process.env.LINE_CHANNEL_SECRET,
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
};
const client = new line.messagingApi.MessagingApiClient({
  channelAccessToken: lineConfig.channelAccessToken,
});

// Path to local user storage
const USERS_FILE = path.join(__dirname, 'users.json');

// Helper: load users from file
function loadUsers() {
  if (!fs.existsSync(USERS_FILE)) return {};
  try {
    return JSON.parse(fs.readFileSync(USERS_FILE, 'utf8'));
  } catch {
    return {};
  }
}

// Helper: save users to file
function saveUsers(users) {
  fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2), 'utf8');
}

// ─── Webhook ──────────────────────────────────────────────────────────────────
// LINE requires raw body for signature validation
app.post(
  '/webhook',
  express.raw({ type: 'application/json' }),
  line.middleware(lineConfig),
  (req, res) => {
    const events = req.body.events || [];
    const users = loadUsers();

    events.forEach((event) => {
      const uid = event.source && event.source.userId;
      if (!uid) return;

      // Register user on follow or message
      if (event.type === 'follow' || event.type === 'message') {
        if (!users[uid]) {
          users[uid] = { userId: uid, token: uuidv4(), createdAt: new Date().toISOString() };
          console.log(`Registered new user: ${uid}`);
        }
      }

      // Unfollow: remove user
      if (event.type === 'unfollow') {
        delete users[uid];
        console.log(`Removed user: ${uid}`);
      }
    });

    saveUsers(users);
    res.sendStatus(200);
  }
);

// Parse JSON for all other routes
app.use(express.json());

// ─── Register endpoint (called from LIFF frontend) ────────────────────────────
// POST /register  { userId }
app.post('/register', (req, res) => {
  const { userId } = req.body;
  if (!userId) return res.status(400).json({ error: 'userId is required' });

  const users = loadUsers();
  if (!users[userId]) {
    users[userId] = { userId, token: uuidv4(), createdAt: new Date().toISOString() };
    saveUsers(users);
    console.log(`Registered user via LIFF: ${userId}`);
  }

  const baseUrl = process.env.BASE_URL || `http://localhost:${PORT}`;
  const link = `${baseUrl}/landing?ref=${users[userId].token}&uid=${userId}`;
  res.json({ userId, token: users[userId].token, link });
});

// ─── Broadcast endpoint ───────────────────────────────────────────────────────
// POST /broadcast  { baseUrl?, message? }
// Sends a personalized link to every registered user via Push Message API.
app.post('/broadcast', async (req, res) => {
  const users = loadUsers();
  const userList = Object.values(users);

  if (userList.length === 0) {
    return res.status(400).json({ error: 'No registered users found' });
  }

  const baseUrl = req.body.baseUrl || process.env.BASE_URL || `http://localhost:${PORT}`;
  const messageTemplate = req.body.message || 'สวัสดี! นี่คือลิ้งเฉพาะสำหรับคุณ: {link}';

  const results = { success: [], failed: [] };

  for (const user of userList) {
    const link = `${baseUrl}/landing?ref=${user.token}&uid=${user.userId}`;
    const text = messageTemplate.replace('{link}', link);

    try {
      await client.pushMessage({
        to: user.userId,
        messages: [{ type: 'text', text }],
      });
      results.success.push(user.userId);
    } catch (err) {
      console.error(`Failed to send to ${user.userId}:`, err.message);
      results.failed.push({ userId: user.userId, error: err.message });
    }
  }

  res.json({
    sent: results.success.length,
    failed: results.failed.length,
    results,
  });
});

// ─── Users list endpoint (admin) ──────────────────────────────────────────────
app.get('/users', (req, res) => {
  const users = loadUsers();
  const baseUrl = process.env.BASE_URL || `http://localhost:${PORT}`;
  const list = Object.values(users).map((u) => ({
    ...u,
    link: `${baseUrl}/landing?ref=${u.token}&uid=${u.userId}`,
  }));
  res.json({ count: list.length, users: list });
});

// ─── Landing page (example personalized destination) ─────────────────────────
app.get('/landing', (req, res) => {
  const { ref, uid } = req.query;
  res.send(`
    <!DOCTYPE html>
    <html>
    <head><meta charset="utf-8"><title>Welcome</title></head>
    <body style="font-family:sans-serif;padding:24px">
      <h2>ยินดีต้อนรับ!</h2>
      <p>ลิ้งนี้สร้างเฉพาะสำหรับคุณ</p>
      <p><b>Token:</b> ${ref || '–'}</p>
      <p><b>User ID:</b> ${uid || '–'}</p>
    </body>
    </html>
  `);
});

// ─── Serve LIFF frontend ──────────────────────────────────────────────────────
app.use(express.static(__dirname));

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
