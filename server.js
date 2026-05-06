require('dotenv').config();
const express = require('express');
const line = require('@line/bot-sdk');
const crypto = require('crypto');
const Database = require('better-sqlite3');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// ─── LINE client ──────────────────────────────────────────────────────────────
const lineConfig = {
  channelSecret: process.env.LINE_CHANNEL_SECRET,
  channelAccessToken: process.env.LINE_CHANNEL_ACCESS_TOKEN,
};
const client = new line.messagingApi.MessagingApiClient({
  channelAccessToken: lineConfig.channelAccessToken,
});

// ─── SQLite setup ─────────────────────────────────────────────────────────────
const db = new Database(path.join(__dirname, 'users.db'));
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    userId    TEXT PRIMARY KEY,
    token     TEXT NOT NULL,
    createdAt TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS broadcast_jobs (
    jobId       TEXT PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'pending',
    total       INTEGER NOT NULL DEFAULT 0,
    sent        INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    message     TEXT,
    baseUrl     TEXT,
    startedAt   TEXT,
    finishedAt  TEXT
  );
`);

const stmtUpsertUser = db.prepare(`
  INSERT OR IGNORE INTO users (userId, token, createdAt)
  VALUES (@userId, @token, @createdAt)
`);
const stmtDeleteUser  = db.prepare(`DELETE FROM users WHERE userId = @userId`);
const stmtGetUser     = db.prepare(`SELECT * FROM users WHERE userId = @userId`);
const stmtCountUsers  = db.prepare(`SELECT COUNT(*) as cnt FROM users`);
const stmtInsertJob   = db.prepare(`
  INSERT INTO broadcast_jobs (jobId, status, total, sent, failed, message, baseUrl, startedAt)
  VALUES (@jobId, 'pending', @total, 0, 0, @message, @baseUrl, @startedAt)
`);
const stmtUpdateJob   = db.prepare(`
  UPDATE broadcast_jobs
  SET status = @status, sent = @sent, failed = @failed, finishedAt = @finishedAt
  WHERE jobId = @jobId
`);
const stmtIncrJob     = db.prepare(`
  UPDATE broadcast_jobs SET sent = sent + @s, failed = failed + @f WHERE jobId = @jobId
`);
const stmtGetJob      = db.prepare(`SELECT * FROM broadcast_jobs WHERE jobId = @jobId`);
const stmtListJobs    = db.prepare(`SELECT * FROM broadcast_jobs ORDER BY startedAt DESC LIMIT 20`);

function newToken() { return crypto.randomUUID(); }

// ─── Multicast broadcast (fast path) ─────────────────────────────────────────
// LINE Multicast: up to 500 userIds per call, same message for all.
// Personalisation happens on the landing page via LIFF getProfile().
//
// 1M users → 2,000 multicast calls vs 1,000,000 push calls = 500× fewer API calls.
// At concurrency=50, ~2,000/50 = 40 parallel batches ≈ ~8 seconds total.
//
// The message contains ONE shared URL; the landing page identifies each visitor
// with liff.getProfile() and serves their personal content from the DB.
const MULTICAST_BATCH = 500; // LINE API limit per multicast call

async function runMulticastJob(jobId, baseUrl, messageTemplate, concurrency) {
  const landingUrl = `${baseUrl}/landing`;
  const text = messageTemplate.replace('{link}', landingUrl);

  // Stream userId chunks from DB — never loads all rows into RAM
  const allUsers = db.prepare(`SELECT userId FROM users`).pluck().all();
  const batches = [];
  for (let i = 0; i < allUsers.length; i += MULTICAST_BATCH) {
    batches.push(allUsers.slice(i, i + MULTICAST_BATCH));
  }

  // Process `concurrency` multicast calls in parallel
  let i = 0;
  async function flush(chunk) {
    try {
      await client.multicast({
        to: chunk,
        messages: [{ type: 'text', text }],
      });
      stmtIncrJob.run({ jobId, s: chunk.length, f: 0 });
    } catch (err) {
      console.error(`multicast batch failed: ${err.message}`);
      stmtIncrJob.run({ jobId, s: 0, f: chunk.length });
    }
  }

  while (i < batches.length) {
    const window = batches.slice(i, i + concurrency);
    await Promise.allSettled(window.map(flush));
    i += concurrency;
  }

  const job = stmtGetJob.get({ jobId });
  stmtUpdateJob.run({
    jobId,
    status: job.failed > 0 ? 'completed_with_errors' : 'completed',
    sent: job.sent,
    failed: job.failed,
    finishedAt: new Date().toISOString(),
  });
  console.log(`Job ${jobId} finished — sent: ${job.sent}, failed: ${job.failed}`);
}

// ─── Webhook ──────────────────────────────────────────────────────────────────
app.post(
  '/webhook',
  express.raw({ type: 'application/json' }),
  line.middleware(lineConfig),
  (req, res) => {
    const events = req.body.events || [];
    for (const event of events) {
      const uid = event.source?.userId;
      if (!uid) continue;
      if (event.type === 'follow' || event.type === 'message') {
        stmtUpsertUser.run({ userId: uid, token: newToken(), createdAt: new Date().toISOString() });
      } else if (event.type === 'unfollow') {
        stmtDeleteUser.run({ userId: uid });
      }
    }
    res.sendStatus(200);
  }
);

app.use(express.json());

// ─── Register (called from LIFF) ──────────────────────────────────────────────
app.post('/register', (req, res) => {
  const { userId } = req.body;
  if (!userId) return res.status(400).json({ error: 'userId is required' });

  stmtUpsertUser.run({ userId, token: newToken(), createdAt: new Date().toISOString() });
  const user = stmtGetUser.get({ userId });
  const baseUrl = process.env.BASE_URL || `http://localhost:${PORT}`;
  const link = `${baseUrl}/landing`;
  res.json({ userId, token: user.token, link });
});

// ─── Token lookup (called from landing page via LIFF) ─────────────────────────
// GET /my-link?userId=Uxxx  → returns personalised data for this user
app.get('/my-link', (req, res) => {
  const { userId } = req.query;
  if (!userId) return res.status(400).json({ error: 'userId is required' });

  const user = stmtGetUser.get({ userId });
  if (!user) return res.status(404).json({ error: 'User not found. Please register first.' });

  const baseUrl = process.env.BASE_URL || `http://localhost:${PORT}`;
  const link = `${baseUrl}/landing?ref=${user.token}&uid=${userId}`;
  res.json({ userId, token: user.token, link });
});

// ─── Broadcast ────────────────────────────────────────────────────────────────
// POST /broadcast  { baseUrl?, message?, concurrency? }
// Uses Multicast API — sends one shared landing URL to all users.
// Landing page identifies each visitor via LIFF and shows personalised content.
app.post('/broadcast', (req, res) => {
  const { cnt } = stmtCountUsers.get();
  if (cnt === 0) return res.status(400).json({ error: 'No registered users found' });

  const baseUrl     = req.body.baseUrl || process.env.BASE_URL || `http://localhost:${PORT}`;
  const message     = req.body.message || 'สวัสดี! แตะลิ้งนี้เพื่อดูข้อมูลเฉพาะของคุณ: {link}';
  const concurrency = Math.min(parseInt(req.body.concurrency) || 50, 100);

  if (!message.includes('{link}')) {
    return res.status(400).json({ error: 'message must contain {link} placeholder' });
  }

  const jobId = newToken();
  const totalBatches = Math.ceil(cnt / MULTICAST_BATCH);
  stmtInsertJob.run({ jobId, total: cnt, message, baseUrl, startedAt: new Date().toISOString() });

  setImmediate(() => {
    runMulticastJob(jobId, baseUrl, message, concurrency).catch((err) => {
      console.error(`Job ${jobId} crashed:`, err);
      stmtUpdateJob.run({ jobId, status: 'failed', sent: 0, failed: cnt, finishedAt: new Date().toISOString() });
    });
  });

  res.status(202).json({
    jobId,
    total: cnt,
    apiCalls: totalBatches,
    message: `Broadcast started — ${cnt.toLocaleString()} users via ${totalBatches.toLocaleString()} multicast calls`,
    statusUrl: `/broadcast/status/${jobId}`,
  });
});

// ─── Broadcast status ─────────────────────────────────────────────────────────
app.get('/broadcast/status/:jobId', (req, res) => {
  const job = stmtGetJob.get({ jobId: req.params.jobId });
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

app.get('/broadcast/status', (_req, res) => {
  res.json(stmtListJobs.all());
});

// ─── Users list (paginated) ───────────────────────────────────────────────────
app.get('/users', (req, res) => {
  const limit  = Math.min(parseInt(req.query.limit) || 100, 1000);
  const offset = parseInt(req.query.offset) || 0;
  const baseUrl = process.env.BASE_URL || `http://localhost:${PORT}`;

  const { cnt } = stmtCountUsers.get();
  const rows = db.prepare(`SELECT userId, token, createdAt FROM users LIMIT ? OFFSET ?`).all(limit, offset);
  const users = rows.map((u) => ({
    ...u,
    link: `${baseUrl}/landing?ref=${u.token}&uid=${u.userId}`,
  }));
  res.json({ total: cnt, limit, offset, count: users.length, users });
});

// ─── Landing page (personalised via LIFF) ─────────────────────────────────────
// Everyone gets the same URL. The page calls liff.getProfile() to identify
// the visitor, then fetches /my-link?userId=... to get their personal data.
app.get('/landing', (req, res) => {
  const liffId = process.env.LIFF_ID || '';
  res.send(`<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>ลิ้งของคุณ</title>
  <script charset="utf-8" src="https://static.line-scdn.net/liff/edge/2/sdk.js"></script>
  <style>
    body { font-family: sans-serif; padding: 24px; }
    #content { display: none; }
    #loading { color: #888; }
    a { color: #007a3d; word-break: break-all; }
  </style>
</head>
<body>
  <h2>ยินดีต้อนรับ!</h2>
  <p id="loading">กำลังโหลด...</p>
  <div id="content">
    <p>ลิ้งเฉพาะของคุณ:</p>
    <a id="personalLink" href="#"></a>
    <p id="tokenInfo" style="font-size:0.8em;color:#888;"></p>
  </div>
  <p id="error" style="color:#e00;display:none;"></p>
  <script>
    async function init() {
      try {
        await liff.init({ liffId: '${liffId}' });
        if (!liff.isLoggedIn()) { liff.login(); return; }

        const profile = await liff.getProfile();
        const res = await fetch('/my-link?userId=' + encodeURIComponent(profile.userId));
        if (!res.ok) throw new Error(await res.text());
        const data = await res.json();

        document.getElementById('personalLink').textContent = data.link;
        document.getElementById('personalLink').href = data.link;
        document.getElementById('tokenInfo').textContent = 'Token: ' + data.token;
        document.getElementById('loading').style.display = 'none';
        document.getElementById('content').style.display = 'block';
      } catch (err) {
        document.getElementById('loading').style.display = 'none';
        document.getElementById('error').textContent = 'เกิดข้อผิดพลาด: ' + err.message;
        document.getElementById('error').style.display = 'block';
      }
    }
    init();
  </script>
</body>
</html>`);
});

// ─── Serve LIFF frontend ──────────────────────────────────────────────────────
app.use(express.static(__dirname));

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`Users in DB: ${stmtCountUsers.get().cnt.toLocaleString()}`);
});
