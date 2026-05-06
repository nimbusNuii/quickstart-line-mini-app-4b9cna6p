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
// Handles millions of users efficiently with indexed lookups and streaming queries.
const db = new Database(path.join(__dirname, 'users.db'));
db.pragma('journal_mode = WAL');  // Better write concurrency
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

// Prepared statements — compiled once, reused for every row
const stmtUpsertUser  = db.prepare(`
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

// ─── Helpers ──────────────────────────────────────────────────────────────────
function newToken() {
  return crypto.randomUUID();
}

/**
 * Send push messages in parallel batches.
 * Processes CHUNK_SIZE users concurrently; streams from DB to avoid loading
 * millions of rows into memory at once.
 *
 * @param {string} jobId
 * @param {string} baseUrl
 * @param {string} messageTemplate  - must contain "{link}"
 * @param {number} concurrency      - parallel requests in-flight at once
 */
async function runBroadcastJob(jobId, baseUrl, messageTemplate, concurrency = 50) {
  // Stream all users via an iterator — never holds the whole table in RAM
  const iter = db.prepare(`SELECT userId, token FROM users`).iterate();

  let pending = [];

  async function flush() {
    // Fire up to `concurrency` requests in parallel
    await Promise.allSettled(
      pending.map(async ({ userId, token }) => {
        const link = `${baseUrl}/landing?ref=${token}&uid=${userId}`;
        const text = messageTemplate.replace('{link}', link);
        try {
          await client.pushMessage({ to: userId, messages: [{ type: 'text', text }] });
          stmtIncrJob.run({ jobId, s: 1, f: 0 });
        } catch (err) {
          console.error(`push failed for ${userId}: ${err.message}`);
          stmtIncrJob.run({ jobId, s: 0, f: 1 });
        }
      })
    );
    pending = [];
  }

  for (const row of iter) {
    pending.push(row);
    if (pending.length >= concurrency) {
      await flush();
    }
  }
  if (pending.length > 0) await flush();

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
  const link = `${baseUrl}/landing?ref=${user.token}&uid=${userId}`;
  res.json({ userId, token: user.token, link });
});

// ─── Broadcast ────────────────────────────────────────────────────────────────
// POST /broadcast  { baseUrl?, message?, concurrency? }
// Returns immediately with a jobId; processing runs in background.
app.post('/broadcast', (req, res) => {
  const { cnt } = stmtCountUsers.get();
  if (cnt === 0) return res.status(400).json({ error: 'No registered users found' });

  const baseUrl     = req.body.baseUrl || process.env.BASE_URL || `http://localhost:${PORT}`;
  const message     = req.body.message || 'สวัสดี! นี่คือลิ้งเฉพาะสำหรับคุณ: {link}';
  const concurrency = Math.min(parseInt(req.body.concurrency) || 50, 200); // max 200

  if (!message.includes('{link}')) {
    return res.status(400).json({ error: 'message must contain {link} placeholder' });
  }

  const jobId = newToken();
  stmtInsertJob.run({ jobId, total: cnt, message, baseUrl, startedAt: new Date().toISOString() });

  // Fire-and-forget background processing
  setImmediate(() => {
    runBroadcastJob(jobId, baseUrl, message, concurrency).catch((err) => {
      console.error(`Job ${jobId} crashed:`, err);
      stmtUpdateJob.run({ jobId, status: 'failed', sent: 0, failed: cnt, finishedAt: new Date().toISOString() });
    });
  });

  res.status(202).json({
    jobId,
    total: cnt,
    message: `Broadcast started for ${cnt.toLocaleString()} users`,
    statusUrl: `/broadcast/status/${jobId}`,
  });
});

// ─── Broadcast status ─────────────────────────────────────────────────────────
app.get('/broadcast/status/:jobId', (req, res) => {
  const job = stmtGetJob.get({ jobId: req.params.jobId });
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

// ─── Users list (admin, paginated) ───────────────────────────────────────────
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

// ─── Landing page ─────────────────────────────────────────────────────────────
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
  console.log(`Users in DB: ${stmtCountUsers.get().cnt.toLocaleString()}`);
});
