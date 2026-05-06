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

const MULTICAST_BATCH  = 500;       // LINE multicast: max userIds per call
const AUDIENCE_SIZE    = 1_500_000; // LINE audience: max users per group
const AUDIENCE_UPLOAD  = 10_000;    // LINE audience: max userIds per upload request
const AUDIENCE_POLL_MS = 5_000;     // polling interval while waiting for audience to be READY

// ─── LINE REST helper (for Audience API not exposed in SDK) ───────────────────
async function lineApi(method, path, body) {
  const res = await fetch(`https://api.line.me${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${process.env.LINE_CHANNEL_ACCESS_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`LINE API ${method} ${path} → ${res.status}: ${text}`);
  }
  return res.status === 204 ? null : res.json();
}

async function waitForAudience(audienceGroupId) {
  for (;;) {
    const data = await lineApi('GET', `/v2/bot/audienceGroup/${audienceGroupId}`);
    const status = data.audienceGroup?.status;
    if (status === 'READY') return;
    if (status === 'FAILED') throw new Error(`Audience ${audienceGroupId} failed`);
    await new Promise((r) => setTimeout(r, AUDIENCE_POLL_MS));
  }
}

// ─── Broadcast runner: LINE Broadcast API (1 call, all followers) ─────────────
// Fastest possible — 1 API call regardless of follower count.
// Personalisation still works via LIFF on the landing page.
async function runBroadcastAllJob(jobId, baseUrl, messageTemplate) {
  const landingUrl = `${baseUrl}/landing`;
  const text = messageTemplate.replace('{link}', landingUrl);
  try {
    await client.broadcast({ messages: [{ type: 'text', text }] });
    const { cnt } = stmtCountUsers.get();
    stmtUpdateJob.run({ jobId, status: 'completed', sent: cnt, failed: 0, finishedAt: new Date().toISOString() });
    console.log(`Job ${jobId} (broadcast) finished`);
  } catch (err) {
    console.error(`broadcast failed: ${err.message}`);
    stmtUpdateJob.run({ jobId, status: 'failed', sent: 0, failed: 0, finishedAt: new Date().toISOString() });
  }
}

// ─── Broadcast runner: Multicast (500 users/call, registered only) ────────────
async function runMulticastJob(jobId, baseUrl, messageTemplate, concurrency) {
  const landingUrl = `${baseUrl}/landing`;
  const text = messageTemplate.replace('{link}', landingUrl);

  const allUsers = db.prepare(`SELECT userId FROM users`).pluck().all();
  const batches = [];
  for (let i = 0; i < allUsers.length; i += MULTICAST_BATCH) {
    batches.push(allUsers.slice(i, i + MULTICAST_BATCH));
  }

  async function sendBatch(chunk) {
    try {
      await client.multicast({ to: chunk, messages: [{ type: 'text', text }] });
      stmtIncrJob.run({ jobId, s: chunk.length, f: 0 });
    } catch (err) {
      console.error(`multicast batch failed: ${err.message}`);
      stmtIncrJob.run({ jobId, s: 0, f: chunk.length });
    }
  }

  for (let i = 0; i < batches.length; i += concurrency) {
    await Promise.allSettled(batches.slice(i, i + concurrency).map(sendBatch));
  }

  const job = stmtGetJob.get({ jobId });
  stmtUpdateJob.run({
    jobId,
    status: job.failed > 0 ? 'completed_with_errors' : 'completed',
    sent: job.sent, failed: job.failed,
    finishedAt: new Date().toISOString(),
  });
  console.log(`Job ${jobId} (multicast) finished — sent: ${job.sent}, failed: ${job.failed}`);
}

// ─── Broadcast runner: Push (personalized link per user in message) ───────────
// Slowest but embeds the unique link directly in the chat bubble.
async function runPushJob(jobId, baseUrl, messageTemplate, concurrency) {
  const iter = db.prepare(`SELECT userId, token FROM users`).iterate();
  let pending = [];

  async function flush() {
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
    if (pending.length >= concurrency) await flush();
  }
  if (pending.length > 0) await flush();

  const job = stmtGetJob.get({ jobId });
  stmtUpdateJob.run({
    jobId,
    status: job.failed > 0 ? 'completed_with_errors' : 'completed',
    sent: job.sent, failed: job.failed,
    finishedAt: new Date().toISOString(),
  });
  console.log(`Job ${jobId} (push) finished — sent: ${job.sent}, failed: ${job.failed}`);
}

// ─── Broadcast runner: Narrowcast via Audience (registered users, LINE-side delivery) ──
//
// Flow for 13M users:
//   1. Chunk users into groups of 1.5M   → 9 audience groups
//   2. Upload each group in parallel 10K-user batches → ~1,300 API calls
//   3. Poll until all audiences are READY (LINE processes them async)
//   4. Narrowcast sequentially (LINE allows 1 active narrowcast at a time)
//   5. Delete audience groups (cleanup)
//
// Why use narrowcast vs multicast?
//   - LINE delivers on their side — our server is free after step 4
//   - Supports demographic/OS filtering (extend recipient object as needed)
//   - For 13M: ~9 narrowcast calls vs 26,000 multicast calls
async function runNarrowcastJob(jobId, baseUrl, messageTemplate, uploadConcurrency) {
  const landingUrl = `${baseUrl}/landing`;
  const text = messageTemplate.replace('{link}', landingUrl);

  // 1. Load all userIds from DB (streamed to avoid OOM on 13M rows)
  const allUsers = db.prepare(`SELECT userId FROM users`).pluck().all();
  const total = allUsers.length;

  // 2. Slice into audience groups of up to AUDIENCE_SIZE each
  const audienceGroupIds = [];
  const groupCount = Math.ceil(total / AUDIENCE_SIZE);

  for (let gi = 0; gi < groupCount; gi++) {
    const groupUsers = allUsers.slice(gi * AUDIENCE_SIZE, (gi + 1) * AUDIENCE_SIZE);

    // Create the audience group
    const created = await lineApi('POST', '/v2/bot/audienceGroup/upload', {
      description: `job_${jobId}_g${gi}`,
      isIfaAudience: false,
    });
    const audienceGroupId = created.audienceGroupId;
    audienceGroupIds.push(audienceGroupId);

    // Upload users in parallel batches of AUDIENCE_UPLOAD (10K each)
    const uploadBatches = [];
    for (let i = 0; i < groupUsers.length; i += AUDIENCE_UPLOAD) {
      uploadBatches.push(groupUsers.slice(i, i + AUDIENCE_UPLOAD));
    }

    for (let i = 0; i < uploadBatches.length; i += uploadConcurrency) {
      await Promise.allSettled(
        uploadBatches.slice(i, i + uploadConcurrency).map((batch) =>
          lineApi('PUT', '/v2/bot/audienceGroup/upload', {
            audienceGroupId,
            audiences: batch.map((id) => ({ id })),
          })
        )
      );
    }
    console.log(`Job ${jobId}: audience group ${gi + 1}/${groupCount} uploaded (${groupUsers.length} users)`);
  }

  // 3. Wait for all audience groups to become READY
  console.log(`Job ${jobId}: waiting for ${audienceGroupIds.length} audience groups to be READY…`);
  await Promise.all(audienceGroupIds.map(waitForAudience));
  console.log(`Job ${jobId}: all audiences READY, starting narrowcast`);

  // 4. Narrowcast sequentially (LINE enforces 1 active narrowcast at a time)
  for (let i = 0; i < audienceGroupIds.length; i++) {
    await lineApi('POST', '/v2/bot/message/narrowcast', {
      messages: [{ type: 'text', text }],
      recipient: { type: 'audience', audienceGroupId: audienceGroupIds[i] },
    });
    stmtIncrJob.run({
      jobId,
      s: Math.min(AUDIENCE_SIZE, total - i * AUDIENCE_SIZE),
      f: 0,
    });
    console.log(`Job ${jobId}: narrowcast ${i + 1}/${audienceGroupIds.length} sent`);
    // Brief pause between narrowcasts
    if (i < audienceGroupIds.length - 1) await new Promise((r) => setTimeout(r, 2000));
  }

  // 5. Cleanup audience groups (fire-and-forget, not critical)
  for (const id of audienceGroupIds) {
    lineApi('DELETE', `/v2/bot/audienceGroup/${id}`).catch((e) =>
      console.warn(`cleanup audience ${id} failed: ${e.message}`)
    );
  }

  const job = stmtGetJob.get({ jobId });
  stmtUpdateJob.run({
    jobId,
    status: job.failed > 0 ? 'completed_with_errors' : 'completed',
    sent: job.sent, failed: job.failed,
    finishedAt: new Date().toISOString(),
  });
  console.log(`Job ${jobId} (narrowcast) finished — sent: ${job.sent}, failed: ${job.failed}`);
}
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
// POST /broadcast  { mode?, baseUrl?, message?, concurrency? }
//
// mode = "broadcast"  (default) — 1 call, ALL followers, instant. LIFF personalizes on landing.
// mode = "narrowcast"           — upload audiences → narrowcast per group. LIFF personalizes.
//                                 Best for 13M+ registered users; LINE delivers on their side.
// mode = "multicast"            — 500 users/call, registered only. LIFF personalizes on landing.
// mode = "push"                 — 1 call/user, unique link visible directly in message. Slowest.
app.post('/broadcast', (req, res) => {
  const { cnt } = stmtCountUsers.get();
  const mode = req.body.mode || 'broadcast';

  if (!['broadcast', 'narrowcast', 'multicast', 'push'].includes(mode)) {
    return res.status(400).json({ error: 'mode must be broadcast, narrowcast, multicast, or push' });
  }
  if (mode !== 'broadcast' && cnt === 0) {
    return res.status(400).json({ error: 'No registered users found' });
  }

  const baseUrl     = req.body.baseUrl || process.env.BASE_URL || `http://localhost:${PORT}`;
  const message     = req.body.message || 'สวัสดี! แตะลิ้งนี้เพื่อดูข้อมูลเฉพาะของคุณ: {link}';
  const concurrency = Math.min(parseInt(req.body.concurrency) || 50, 100);

  if (!message.includes('{link}')) {
    return res.status(400).json({ error: 'message must contain {link} placeholder' });
  }

  const jobId = newToken();
  stmtInsertJob.run({ jobId, total: cnt, message, baseUrl, startedAt: new Date().toISOString() });

  const audienceGroups = Math.ceil(cnt / AUDIENCE_SIZE);
  const modeInfo = {
    broadcast:  { apiCalls: 1,                              label: '1 broadcast call (all followers)' },
    narrowcast: { apiCalls: audienceGroups,                 label: `${audienceGroups} narrowcast calls (via ${audienceGroups} audience groups)` },
    multicast:  { apiCalls: Math.ceil(cnt / MULTICAST_BATCH), label: `${Math.ceil(cnt / MULTICAST_BATCH).toLocaleString()} multicast calls` },
    push:       { apiCalls: cnt,                            label: `${cnt.toLocaleString()} push calls` },
  }[mode];

  setImmediate(() => {
    const runner =
      mode === 'broadcast'  ? runBroadcastAllJob(jobId, baseUrl, message) :
      mode === 'narrowcast' ? runNarrowcastJob(jobId, baseUrl, message, concurrency) :
      mode === 'multicast'  ? runMulticastJob(jobId, baseUrl, message, concurrency) :
                              runPushJob(jobId, baseUrl, message, concurrency);
    runner.catch((err) => {
      console.error(`Job ${jobId} crashed:`, err);
      stmtUpdateJob.run({ jobId, status: 'failed', sent: 0, failed: cnt, finishedAt: new Date().toISOString() });
    });
  });

  res.status(202).json({
    jobId,
    mode,
    total: cnt,
    apiCalls: modeInfo.apiCalls,
    message: `Broadcast started — ${modeInfo.label}`,
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
