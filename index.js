// Import stylesheets
import './style.css';

// UI elements
const btnRegister = document.querySelector('#btnRegister');
const btnLogIn    = document.querySelector('#btnLogIn');
const btnLogOut   = document.querySelector('#btnLogOut');

const pictureUrl    = document.querySelector('#pictureUrl');
const displayName   = document.querySelector('#displayName');
const userIdEl      = document.querySelector('#userId');
const statusMessage = document.querySelector('#statusMessage');
const myLinkSection = document.querySelector('#myLinkSection');
const myLinkEl      = document.querySelector('#myLink');

// ─── Helpers ──────────────────────────────────────────────────────────────────
function showStatus(msg, isError = false) {
  statusMessage.textContent = msg;
  statusMessage.style.color = isError ? '#e00' : '#080';
}

function showButton(el) {
  el.style.display = 'block';
}

// ─── Register user and fetch personal link ────────────────────────────────────
async function registerUser(userId) {
  try {
    showStatus('กำลังลงทะเบียน...');
    const res = await fetch('/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ userId }),
    });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    myLinkEl.textContent = data.link;
    myLinkEl.href = data.link;
    myLinkSection.style.display = 'block';
    showStatus('ลงทะเบียนสำเร็จ!');
  } catch (err) {
    showStatus(`ลงทะเบียนไม่สำเร็จ: ${err.message}`, true);
  }
}

// ─── Main LIFF bootstrap ──────────────────────────────────────────────────────
async function main() {
  const liffId = window.__LIFF_ID__ || '';

  try {
    await liff.init({ liffId });
  } catch (err) {
    showStatus(`LIFF init error: ${err.message}`, true);
    return;
  }

  if (!liff.isLoggedIn()) {
    showButton(btnLogIn);
    btnLogIn.addEventListener('click', () => liff.login());
    displayName.textContent = 'กรุณาเข้าสู่ระบบ';
    return;
  }

  // Logged in — show profile
  showButton(btnLogOut);
  showButton(btnRegister);
  btnLogOut.addEventListener('click', () => liff.logout());

  try {
    const profile = await liff.getProfile();
    pictureUrl.src = profile.pictureUrl || pictureUrl.src;
    displayName.textContent = profile.displayName;
    userIdEl.textContent = `ID: ${profile.userId}`;

    btnRegister.addEventListener('click', () => registerUser(profile.userId));
  } catch (err) {
    showStatus(`ดึงโปรไฟล์ไม่สำเร็จ: ${err.message}`, true);
  }
}

main();
