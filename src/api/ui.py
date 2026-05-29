"""Minimal ops dashboard served at /ui.

Single-file vanilla HTML/CSS/JS — no build step, no framework, no
external assets. Loads /status every 5s and /admin/quota every 60s,
authenticates each XHR with an API key the operator pastes into the
page (stored in localStorage). Restart buttons hit
POST /admin/workers/{id}/restart.

The page itself is unauthenticated (anyone can load it). The data
endpoints it calls are not — without a valid API key the dashboard
just shows error banners. Treat the dashboard like /status: it's
fine on a trusted internal network; behind a reverse proxy +
ACL for anything wider.
"""
from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, RedirectResponse


_INDEX_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>claude-subscription-proxy</title>
<style>
:root {
  --bg: #0d1117;
  --panel: #161b22;
  --border: #30363d;
  --text: #c9d1d9;
  --text-dim: #8b949e;
  --green: #3fb950;
  --yellow: #d29922;
  --red: #f85149;
  --blue: #58a6ff;
}
* { box-sizing: border-box; }
body {
  background: var(--bg); color: var(--text); margin: 0; padding: 16px;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 13px; line-height: 1.5;
}
h1 { margin: 0; font-size: 16px; font-weight: 600; }
h2 { margin: 0; font-size: 11px; color: var(--text-dim);
     text-transform: uppercase; letter-spacing: 0.5px; font-weight: 600; }
.card {
  background: var(--panel); border: 1px solid var(--border);
  border-radius: 6px; padding: 12px 16px; margin-bottom: 12px;
}
.row { display: flex; justify-content: space-between; align-items: center; gap: 16px; }
.row > div { display: flex; align-items: center; gap: 8px; }
.muted { color: var(--text-dim); }
.btn {
  background: #21262d; border: 1px solid var(--border); color: var(--text);
  padding: 4px 12px; border-radius: 4px; cursor: pointer;
  font-family: inherit; font-size: 12px;
}
.btn:hover { background: #30363d; }
.btn:disabled { opacity: 0.5; cursor: not-allowed; }
.btn-danger { color: var(--red); border-color: #4d1f1f; }
.btn-danger:hover { background: #4d1f1f; }
input[type="password"], input[type="text"] {
  background: var(--bg); border: 1px solid var(--border); color: var(--text);
  padding: 4px 8px; border-radius: 4px;
  font-family: inherit; font-size: 12px; width: 360px;
}
input:focus { outline: none; border-color: var(--blue); }
.bar { background: var(--border); height: 6px; border-radius: 3px;
       overflow: hidden; margin: 6px 0 4px; }
.bar-fill { height: 100%; background: var(--green);
            transition: width 0.4s ease, background 0.3s; }
.bar-fill.warn { background: var(--yellow); }
.bar-fill.danger { background: var(--red); }
table { width: 100%; border-collapse: collapse; margin-top: 8px; }
th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--border); }
th { color: var(--text-dim); font-weight: normal;
     text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px; }
tbody tr:hover { background: #1f242c; }
.status-cell { width: 100px; }
.status-icon { display: inline-block; width: 8px; height: 8px; border-radius: 50%;
               margin-right: 6px; vertical-align: middle; }
.status-idle .status-icon { background: var(--green); }
.status-working .status-icon { background: var(--yellow);
                               animation: pulse 1.4s ease-in-out infinite; }
.status-stuck .status-icon { background: var(--red); }
.status-dead .status-icon { background: var(--text-dim); }
@keyframes pulse { 50% { opacity: 0.4; } }
.preview {
  color: var(--text-dim); font-style: italic; max-width: 360px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.banner {
  background: #4d1f1f; border: 1px solid var(--red); color: #ffb4b4;
  padding: 8px 12px; border-radius: 4px; margin-bottom: 12px;
}
.quota-grid { display: grid;
              grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
              gap: 16px; margin-top: 8px; }
.quota-item .name { color: var(--text-dim); font-size: 11px;
                    text-transform: uppercase; letter-spacing: 0.5px; }
.quota-item .pct { font-size: 22px; font-weight: 600; margin: 2px 0; }
.quota-item .reset { color: var(--text-dim); font-size: 11px; }
.refresh-spin { display: inline-block; transition: transform 0.5s; }
.refresh-spin.spinning { transform: rotate(360deg); }
.right { text-align: right; }
.tabular { font-variant-numeric: tabular-nums; }
.stale { color: var(--yellow); }
</style>
</head>
<body>

<div class="card row">
  <div>
    <h1>claude-subscription-proxy</h1>
    <span class="muted" id="version">—</span>
  </div>
  <div>
    <input type="password" id="apikey" placeholder="API key (sk-internal-...)">
    <button class="btn" onclick="saveKey()">保存</button>
  </div>
</div>

<div id="banner" class="banner" style="display:none"></div>

<div class="card">
  <div class="row">
    <h2>Quota（OAuth 账号配额）</h2>
    <span class="muted" id="quota-meta">—</span>
  </div>
  <div class="quota-grid" id="quota-grid">
    <div class="muted">输入 API key 后加载</div>
  </div>
</div>

<div class="card">
  <div class="row">
    <h2>Workers</h2>
    <span class="muted" id="worker-summary">—</span>
  </div>
  <table>
    <thead>
      <tr>
        <th class="status-cell">状态</th>
        <th>user</th>
        <th class="right tabular">age</th>
        <th class="right tabular">busy</th>
        <th class="right tabular">stalled</th>
        <th class="right tabular">bytes</th>
        <th>model</th>
        <th>prompt</th>
        <th></th>
      </tr>
    </thead>
    <tbody id="worker-tbody">
      <tr><td colspan="9" class="muted">输入 API key 后加载</td></tr>
    </tbody>
  </table>
</div>

<script>
const $ = (id) => document.getElementById(id);
let apiKey = localStorage.getItem("apiKey") || "";
$("apikey").value = apiKey;

function saveKey() {
  apiKey = $("apikey").value.trim();
  localStorage.setItem("apiKey", apiKey);
  hideBanner();
  loadAll();
}

function showBanner(msg) { $("banner").textContent = msg; $("banner").style.display = "block"; }
function hideBanner() { $("banner").style.display = "none"; }

async function fetchJson(path, opts = {}) {
  if (!apiKey) throw new Error("未设置 API key");
  opts.headers = { ...(opts.headers || {}), "Authorization": "Bearer " + apiKey };
  const r = await fetch(path, opts);
  if (!r.ok) {
    let t = "";
    try { t = await r.text(); } catch (_) {}
    throw new Error(`HTTP ${r.status}: ${t.slice(0, 300)}`);
  }
  return r.json();
}

function fmtPct(u) { return (u == null) ? "—" : Math.floor(u * 100) + "%"; }
function barCls(u) {
  if (u == null) return "";
  if (u > 0.85) return "danger";
  if (u > 0.6) return "warn";
  return "";
}
function fmtReset(iso) {
  if (!iso) return "";
  const dt = new Date(iso);
  if (isNaN(dt)) return "";
  const ms = dt - new Date();
  if (ms <= 0) return "（已过期）";
  const totalMin = Math.floor(ms / 60000);
  const d = Math.floor(totalMin / 1440);
  const h = Math.floor((totalMin % 1440) / 60);
  const m = totalMin % 60;
  if (d > 0) return `${d}天${h}小时后重置`;
  if (h > 0) return `${h}小时${m}分钟后重置`;
  return `${m}分钟后重置`;
}
function fmtBytes(n) {
  if (n == null) return "—";
  if (n < 1024) return n + " B";
  if (n < 1048576) return (n / 1024).toFixed(1) + " KB";
  return (n / 1048576).toFixed(1) + " MB";
}
function fmtAge(s) {
  if (s == null) return "—";
  if (s < 60) return Math.floor(s) + "s";
  if (s < 3600) return Math.floor(s / 60) + "m";
  if (s < 86400) return (s / 3600).toFixed(1) + "h";
  return (s / 86400).toFixed(1) + "d";
}
function fmtStalled(s) {
  if (s == null) return "—";
  if (s < 0.5) return "0s";
  if (s < 60) return Math.floor(s) + "s";
  return Math.floor(s / 60) + "m" + Math.floor(s % 60) + "s";
}

async function loadQuota() {
  if (!apiKey) return;
  try {
    const d = await fetchJson("/admin/quota");
    renderQuota(d);
    const meta = [];
    if (d.cached) meta.push(`缓存 ${d.age_seconds}s 前`);
    if (d.stale) meta.push(`<span class="stale">⚠ 上游失败，显示旧值</span>`);
    if (!d.cached) meta.push("刚刷新");
    $("quota-meta").innerHTML = meta.join(" · ");
  } catch (e) {
    $("quota-grid").innerHTML = `<div class="muted">quota error: ${escapeHtml(e.message)}</div>`;
    if (e.message.startsWith("HTTP 401")) showBanner("API key 不对（401 Unauthorized）");
  }
}

function pickWindow(data, key) {
  // Upstream shape may be either { five_hour: {utilization,...}, ... }
  // or a flat array of {rateLimitType, utilization, ...}. Try both.
  if (data && typeof data === "object" && data[key]) return data[key];
  if (Array.isArray(data)) {
    return data.find(x => x && (x.rateLimitType === key || x.rate_limit_type === key)) || null;
  }
  return null;
}

function renderQuota(data) {
  const windows = [
    {key: "five_hour",        label: "5 小时窗口"},
    {key: "seven_day",        label: "7 天总额度"},
    {key: "seven_day_opus",   label: "7 天 Opus"},
    {key: "seven_day_sonnet", label: "7 天 Sonnet"},
  ];
  const parts = [];
  for (const w of windows) {
    const d = pickWindow(data, w.key) || {};
    const u = d.utilization ?? d.utilizationPercent ?? null;
    const norm = (typeof u === "number" && u > 1) ? u / 100 : u;  // tolerate 0..100 too
    const reset = d.resets_at || d.resetsAt || d.reset_at || d.resetAt || "";
    parts.push(`
      <div class="quota-item">
        <div class="name">${w.label}</div>
        <div class="pct">${fmtPct(norm)}</div>
        <div class="bar"><div class="bar-fill ${barCls(norm)}" style="width:${(norm || 0) * 100}%"></div></div>
        <div class="reset">${fmtReset(reset)}</div>
      </div>
    `);
  }
  $("quota-grid").innerHTML = parts.join("");
}

async function loadStatus() {
  if (!apiKey) return;
  try {
    const d = await fetchJson("/status");
    $("version").textContent = d.claude_version || "—";
    $("worker-summary").textContent =
      `count=${d.worker_count}  alive=${d.alive_count}  busy=${d.busy_count}  stuck=${d.stuck_count}`;
    renderWorkers(d.workers || []);
    hideBanner();
  } catch (e) {
    $("worker-tbody").innerHTML = `<tr><td colspan="9" class="muted">status error: ${escapeHtml(e.message)}</td></tr>`;
    if (e.message.startsWith("HTTP 401")) showBanner("API key 不对（401 Unauthorized）");
  }
}

function workerStatus(w) {
  if (!w.alive) return {cls: "dead", label: "DEAD"};
  if (w.in_flight === 0) return {cls: "idle", label: "IDLE"};
  if (w.stuck) return {cls: "stuck", label: "STUCK"};
  return {cls: "working", label: "WORKING"};
}

function renderWorkers(workers) {
  if (!workers.length) {
    $("worker-tbody").innerHTML = `<tr><td colspan="9" class="muted">no workers</td></tr>`;
    return;
  }
  const rows = workers.map(w => {
    const s = workerStatus(w);
    const d = (w.in_flight_detail || [])[0];
    const model = d?.body?.model || "—";
    const preview = d?.body?.last_user_preview || "—";
    return `
      <tr class="status-${s.cls}">
        <td><span class="status-icon"></span>${s.label}</td>
        <td>${escapeHtml(w.user_id)}</td>
        <td class="right tabular">${fmtAge(w.age_seconds)}</td>
        <td class="right tabular">${w.in_flight}</td>
        <td class="right tabular">${d ? fmtStalled(d.stalled_seconds) : "—"}</td>
        <td class="right tabular">${d ? fmtBytes(d.bytes_received) : "—"}</td>
        <td>${escapeHtml(model)}</td>
        <td class="preview" title="${escapeHtml(preview)}">${escapeHtml(preview)}</td>
        <td><button class="btn btn-danger" onclick="restartWorker('${escapeHtml(w.user_id)}', this)">restart</button></td>
      </tr>
    `;
  });
  $("worker-tbody").innerHTML = rows.join("");
}

async function restartWorker(userId, btn) {
  if (!confirm(`确定重启 worker '${userId}'？\\n当前 in-flight 请求会被截断（drain 最多 60s，超时强制 kill）。`)) return;
  btn.disabled = true;
  btn.textContent = "...";
  try {
    const result = await fetchJson(`/admin/workers/${encodeURIComponent(userId)}/restart`, {method: "POST"});
    alert(`重启完成 ${userId}\\nold_age=${result.old_age_seconds}s  forced=${result.forced}`);
    loadStatus();
  } catch (e) {
    alert(`重启失败 ${userId}: ${e.message}`);
  } finally {
    btn.disabled = false;
    btn.textContent = "restart";
  }
}

function escapeHtml(s) {
  return String(s ?? "")
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

function loadAll() { loadStatus(); loadQuota(); }

// Auto-refresh. /status is cheap (no upstream); /quota hits Anthropic
// (cached server-side for 30s) so we poll it less often.
setInterval(loadStatus, 5000);
setInterval(loadQuota, 60000);

if (apiKey) loadAll();
else $("apikey").focus();
</script>
</body>
</html>
"""


def build_router() -> APIRouter:
    router = APIRouter()

    @router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
    async def ui() -> HTMLResponse:
        # No auth on the page itself — anyone with network reach can
        # load the HTML, but every XHR it makes carries an API key and
        # the data endpoints reject without one. Same trust model as
        # serving static dashboard assets from a reverse proxy.
        return HTMLResponse(_INDEX_HTML)

    @router.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/ui", status_code=302)

    return router
