const statusEl = document.getElementById('status');
const streamEl = document.getElementById('stream');
const q = document.getElementById('q');
const kind = document.getElementById('kind');
const level = document.getElementById('level');
const pauseBtn = document.getElementById('pause');
const clearBtn = document.getElementById('clear');
const meta = document.getElementById('meta');

const rpsEl = document.getElementById('rps');
const err5xxEl = document.getElementById('err5xx');
const p95El = document.getElementById('p95');
const inflightEl = document.getElementById('inflight');
const statusbEl = document.getElementById('statusb');

const routesEl = document.getElementById('routes');
const sigsEl = document.getElementById('sigs');
const latNotice = document.getElementById('latNotice');

const recentEl = document.getElementById('recent');
const slowEl = document.getElementById('slowtraces');
const traceErrors = document.getElementById('traceErrors');
const traceMinDur = document.getElementById('traceMinDur');
const traceFilter = document.getElementById('traceFilter');

const traceDrawer = document.getElementById('traceDrawer');
const traceClose = document.getElementById('traceClose');
const traceCopy = document.getElementById('traceCopy');
const traceDownload = document.getElementById('traceDownload');
const traceTitle = document.getElementById('traceTitle');
const traceSummary = document.getElementById('traceSummary');
const traceTimeline = document.getElementById('traceTimeline');
const traceBreakdown = document.getElementById('traceBreakdown');
const traceLogs = document.getElementById('traceLogs');

let paused = false;
let total = 0, shown = 0;
let sortBy = 'p95';
const maxNodes = 400;
let currentTrace = null;

function fmtTime(ts){
  const d = new Date(ts * 1000);
  return d.toLocaleTimeString();
}
function fmtMs(x){
  if (x === null || x === undefined) return '--';
  return Math.round(x) + 'ms';
}
function pct(x){ return (x * 100).toFixed(2) + '%'; }

function fmtStatus(s){
  if (s === null || s === undefined) return '--';
  return String(s);
}

function spanType(span){
  const metaType = (span.meta && span.meta.type) ? String(span.meta.type) : '';
  const name = (metaType || span.name || '').toLowerCase();
  if (name.includes('db') || name.includes('sql') || name.includes('postgres')) return 'db';
  if (name.includes('http') || name.includes('grpc') || name.includes('rpc')) return 'http';
  if (name.includes('cache') || name.includes('redis') || name.includes('memcache')) return 'cache';
  return name ? name.split(' ')[0] : 'other';
}

function matches(ev){
  const qq = q.value.trim().toLowerCase();
  const k = kind.value.trim();
  const lv = level.value.trim();

  if (k){
    if (k === 'req'){
      if (!(ev.kind === 'req' || ev.kind === 'req_start' || ev.kind === 'req_end')) return false;
    } else if (ev.kind !== k){
      return false;
    }
  }
  if (ev.kind === 'log' && lv && (ev.level||'').toUpperCase() !== lv) return false;

  if (!qq) return true;

  const isReq = ev.kind === 'req' || ev.kind === 'req_start' || ev.kind === 'req_end';
  const ep = isReq ? ((ev.method||'') + ' ' + (ev.route||ev.path||'')) : '';
  const hay = ((ev.service||'') + ' ' + ep + ' ' + (ev.message||'') + ' ' + (ev.trace||'')).toLowerCase();
  return hay.includes(qq);
}

function add(ev){
  total++;
  if (!matches(ev)) { meta.textContent = `total ${total} | shown ${shown}`; return; }
  shown++;

  const div = document.createElement('div');
  div.className = 'logline';

  let lvl = 'INFO';
  let text = '';

  if (ev.kind === 'req' || ev.kind === 'req_end' || ev.kind === 'req_start'){
    lvl = 'REQ';
    const ep = (ev.method||'GET') + ' ' + (ev.route || ev.path);
    if (ev.kind === 'req_start'){
      text = `${ep} -> start`;
    } else {
      const lat = ev.duration_ms != null ? ` (${Math.round(ev.duration_ms)}ms)` : '';
      text = `${ep} -> ${ev.status}${lat}`;
    }
  } else {
    lvl = (ev.level || 'INFO').toUpperCase();
    text = ev.message || '';
    if (ev.trace) text += "\n" + ev.trace;
  }

  div.innerHTML = `
    <div class="t">${fmtTime(ev.ts)}</div>
    <div class="svc" title="${ev.service||''}">${ev.service||'service'}</div>
    <div class="msg"><span class="lvl ${lvl}">${lvl}</span> - ${escapeHtml(text)}</div>
  `;

  streamEl.prepend(div);
  while (streamEl.children.length > maxNodes) streamEl.removeChild(streamEl.lastChild);
  meta.textContent = `total ${total} | shown ${shown}`;
}

function escapeHtml(s){
  return (s||'')
    .replaceAll('&','&amp;')
    .replaceAll('<','&lt;')
    .replaceAll('>','&gt;')
    .replaceAll('"','&quot;')
    .replaceAll("'","&#39;");
}

async function refreshMetrics(){
  try{
    const res = await fetch('/metrics');
    const m = await res.json();

    rpsEl.textContent = (m.rps || 0).toFixed(2);
    err5xxEl.textContent = pct(m.error_rate_5xx || 0);
    p95El.textContent = fmtMs(m.p95_ms);
    inflightEl.textContent = (m.inflight ?? 0);

    const s = m.status || {};
    statusbEl.textContent = `2xx ${s["2xx"]||0} | 4xx ${s["4xx"]||0} | 5xx ${s["5xx"]||0}`;

    latNotice.textContent = m.latency_available ? '' : 'latency unavailable in access-log mode (unless JSON timed logs)';
  }catch{}
}

async function refreshRoutes(){
  try{
    const res = await fetch(`/metrics/endpoints?limit=15&sort_by=${sortBy}`);
    const data = await res.json();
    const rows = data.endpoints || [];

    routesEl.innerHTML = rows.map(r => {
      const tails = r.tails || {};
      const status = r.status || {};
      const ep = r.endpoint || '';
      return `
        <div class="rowitem rowitem-endpoint">
          <div class="ep" title="${escapeHtml(ep)}">${escapeHtml(ep)}</div>
          <div class="t">p95 ${r.p95_ms ? Math.round(r.p95_ms)+'ms' : '--'}</div>
          <div class="t">rps ${(r.rps||0).toFixed(2)}</div>
          <div class="t">4xx ${pct((status["4xx"]||0) / (r.count||1))}</div>
          <div class="t">5xx ${pct(r.error_rate_5xx||0)}</div>
          <div class="t muted">>250 ${pct(tails.gt250||0)} | >500 ${pct(tails.gt500||0)} | >1s ${pct(tails.gt1000||0)} | >2s ${pct(tails.gt2000||0)}</div>
        </div>
      `;
    }).join('');
  }catch{}
}

async function refreshSigs(){
  try{
    const res = await fetch('/metrics/errorsigs?limit=10');
    const data = await res.json();
    const rows = data.signatures || [];
    sigsEl.innerHTML = rows.map(r => `
      <div class="rowitem" style="grid-template-columns: 1fr 90px;">
        <div class="ep" title="${r.sig}">sig ${r.sig}</div>
        <div class="t">x${r.count}</div>
      </div>
    `).join('');
  }catch{}
}

function applyTraceFilters(rows){
  let out = rows;
  const minDur = parseFloat(traceMinDur.value || '0');
  const q = (traceFilter.value || '').trim().toLowerCase();
  if (traceErrors.checked) out = out.filter(r => (r.status || 0) >= 500);
  if (minDur) out = out.filter(r => (r.duration_ms || 0) >= minDur);
  if (q) out = out.filter(r => (r.endpoint || '').toLowerCase().includes(q));
  return out;
}

function renderTraceTable(el, rows){
  if (!rows.length){
    el.innerHTML = '<div class="muted">no requests</div>';
    return;
  }
  el.innerHTML = rows.map(r => {
    const endpoint = r.endpoint || ((r.method||'GET') + ' ' + (r.route || r.path || ''));
    const status = fmtStatus(r.status);
    const dur = r.duration_ms != null ? Math.round(r.duration_ms) + 'ms' : '--';
    const traceId = r.trace_id || '--';
    const rid = r.request_id || '';
    const time = r.ts ? fmtTime(r.ts) : '--';
    const service = r.service || 'unknown';
    const rowClass = rid ? 'rowitem rowitem-trace trace-row' : 'rowitem rowitem-trace';
    return `
      <div class="${rowClass}" data-request-id="${rid}">
        <div class="t">${time}</div>
        <div class="svc" title="${escapeHtml(service)}">${escapeHtml(service)}</div>
        <div class="ep" title="${escapeHtml(endpoint)}">${escapeHtml(endpoint)}</div>
        <div class="t">${status}</div>
        <div class="t">${dur}</div>
        <div class="t" title="${escapeHtml(traceId)}">${escapeHtml(traceId)}</div>
      </div>
    `;
  }).join('');

  el.querySelectorAll('.trace-row').forEach(row => {
    const rid = row.getAttribute('data-request-id');
    if (rid) row.addEventListener('click', () => openTrace(rid));
  });
}

async function refreshTraces(){
  try{
    const res = await fetch('/metrics/traces?limit=200&slow_limit=10');
    const data = await res.json();
    const recent = applyTraceFilters(data.recent || []);
    renderTraceTable(recentEl, recent);
    renderTraceTable(slowEl, data.top_slow || []);
  }catch{}
}

function renderTraceDetail(data){
  currentTrace = data;
  const req = data.request || {};
  const headers = data.headers || {};
  const metaInfo = data.meta || {};
  const endpoint = req.endpoint || ((req.method||'GET') + ' ' + (req.route || req.path || ''));

  traceTitle.textContent = `${req.service || 'service'} | ${endpoint}`;
  traceSummary.textContent = [
    `status ${fmtStatus(req.status)} | ${fmtMs(req.duration_ms)}`,
    `request ${req.request_id || '--'}`,
    `trace ${req.trace_id || '--'}`,
    `span ${req.span_id || '--'}`,
    `start ${req.start_ts ? fmtTime(req.start_ts) : '--'} | end ${req.end_ts ? fmtTime(req.end_ts) : '--'}`,
    `headers ${JSON.stringify(headers)}`,
    `meta ${JSON.stringify(metaInfo)}`,
  ].join('\n');

  const spans = data.spans || [];
  if (!spans.length){
    traceTimeline.innerHTML = '<div class="muted">no spans</div>';
    traceBreakdown.innerHTML = '<div class="muted">no span data</div>';
  } else {
    const baseTs = req.start_ts || (spans.find(s => s.ts) || {}).ts || 0;
    const spanEnds = spans.map(s => (((s.ts || baseTs) - baseTs) * 1000) + (s.duration_ms || 0));
    const totalMs = req.duration_ms || Math.max(...spanEnds) || 1;
    traceTimeline.innerHTML = spans.map(s => {
      const startMs = Math.max(0, ((s.ts || baseTs) - baseTs) * 1000);
      const durMs = s.duration_ms || 0;
      const leftPct = Math.min(100, (startMs / totalMs) * 100);
      const widthPct = Math.max(1, Math.min(100 - leftPct, (durMs / totalMs) * 100));
      const name = s.name || 'span';
      return `
        <div class="rowitem rowitem-span">
          <div class="ep" title="${escapeHtml(name)}">${escapeHtml(name)}</div>
          <div class="spanbar"><div class="spanfill" style="left:${leftPct}%; width:${widthPct}%;"></div></div>
          <div class="t">${fmtMs(durMs)}</div>
        </div>
      `;
    }).join('');

    const totals = {};
    spans.forEach(s => {
      const t = spanType(s);
      const dur = s.duration_ms || 0;
      totals[t] = (totals[t] || 0) + dur;
    });
    const breakdown = Object.entries(totals).sort((a, b) => b[1] - a[1]);
    traceBreakdown.innerHTML = breakdown.map(([t, dur]) => `
      <div class="rowitem rowitem-breakdown">
        <div class="ep">${escapeHtml(t)}</div>
        <div class="t">${fmtMs(dur)}</div>
      </div>
    `).join('');
  }

  const logs = data.logs || [];
  if (!logs.length){
    traceLogs.innerHTML = '<div class="muted">no logs</div>';
  } else {
    traceLogs.innerHTML = logs.map(l => {
      const text = l.trace ? `${l.message || ''}\n${l.trace}` : (l.message || '');
      return `
        <div class="trace-log">
          <div class="t">${l.ts ? fmtTime(l.ts) : '--'}</div>
          <div class="msg"><span class="lvl ${l.level || 'INFO'}">${l.level || 'INFO'}</span> - ${escapeHtml(text)}</div>
        </div>
      `;
    }).join('');
  }
}

async function openTrace(requestId){
  try{
    const res = await fetch(`/trace/${requestId}`);
    if (!res.ok) return;
    const data = await res.json();
    renderTraceDetail(data);
    traceDrawer.classList.add('open');
  }catch{}
}

function connect(){
  statusEl.textContent = 'connecting...';
  const es = new EventSource('/stream');

  es.onopen = () => { statusEl.textContent = paused ? 'paused' : 'live'; };
  es.onerror = () => {
    statusEl.textContent = 'disconnected (retrying...)';
    es.close();
    setTimeout(connect, 800);
  };
  es.onmessage = (e) => {
    if (paused) return;
    try { add(JSON.parse(e.data)); } catch {}
  };
}

document.querySelectorAll('button[data-sort]').forEach(btn => {
  if (btn.getAttribute('data-sort') === sortBy) btn.classList.add('active');
  btn.addEventListener('click', () => {
    sortBy = btn.getAttribute('data-sort');
    document.querySelectorAll('button[data-sort]').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    refreshRoutes();
  });
});

traceClose.onclick = () => {
  traceDrawer.classList.remove('open');
};
traceCopy.onclick = async () => {
  if (!currentTrace) return;
  const text = JSON.stringify(currentTrace, null, 2);
  try{
    await navigator.clipboard.writeText(text);
    traceCopy.textContent = 'copied';
    setTimeout(() => { traceCopy.textContent = 'copy JSON'; }, 1200);
  }catch{
    const ta = document.createElement('textarea');
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    try{ document.execCommand('copy'); }catch{}
    document.body.removeChild(ta);
  }
};
traceDownload.onclick = () => {
  if (!currentTrace) return;
  const rid = (currentTrace.request && currentTrace.request.request_id) || 'trace';
  const blob = new Blob([JSON.stringify(currentTrace, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `trace-${rid}.json`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};
traceErrors.onchange = refreshTraces;
traceMinDur.oninput = refreshTraces;
traceFilter.oninput = refreshTraces;

pauseBtn.onclick = () => {
  paused = !paused;
  pauseBtn.textContent = paused ? 'resume' : 'pause';
  statusEl.textContent = paused ? 'paused' : 'live';
};

clearBtn.onclick = () => {
  streamEl.innerHTML = '';
  total = 0; shown = 0;
  meta.textContent = '';
};

setInterval(refreshMetrics, 1000);
setInterval(refreshRoutes, 1500);
setInterval(refreshSigs, 2000);
setInterval(refreshTraces, 2000);

refreshMetrics();
refreshRoutes();
refreshSigs();
refreshTraces();
connect();
