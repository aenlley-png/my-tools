// popup 逻辑（不依赖 jQuery）
const $ = (id) => document.getElementById(id);
const send = (msg) => new Promise((resolve) => chrome.runtime.sendMessage(msg, resolve));

document.addEventListener('DOMContentLoaded', refresh);
$('authorization_btn').addEventListener('click', onAuthorize);
$('runNow_btn')      .addEventListener('click', onRunNow);
$('logout_btn')      .addEventListener('click', onLogout);
$('checkState_btn')  .addEventListener('click', onCheckState);
$('login_a')         .addEventListener('click', () => chrome.tabs.create({ url: 'https://sellercentral.amazon.com/' }));

async function refresh() {
  const r = await send({ action: 'getProfile' });
  const p = r && r.value || {};
  if (p.authorized) {
    show('content_div'); hide('auth_div');
    if (p.activated && p.accessible)      { show('online_div');  hide('unlogin_div'); hide('offline_div'); }
    else if (!p.activated)                 { show('unlogin_div'); hide('online_div');  hide('offline_div'); }
    else                                   { show('offline_div');hide('online_div');  hide('unlogin_div'); }
    renderRecorders();
  } else {
    show('auth_div'); hide('content_div');
  }
}

async function onAuthorize() {
  const code = $('authcode_text').value.trim().toUpperCase();
  if (!/^[0-9A-F]{16}$/.test(code)) { return showError('请输入 16 位授权码'); }
  setLoading(true);
  let r;
  try {
    r = await Promise.race([
      send({ action: 'authorization', authCode: code }),
      new Promise(resolve => setTimeout(() => resolve('__TIMEOUT__'), 30000)),
    ]);
  } catch (e) { setLoading(false); return showError('调用异常：' + e.message); }
  setLoading(false);
  if (r === '__TIMEOUT__') return showError('Service Worker 30s 未响应。请到扩展程序页 → V5 Plugin-STOCK → 「Service Worker」点开看错误，或重新加载插件。');
  if (!r) return showError('插件未响应（service worker 可能已停止）。请到 chrome://extensions 重新加载 V5 Plugin-STOCK。');
  if (r.status === 1) { refresh(); }
  else { showError(r.message || ('授权失败 status=' + r.status)); }
}

async function onLogout()      { await send({ action: 'logoutMethod' }); refresh(); }
async function onCheckState()  { await send({ action: 'checkState' });   refresh(); }
async function onRunNow() {
  $('runNow_btn').disabled = true;
  await send({ action: 'runNow' });
  $('runNow_btn').disabled = false;
  renderRecorders();
}

async function renderRecorders() {
  const r = await send({ action: 'stock' });
  const recs = (r && r.value && r.value.recorders) || [];
  $('stock_recorder_ul').innerHTML = recs.slice().reverse().map(rec => `
    <li class="${rec.status === 1 ? 'ok' : 'fail'}">
      <span class="badge">${rec.status === 1 ? 'SUCCESS' : 'FAILED'}</span>
      <span class="action">${escape(rec.action)}</span>
      <span class="muted small">#${rec.taskId}</span>
      <span class="muted small">items:${rec.itemCount}</span>
      <span class="muted small">${rec.durationMs}ms</span>
      <span class="muted small">${escape(rec.time || '')}</span>
      ${rec.message ? `<div class="muted small">${escape(rec.message)}</div>` : ''}
    </li>`).join('') || '<li class="muted small">暂无记录</li>';
}

function show(id) { $(id).style.display = ''; }
function hide(id) { $(id).style.display = 'none'; }
function setLoading(v) { $('authorization_btn').disabled = v; }
function showError(msg) { const el = $('error_prompt_div'); el.textContent = msg; el.classList.remove('hide'); }
function escape(s) { return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
