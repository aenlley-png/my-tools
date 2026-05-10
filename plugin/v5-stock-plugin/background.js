// V5 Plugin-STOCK · Service Worker 入口
// 与 V5 Plugin-AD 同结构：importScripts → onAlarm dispatcher → onMessage RPC
//
// Storage key: STOCK_PROFILE_DATA
// 后端：/plugin/stock/auth | atoken | heartbeat | inventory/download | inventory/upload

importScripts('DEFINE.js');
importScripts('scripts/HttpClient.js');
importScripts('scripts/LazyClient.js');
importScripts('scripts/CACHE.js');
importScripts('scripts/PROFILE.js');
importScripts('scripts/session.js');
importScripts('scripts/ShopClient.js');
importScripts('scripts/service/AuthService.js');
importScripts('scripts/service/ProfileService.js');
importScripts('scripts/service/StockService.js');
importScripts('scripts/task/TaskManager.js');
importScripts('scripts/task/AuthTask.js');
importScripts('scripts/task/ProfileTask.js');
importScripts('scripts/task/StockTask.js');

// ── 周期任务注册 ─────────────────────────────────────
function startTask() {
  // 5 分钟刷 token + 心跳
  TaskManager.inject('AuthTask.run', AuthTask, 5 * 60, () => updateIcon());
  // 2 分钟刷 merchantId / 登录状态
  TaskManager.inject('ProfileTask.run', ProfileTask, 2 * 60, () => updateIcon());
  // 10 分钟拉一次指令并执行（也可由 popup 立即触发）
  if (DEFINE.RUNTIME === 'PROD') {
    TaskManager.inject('StockTask.run', StockTask, 10 * 60, () => {});
  }
}

// ── alarm dispatcher ────────────────────────────────
chrome.alarms.onAlarm.addListener((alarm) => {
  switch (alarm.name) {
    case 'AuthTask.run':    AuthTask.callback = () => updateIcon(); AuthTask.run(); break;
    case 'ProfileTask.run': ProfileTask.callback = () => updateIcon(); ProfileTask.run(); break;
    case 'StockTask.run':   StockTask.callback = () => {}; StockTask.run(); break;
  }
});

// ── 图标状态 ────────────────────────────────────────
function updateIcon() {
  PROFILE.valid((ok) => {
    chrome.action.setIcon({ path: ok ? 'icons/icon48.png' : 'icons/icon48-gray.png' });
  });
}

// ── 启动检查 ────────────────────────────────────────
async function checkProfile() {
  const profile = await session.get(DEFINE.PROFILE_KEY);
  if (!profile || !profile.authorized) { updateIcon(); return false; }
  // 拉一次 seller profile
  const sp = await ProfileService.sellerProfile();
  if (sp) {
    await session.set(DEFINE.PROFILE_KEY, { ...profile, activated: true,
      merchantId: sp.merchantId, marketplaceId: sp.marketplaceId || profile.marketplaceId });
  }
  // 试着刷一次 token
  const r = await AuthService.refresh(profile.refreshToken);
  if (r.status) {
    await session.set(DEFINE.PROFILE_KEY, { ...profile, accessible: true,
      accessToken: r.accessToken, expiryTime: r.expiryTime });
  }
  updateIcon();
  return true;
}

async function authorization(authCode) {
  const out = { status: 0, message: null, value: null };
  // 先做 sellerProfile，方便把 merchantId 一并提交（等价于 AD 插件的 entityId）
  let sp = null;
  try { sp = await ProfileService.sellerProfile(); } catch {}
  if (!sp || !sp.merchantId) {
    out.message = '未登录亚马逊后台（Seller Central），请先登录 sellercentral.amazon.com';
    out.value   = { activated: false };
    return out;
  }
  const auth = await AuthService.auth(
    sp.merchantId,                          // entityId（用 merchantId 表示店铺）
    sp.marketplaceId || DEFINE.MKID_US,
    null, null, authCode
  );
  if (auth.status) {
    const patch = {
      authorized: true, authStatus: 1, accessible: true, activated: true,
      authCode,
      entityId: sp.merchantId,
      merchantId: sp.merchantId,
      marketplaceId: sp.marketplaceId || DEFINE.MKID_US,
      refreshToken: auth.refreshToken,
      accessToken:  auth.accessToken,
      expiryTime:   auth.expiryTime,
    };
    await session.set(DEFINE.PROFILE_KEY, patch);
    updateIcon();
    startTask();
    out.status = 1; out.value = { activated: true };
  } else {
    out.status = 0;
    out.message = auth.message;
    out.value = { activated: true };
  }
  return out;
}

async function onStart() {
  const ok = await checkProfile();
  if (ok) startTask();
}

chrome.runtime.onStartup.addListener(() => { console.log('onStartup'); onStart(); });
chrome.runtime.onInstalled.addListener(() => { console.log('onInstalled'); onStart(); });

// ── popup 通信 ──────────────────────────────────────
chrome.runtime.onMessage.addListener((msg, sender, send) => {
  const reply = { status: 0, message: null, value: null };
  if (msg.action === 'authorization') {
    authorization(msg.authCode).then((r) => send(r));
  } else if (msg.action === 'logoutMethod') {
    chrome.storage.local.remove(DEFINE.PROFILE_KEY, () => {
      PROFILE.getAllStorage((data) => send(data));
    });
  } else if (msg.action === 'checkState') {
    onStart().then(() => {
      reply.status = 1;
      PROFILE.getAllStorage((data) => { reply.value = data.activated; send(reply); });
    });
  } else if (msg.action === 'getProfile') {
    reply.status = 1;
    PROFILE.getAllStorage((data) => { reply.value = data; send(reply); });
  } else if (msg.action === 'stock') {
    reply.status = 1; reply.value = CACHE.stock; send(reply);
  } else if (msg.action === 'runNow') {
    // 立即触发一轮拉取 / 执行
    StockTask.run().then(() => { reply.status = 1; send(reply); });
  } else {
    reply.message = 'unknown action: ' + msg.action; send(reply);
  }
  return true;
});
