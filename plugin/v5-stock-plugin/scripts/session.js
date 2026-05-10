// chrome.storage.local 的 Promise 包装（与 AD 插件 session.js 同接口）
function nano_promise(fn, args = []) {
  return () => new Promise((resolve, reject) => {
    args.push(resolve, reject);
    fn(...args);
  });
}
function session_set(key, value, area = 'local', ok = () => {}, fail = () => {}) {
  if (!key) return console.error('session key is empty'), fail();
  chrome.storage[area].set({ [key]: JSON.stringify(value) }, (r) => {
    const err = chrome.runtime.lastError;
    if (err) { console.warn(err); fail(err); return; }
    ok(r);
  });
}
function session_get(key, fallback = null, area = 'local', ok = () => {}, fail = () => {}) {
  if (!key) return console.error('session key is empty'), fail();
  chrome.storage[area].get({ [key]: null }, (r) => {
    if (r[key]) {
      try { ok(JSON.parse(r[key])); } catch { ok(fallback); }
    } else ok(fallback);
  });
}
function session_remove(key, ok = () => {}, fail = () => {}) {
  chrome.storage.local.remove(key, ok);
}
function session_clear(ok = () => {}, fail = () => {}) {
  chrome.storage.local.clear(ok);
}
const session = {
  get:    (k, fb = null, area = 'local') => nano_promise(session_get,  [k, fb, area])(),
  set:    (k, v, area = 'local')         => nano_promise(session_set,  [k, v, area])(),
  remove: (k)                            => nano_promise(session_remove, [k])(),
  clear:  ()                             => nano_promise(session_clear)(),
};
