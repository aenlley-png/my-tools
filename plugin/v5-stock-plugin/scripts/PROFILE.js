// 全局 profile 读写（与 AD 插件 PROFILE.js 同接口，仅 storage key 改为 STOCK_PROFILE_DATA）
const PROFILE = {
  valid(callback) {
    chrome.storage.local.get(DEFINE.PROFILE_KEY, (obj) => {
      const data = obj[DEFINE.PROFILE_KEY] ? JSON.parse(obj[DEFINE.PROFILE_KEY]) : {};
      callback(!!(data.authorized && data.authStatus && data.accessible));
    });
  },
  getAllStorage(callback) {
    chrome.storage.local.get(DEFINE.PROFILE_KEY, (obj) => {
      callback(obj[DEFINE.PROFILE_KEY] ? JSON.parse(obj[DEFINE.PROFILE_KEY]) : {});
    });
  },
  getStorage(key, callback) {
    this.getAllStorage((data) => callback(data[key]));
  },
  setStorage(patch) {
    this.getAllStorage((data) => {
      const merged = { ...data, ...patch };
      chrome.storage.local.set({ [DEFINE.PROFILE_KEY]: JSON.stringify(merged) });
    });
  },
};
