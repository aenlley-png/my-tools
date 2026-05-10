// chrome.alarms 调度封装（与 AD 插件 TaskManager.js 同接口）
const TaskManager = {
  inject(name, taskObj, periodSec, callback) {
    taskObj.callback = callback;
    chrome.alarms.create(name, { periodInMinutes: periodSec / 60 });
  },
  destory(name) { chrome.alarms.clear(name); },
  destoryAll()  { chrome.alarms.clearAll(); },
};
