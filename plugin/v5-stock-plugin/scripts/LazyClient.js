// 限速 HTTP 客户端：保证同一档位的请求最小间隔（避免亚马逊风控）
const LazyClient = {
  L1: { interval: 1000,  lastTime: null }, // 1s
  L2S:{ interval: 2000,  lastTime: null }, // 2s
  L2: { interval: 3000,  lastTime: null }, // 3s
  L3: { interval: 10000, lastTime: null }, // 10s

  agetL1: (u, h) => LazyClient.aget(LazyClient.L1, u, h),
  agetL2: (u, h) => LazyClient.aget(LazyClient.L2, u, h),
  agetL3: (u, h) => LazyClient.aget(LazyClient.L3, u, h),
  apostL1: (u, h, b) => LazyClient.apost(LazyClient.L1, u, h, b),
  apostL2S:(u, h, b) => LazyClient.apost(LazyClient.L2S,u, h, b),
  apostL2: (u, h, b) => LazyClient.apost(LazyClient.L2, u, h, b),
  apostL3: (u, h, b) => LazyClient.apost(LazyClient.L3, u, h, b),

  aget(level, url, headers) {
    return new Promise(resolve => {
      setTimeout(() => {
        HttpClient.asyncHttp(HttpClient.METHOD_GET, url, headers, null, (t) => resolve(t));
      }, LazyClient.timeDelay(level));
    });
  },
  apost(level, url, headers, body) {
    return new Promise(resolve => {
      setTimeout(() => {
        HttpClient.asyncHttp(HttpClient.METHOD_POST, url, headers, body, (t) => resolve(t));
      }, LazyClient.timeDelay(level));
    });
  },
  timeDelay(level = LazyClient.L1) {
    const now = Date.now();
    let delay;
    if (level.lastTime) {
      if (level.lastTime + level.interval < now) { delay = 0; level.lastTime = now; }
      else { delay = level.lastTime + level.interval - now; level.lastTime += level.interval; }
    } else { delay = 0; level.lastTime = now; }
    return delay;
  },
};
