// HTTP 客户端：通过 declarativeNetRequest 动态规则注入自定义 header（绕开 fetch 的安全 header 限制）
// 与 AD 插件 HttpClient.js 同接口（METHOD_GET/POST/PUT, get/post/aget/apost/apostHeaders ...）
const HttpClient = {
  METHOD_GET:  'GET',
  METHOD_POST: 'POST',
  METHOD_PUT:  'PUT',

  aget(url, headers) {
    return new Promise(resolve => {
      HttpClient.asyncHttp(HttpClient.METHOD_GET, url, headers, null, (text) => resolve(text));
    });
  },
  apost(url, headers, body) {
    return new Promise(resolve => {
      HttpClient.asyncHttp(HttpClient.METHOD_POST, url, headers, body, (text) => resolve(text));
    });
  },
  aput(url, headers, body) {
    return new Promise(resolve => {
      HttpClient.asyncHttp(HttpClient.METHOD_PUT, url, headers, body, (text) => resolve(text));
    });
  },
  asyncHttp(method, url, headers, body, cb) {
    const rulesObj = HttpClient.toRulesObj(headers || {}, url);
    chrome.declarativeNetRequest.updateDynamicRules(rulesObj, () => {
      fetch(url, { method, mode: 'cors', credentials: 'include', body: body || undefined })
        .then(r => r.text())
        .then(t => cb(t, t))
        .catch(e => cb(null, e));
    });
  },

  // 同 AD 插件：根据 URL 选择规则槽位（避免规则 id 冲突）
  toRulesObj(headers, url) {
    let id = 1, urlFilter = 'advertising.amazon.com*';
    if (url.includes('advertising.amazon.com'))         { id = 1; urlFilter = 'advertising.amazon.com*'; }
    else if (url.includes('www.amazon.com'))            { id = 2; urlFilter = 'www.amazon.com*'; }
    else if (url.includes('sellercentral.amazon'))      { id = 3; urlFilter = 'sellercentral.amazon*'; }
    else if (url.includes('auth'))                      { id = 4; urlFilter = '*'; }
    else                                                { id = 5; urlFilter = '*'; }

    const rule = {
      id, priority: id,
      condition: { urlFilter, resourceTypes: ['main_frame', 'xmlhttprequest'] },
      action: {
        type: 'modifyHeaders',
        requestHeaders: [{ header: 'Accept', operation: 'set', value: '*/*' }],
      },
    };
    for (const k in headers) {
      // ce-foo-bar  →  foo-bar （绕开 chrome 的 forbidden headers）
      const realName = k.startsWith('ce-') ? k.slice(3) : k;
      if (headers[k] == null || headers[k] === '') {
        console.warn('header value empty:', k);
        continue;
      }
      rule.action.requestHeaders.push({ header: realName, operation: 'set', value: String(headers[k]) });
    }
    return { removeRuleIds: [id], addRules: [rule] };
  },
};
