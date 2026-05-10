// 拉 Seller Central profile 信息（merchantId / marketplaceId / 登录状态）
// 检测优先级：cookies > HTML 抓取
const ProfileService = {
  // 通过 chrome.cookies 判断是否已登录（最可靠：不依赖 HTML 解析）
  async isLoggedIn() {
    return new Promise(resolve => {
      try {
        chrome.cookies.getAll({ domain: 'amazon.com' }, (cookies) => {
          const list = cookies || [];
          // Seller Central 常见会话 cookie：at-main / sess-at-main / session-id-time / x-main 等
          // 只要有 at-main / sess-at-main 之一，就视为已登录
          const has = list.some(c =>
            ['at-main','sess-at-main','x-main','sess-at-main-' /* 区域变体前缀 */]
              .some(n => (c.name || '').startsWith(n))
            && /amazon/.test(c.domain || '')
          );
          resolve(has);
        });
      } catch (e) { resolve(false); }
    });
  },

  async sellerProfile() {
    // 多个候选页 + 多个正则模式
    const candidates = [
      'https://sellercentral.amazon.com/inventoryplanning/inventory-health',
      'https://sellercentral.amazon.com/home',
      'https://sellercentral.amazon.com/gp/homepage.html',
      'https://sellercentral.amazon.com/',
    ];
    const patterns = {
      merchantId: [
        /"merchantId":\s*"([A-Z0-9]{10,})"/,
        /"obfuscatedMerchantId":\s*"([A-Z0-9]{10,})"/,
        /obfuscatedMerchantId=([A-Z0-9]{10,})/,
        /merchantId=([A-Z0-9]{10,})/,
        /data-merchant-id="([A-Z0-9]{10,})"/,
        /"sellingPartnerId":\s*"([A-Z0-9]{10,})"/,
      ],
      marketplaceId: [
        /"marketplaceId":\s*"([A-Z0-9]{10,})"/,
        /marketplaceID=([A-Z0-9]{10,})/,
        /mons_sel_mkid=([A-Z0-9]{10,})/,
      ],
    };
    for (const url of candidates) {
      const html = await LazyClient.agetL1(url, {});
      if (!html || html.length < 200) continue;
      let merchantId = null, marketplaceId = null;
      for (const p of patterns.merchantId) { const m = html.match(p); if (m) { merchantId = m[1]; break; } }
      for (const p of patterns.marketplaceId) { const m = html.match(p); if (m) { marketplaceId = m[1]; break; } }
      if (merchantId) return { merchantId, marketplaceId, source: url };
    }
    // HTML 抠不到 → 退而求其次用 cookie：从 mons_selected_dir / x-amz-account-id 等找
    return await new Promise(resolve => {
      chrome.cookies.getAll({ domain: 'amazon.com' }, (cookies) => {
        const list = cookies || [];
        const dir = list.find(c => c.name === 'mons_selected_dir');
        const mkid = list.find(c => c.name === 'mons_sel_mkid');
        if (dir && dir.value) {
          // 形如 "amzn1.merchant.o.AXXXXXXXX" — 截最后一段
          const m = dir.value.match(/A[A-Z0-9]{9,}/);
          resolve({
            merchantId: m ? m[0] : null,
            marketplaceId: mkid && mkid.value ? mkid.value : null,
            source: 'cookie',
          });
        } else resolve(null);
      });
    });
  },
};
