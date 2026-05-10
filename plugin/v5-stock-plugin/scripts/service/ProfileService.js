// 从亚马逊后台拉 profile 信息（merchantId / marketplaceId）
// 思路：访问一个轻量的 sellercentral 页面，正则提取 merchantId 等
const ProfileService = {
  async sellerProfile() {
    // /home 是 Seller Central 起始页；返回的 HTML 里通常带 obfuscatedMerchantId / merchantSelectionState
    const candidates = [
      'https://sellercentral.amazon.com/home',
      'https://sellercentral.amazon.com/gp/homepage.html',
    ];
    for (const url of candidates) {
      const html = await LazyClient.agetL1(url, {});
      if (!html) continue;
      const merchant = (html.match(/"merchantId":\s*"([A-Z0-9]{10,})"/) || html.match(/obfuscatedMerchantId=([A-Z0-9]{10,})/)) || [];
      const marketplaceId = (html.match(/"marketplaceId":\s*"([A-Z0-9]{10,})"/) || []);
      if (merchant[1]) {
        return {
          merchantId: merchant[1],
          marketplaceId: marketplaceId[1] || null,
        };
      }
    }
    return null;
  },
};
