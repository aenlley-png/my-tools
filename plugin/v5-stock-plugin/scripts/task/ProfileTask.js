// 周期性刷 Seller Central profile（merchantId / 在线状态）
const ProfileTask = {
  callback: null,
  async run() {
    const prev = await session.get(DEFINE.PROFILE_KEY) || {};
    const p = await ProfileService.sellerProfile();
    if (p && p.merchantId) {
      await session.set(DEFINE.PROFILE_KEY, {
        ...prev,
        activated: true,
        merchantId: p.merchantId,
        marketplaceId: p.marketplaceId || prev.marketplaceId || DEFINE.MKID_US,
      });
    } else {
      await session.set(DEFINE.PROFILE_KEY, { ...prev, activated: false });
    }
    if (typeof ProfileTask.callback === 'function') ProfileTask.callback();
  },
};
