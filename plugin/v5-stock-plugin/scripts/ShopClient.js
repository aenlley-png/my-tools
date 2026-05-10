// 后端通信封装：所有 /plugin/stock/* 请求都经过这里
// 与 AD 插件 ShopClient.js 同接口；headers 携带 pluginType / authorization / entityId / marketplaceId / advertiserId / advertiserType
const ShopClient = {
  async apost(path, body, accessToken, entityId, marketplaceId) {
    const profile = await session.get(DEFINE.PROFILE_KEY) || {};
    const headers = {
      'content-type':   'application/json',
      'pluginType':     DEFINE.PLUGIN_TYPE,
      'entityId':       entityId       || profile.entityId       || '',
      'marketplaceId':  marketplaceId  || profile.marketplaceId  || '',
      'authorization':  accessToken    || profile.accessToken    || '',
      'advertiserId':   profile.advertiserId   || '',
      'advertiserType': profile.advertiserType || '',
    };
    return await HttpClient.apost(DEFINE.SHOP5_URL + path, headers, JSON.stringify(body || {}));
  },
  async apostAuth(path, body) {
    return await HttpClient.apost(DEFINE.SHOP5_URL + path, { 'content-type': 'application/json' }, JSON.stringify(body || {}));
  },
};
