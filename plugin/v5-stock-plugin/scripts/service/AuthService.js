// 授权 / 续期 / 心跳（与 AD 插件 AuthService.js 对应；后端路径换成 /plugin/stock/*）
const AuthService = {
  async auth(entityId, marketplaceId, advertiserId, advertiserType, authCode) {
    const out = { status: 0, message: null, refreshToken: null, accessToken: null, expiryTime: null };
    const body = {
      pluginType: DEFINE.PLUGIN_TYPE,
      entityId, marketplaceId, advertiserId, advertiserType, authCode,
    };
    const text = await ShopClient.apostAuth('/plugin/stock/auth', body);
    if (!text) { out.message = '网络异常，请稍后再试'; return out; }
    const json = JSON.parse(text);
    if (json.code === 0) {
      out.status = 1;
      out.refreshToken = json.refreshToken;
      out.accessToken  = json.accessToken;
      out.expiryTime   = json.expiryTime;
    } else {
      out.message = json.msg || ('授权失败 ' + json.code);
    }
    return out;
  },

  async refresh(refreshToken) {
    const out = { status: 0, message: null, accessToken: null, expiryTime: null };
    const text = await ShopClient.apostAuth('/plugin/stock/atoken', {
      pluginType: DEFINE.PLUGIN_TYPE, refreshToken,
    });
    if (!text) { out.message = '网络异常'; return out; }
    const json = JSON.parse(text);
    if (json.code === 0) {
      out.status = 1;
      out.accessToken = json.accessToken;
      out.expiryTime  = json.expiryTime;
    } else {
      out.message = json.msg || '刷新失败';
    }
    return out;
  },

  async beat() {
    const profile = await session.get(DEFINE.PROFILE_KEY);
    const out = { status: 0, message: null };
    if (!profile) return out;
    const body = {
      pluginType: DEFINE.PLUGIN_TYPE,
      entityId: profile.entityId,
      marketplaceId: profile.marketplaceId,
      advertiserId: profile.advertiserId,
      advertiserType: profile.advertiserType,
      status: profile.accessible ? (profile.activated ? 1 : 3) : 2,
    };
    const text = await ShopClient.apost('/plugin/stock/heartbeat', body);
    if (!text) return out;
    const json = JSON.parse(text);
    if (json.code === 0) out.status = 1; else out.message = json.msg;
    return out;
  },
};
