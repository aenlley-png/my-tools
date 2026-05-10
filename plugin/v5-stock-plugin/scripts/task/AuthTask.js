// 周期性刷 token + 心跳
const AuthTask = {
  callback: null,
  async run() {
    setTimeout(async () => {
      await AuthTask._run();
      await AuthTask.beat();
    }, 1500);
  },
  async _run() {
    const profile = await session.get(DEFINE.PROFILE_KEY);
    if (!profile || !profile.refreshToken) return;
    const r = await AuthService.refresh(profile.refreshToken);
    const patch = r.status
      ? { accessible: true,  accessToken: r.accessToken, expiryTime: r.expiryTime }
      : { accessible: false, accessToken: null,          expiryTime: null };
    await session.set(DEFINE.PROFILE_KEY, { ...profile, ...patch });
    if (typeof AuthTask.callback === 'function') {
      AuthTask.callback({ accessible: patch.accessible, accessToken: patch.accessToken });
    }
  },
  async beat() {
    const r = await AuthService.beat();
    if (!r.status) console.log('[AuthTask] heartbeat fail:', r.message);
  },
};
