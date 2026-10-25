# QQ 官方机器人（API 聊天版）

QQ 开放平台官方机器人：WebSocket 接收私聊（C2C）/群 @消息 → 调用任意 OpenAI 兼容聊天 API → 被动回复。

单进程、零框架，只依赖 `ws`。所有配置走环境变量：

| 变量 | 必填 | 说明 | 默认 |
|---|---|---|---|
| `APP_ID` | ✅ | 开放平台 AppID | - |
| `APP_SECRET` | ✅ | 开放平台 AppSecret | - |
| `LLM_API_KEY` | ✅ | LLM 服务的 API Key | - |
| `LLM_BASE_URL` | | OpenAI 兼容接口地址 | `https://open.bigmodel.cn/api/paas/v4` |
| `LLM_MODEL` | | 模型名 | `glm-4-flash` |
| `SYSTEM_PROMPT` | | 机器人人设 | 简洁中文口语 |
| `HISTORY_TURNS` | | 每会话记忆轮数 | `6` |
| `COOLDOWN_MS` | | 单用户提问冷却 | `3000` |
| `PORT` | | 健康检查 HTTP 端口 | `8080` |

## 本地运行

```bash
npm install
APP_ID=xxx APP_SECRET=xxx LLM_API_KEY=sk-xxx node bot.js
```

## 说明

- 该入口创建的机器人**仅供创建人私聊**，不支持群聊（开放平台限制）
- 被动回复：收到消息后 5 分钟内、同一条消息最多回复 4 次
- 断线自动重连 + 断点续传（RESUME），重启不丢会话
