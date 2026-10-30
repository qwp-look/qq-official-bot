// QQ 官方机器人（QQ 开放平台）
// WebSocket 接收群 @消息 / C2C 私聊 → 调 LLM API → 被动回复
import http from 'node:http';
import { WebSocket } from 'ws';

const {
  PORT = '8080',
  APP_ID = '',
  APP_SECRET = '',
  LLM_API_MODE = 'chat',        // 'chat' = {base}/chat/completions；'responses' = {base}/responses
  LLM_BASE_URL = 'https://open.bigmodel.cn/api/paas/v4', // 任意 OpenAI 兼容接口
  LLM_API_KEY = '',
  LLM_MODEL = 'glm-4-flash',
  LLM_REASONING = 'high',       // responses 模式的思考强度：minimal/low/medium/high
  SYSTEM_PROMPT = '你是一个友好的 QQ 聊天机器人，用简洁自然的中文口语回答，不要用 markdown 格式。',
  HISTORY_TURNS = '6',     // 每个会话保留的对话轮数
  COOLDOWN_MS = '3000',    // 同一用户两次提问最小间隔
  MAX_REPLY_CHARS = '1500',
  AGENT_URL = '',          // infrlo agent 地址；留空则禁用工具
  AGENT_TOKEN = '',        // agent Bearer token
  PING_URLS = '',          // 逗号分隔，定时 ping 保活（如 infrlo agent 地址）
  PING_INTERVAL_MS = '240000',
  TEST_MESSAGE = '',       // 调试：启动后模拟一条用户消息（仅本地测试用）
} = process.env;

if (!APP_ID || !APP_SECRET) { console.error('缺少 APP_ID / APP_SECRET'); process.exit(1); }

// ---------- 鉴权：AppID + AppSecret -> AppAccessToken ----------
let accessToken = '';
let tokenExpireAt = 0;

async function refreshAccessToken(force = false) {
  if (!force && accessToken && Date.now() < tokenExpireAt - 5 * 60_000) return accessToken;
  const res = await fetch('https://bots.qq.com/app/getAppAccessToken', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ appId: APP_ID, clientSecret: APP_SECRET }),
  });
  if (!res.ok) throw new Error(`getAppAccessToken ${res.status}: ${(await res.text()).slice(0, 300)}`);
  const data = await res.json();
  accessToken = data.access_token;
  tokenExpireAt = Date.now() + (Number(data.expires_in) || 7200) * 1000;
  console.log('access token refreshed, expires_in =', data.expires_in);
  return accessToken;
}

async function qqApi(method, path, body) {
  const doFetch = async () => fetch(`https://api.sgroup.qq.com${path}`, {
    method,
    headers: { 'Content-Type': 'application/json', Authorization: `QQBot ${accessToken}` },
    body: body ? JSON.stringify(body) : undefined,
  });
  await refreshAccessToken();
  let res = await doFetch();
  if (res.status === 401) { await refreshAccessToken(true); res = await doFetch(); }
  const text = await res.text();
  if (!res.ok) throw new Error(`QQ API ${method} ${path} ${res.status}: ${text.slice(0, 300)}`);
  return text ? JSON.parse(text) : null;
}

// ---------- LLM（chat completions 或 responses 两种模式） ----------
async function chatLLM(messages) {
  const base = LLM_BASE_URL.replace(/\/$/, '');
  const isResponses = LLM_API_MODE === 'responses';
  const res = await fetch(isResponses ? `${base}/responses` : `${base}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${LLM_API_KEY}` },
    body: JSON.stringify(isResponses
      ? { model: LLM_MODEL, input: messages, reasoning: { effort: LLM_REASONING } }
      : { model: LLM_MODEL, messages }),
  });
  if (!res.ok) throw new Error(`LLM API ${res.status}: ${(await res.text()).slice(0, 300)}`);
  const data = await res.json();
  if (!isResponses) return data.choices[0].message.content.trim();
  const text = (data.output ?? [])
    .filter((o) => o.type === 'message')
    .flatMap((o) => o.content ?? [])
    .filter((c) => c.type === 'output_text')
    .map((c) => c.text)
    .join('')
    .trim();
  if (!text) throw new Error(`responses API 无文本输出: ${JSON.stringify(data).slice(0, 200)}`);
  return text;
}

// ---------- Agent 工具调用（LLM 输出 JSON 行 -> 调 infrlo agent -> 结果回填） ----------
const AGENT_TOOLS_PROMPT = [
  '# 远程服务器工具',
  '你可以操作一台联网的 Linux 服务器。确需执行操作时，在回复中单独输出一行 JSON（该行只含 JSON，不要加代码块标记）：',
  '{"tool":"run","command":"<shell 命令>"} 在服务器上执行 shell 命令',
  '{"tool":"fetch","url":"<http(s) 地址>"} 让服务器抓取网页并返回正文',
  '系统会以 TOOL_RESULT 消息返回结果，之后你根据结果用简洁自然的中文回答用户。',
  '日常闲聊或不需要工具时直接回答，禁止输出 JSON。',
].join('\n');

function extractToolCall(text) {
  for (const line of String(text).split('\n')) {
    const t = line.trim();
    if (!t.startsWith('{') || !t.includes('"tool"')) continue;
    try {
      const obj = JSON.parse(t);
      if (obj.tool === 'run' && obj.command) return obj;
      if (obj.tool === 'fetch' && obj.url) return obj;
    } catch { /* 该行不是合法 JSON，继续 */ }
  }
  return null;
}

async function agentTool(call) {
  const path = call.tool === 'run' ? '/run' : '/fetch';
  const body = call.tool === 'run' ? { command: String(call.command).slice(0, 2000) } : { url: String(call.url) };
  const res = await fetch(`${AGENT_URL.replace(/\/$/, '')}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${AGENT_TOKEN}` },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(90000),
  });
  const data = await res.json();
  return JSON.stringify(data).slice(0, 3500);
}

// ---------- 会话状态 ----------
const histories = new Map();  // key -> [{role, content}]
const cooldowns = new Map();  // openid -> last ts
const replySeq = new Map();   // msg_id -> 已回复次数（被动回复同一条消息最多 4 次）

function nextMsgSeq(msgId) {
  const n = (replySeq.get(msgId) ?? 0) + 1;
  replySeq.set(msgId, n);
  setTimeout(() => replySeq.delete(msgId), 10 * 60_000).unref?.();
  return Math.min(n, 4);
}

async function reply(path, msgId, content) {
  await qqApi('POST', path, { msg_type: 0, content, msg_id: msgId, msg_seq: nextMsgSeq(msgId) });
}

async function handleMessage({ key, authorId, text, msgId, sendPath }) {
  text = text.trim();
  if (!text) return;
  const last = cooldowns.get(authorId) ?? 0;
  if (Date.now() - last < Number(COOLDOWN_MS)) return;
  cooldowns.set(authorId, Date.now());

  const history = histories.get(key) ?? [];
  history.push({ role: 'user', content: text });
  console.log(`[${key}] ${authorId}: ${text}`);

  const sys = { role: 'system', content: AGENT_URL ? `${SYSTEM_PROMPT}\n${AGENT_TOOLS_PROMPT}` : SYSTEM_PROMPT };
  const messages = [sys, ...history.slice(-Number(HISTORY_TURNS) * 2)];
  let answer = null;
  try {
    // 工具循环：LLM 可输出最多 3 次工具调用，结果回填后继续生成
    for (let round = 0; round < 3; round++) {
      answer = await chatLLM(messages);
      const call = AGENT_URL ? extractToolCall(answer) : null;
      if (!call) break;
      console.log(`[${key}] tool round ${round + 1}: ${JSON.stringify(call).slice(0, 150)}`);
      let result;
      try { result = await agentTool(call); } catch (err) { result = JSON.stringify({ error: err.message }); }
      messages.push({ role: 'assistant', content: answer }, { role: 'user', content: `TOOL_RESULT: ${result}` });
      answer = null;
    }
    if (!answer) {
      messages.push({ role: 'user', content: '（工具调用次数已达上限，请根据已有信息直接用中文回答，不要再调用工具）' });
      answer = await chatLLM(messages);
    }
  } catch (err) {
    console.error('LLM error:', err.message);
    answer = '（AI 接口出错了，稍后再试）';
  }
  if (answer.length > Number(MAX_REPLY_CHARS)) answer = answer.slice(0, Number(MAX_REPLY_CHARS)) + '…';

  history.push({ role: 'assistant', content: answer });
  histories.set(key, history.slice(-Number(HISTORY_TURNS) * 4));

  try {
    await reply(sendPath, msgId, answer);
    console.log(`[${key}] -> ${answer.slice(0, 80)}`);
  } catch (err) {
    console.error('send error:', err.message);
  }
}

// 群里 @机器人
function onGroupMessage(d) {
  return handleMessage({
    key: `g:${d.group_openid}`,
    authorId: d.author?.member_openid ?? 'unknown',
    text: d.content ?? '',
    msgId: d.id,
    sendPath: `/v2/groups/${d.group_openid}/messages`,
  });
}

// 用户私聊机器人
function onC2CMessage(d) {
  const openid = d.author?.user_openid ?? d.author?.open_id ?? d.author?.id;
  return handleMessage({
    key: `u:${openid}`,
    authorId: openid ?? 'unknown',
    text: d.content ?? '',
    msgId: d.id,
    sendPath: `/v2/users/${openid}/messages`,
  });
}

// ---------- WebSocket 接入 ----------
// 群聊 + 单聊（GROUP_AT_MESSAGE_CREATE / C2C_MESSAGE_CREATE 等）= GROUP_AND_C2C_EVENT (1 << 25)
const INTENTS = 1 << 25;
let ws = null;
let seq = 0;
let sessionId = '';
let heartbeatTimer = null;
let lastAckAt = 0;
let closedByUs = false;
let gatewayUrl = '';      // 缓存网关地址，避免重连时反复请求触发限频
let retryDelay = 3000;    // 重连退避

function send(obj) { if (ws?.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj)); }

function identify() {
  send({ op: 2, d: { token: `QQBot ${accessToken}`, intents: INTENTS, shard: [0, 1], properties: {} } });
}
function resume() {
  send({ op: 6, d: { token: `QQBot ${accessToken}`, session_id: sessionId, seq } });
}
function startHeartbeat(intervalMs) {
  clearInterval(heartbeatTimer);
  lastAckAt = Date.now();
  heartbeatTimer = setInterval(() => {
    if (Date.now() - lastAckAt > intervalMs * 2) { console.log('heartbeat timeout, reconnect'); ws?.close(); return; }
    send({ op: 1, d: seq });
  }, intervalMs);
}

function scheduleReconnect() {
  if (closedByUs) return; // 已有人安排重连
  closedByUs = true;
  clearInterval(heartbeatTimer);
  ws?.close();
  console.log(`reconnect in ${retryDelay / 1000}s...`);
  setTimeout(() => { closedByUs = false; connect(); }, retryDelay);
  retryDelay = Math.min(retryDelay * 2, 60000);
}

async function connect() {
  await refreshAccessToken();
  if (!gatewayUrl) {
    const g = await qqApi('GET', '/gateway');
    gatewayUrl = g.url;
  }
  console.log('connecting gateway:', gatewayUrl);
  closedByUs = false;
  ws = new WebSocket(gatewayUrl);

  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const { op, t, s, d } = msg;
    if (s) seq = s;
    switch (op) {
      case 10: // Hello
        startHeartbeat(d.heartbeat_interval);
        if (sessionId) resume(); else identify();
        break;
      case 11: lastAckAt = Date.now(); break;               // Heartbeat ACK
      case 0:
        if (t === 'READY') { sessionId = d.session_id; retryDelay = 3000; console.log('READY, bot id =', d.user?.id); }
        else if (t === 'RESUMED') { retryDelay = 3000; console.log('session resumed'); }
        else if (t === 'GROUP_AT_MESSAGE_CREATE') onGroupMessage(d).catch((e) => console.error(e.message));
        else if (t === 'C2C_MESSAGE_CREATE') onC2CMessage(d).catch((e) => console.error(e.message));
        break;
      case 7: console.log('server asks reconnect'); scheduleReconnect(); break;
      case 9: console.log('invalid session, re-identify'); sessionId = ''; seq = 0; scheduleReconnect(); break;
    }
  });

  ws.on('error', (e) => console.error('ws error:', e.message));
  ws.on('close', () => {
    clearInterval(heartbeatTimer);
    if (!closedByUs) scheduleReconnect();
  });
}
const onConnErr = (e) => {
  console.error('connect error:', e.message);
  setTimeout(() => connect().catch(onConnErr), retryDelay);
  retryDelay = Math.min(retryDelay * 2, 60000);
};

// ---------- 健康检查 HTTP ----------
http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' }).end(`qq official bot ok\nseq=${seq} session=${sessionId ? 'yes' : 'no'}\n`);
}).listen(Number(PORT), () => console.log(`health server on :${PORT}, starting websocket...`));

// ---------- 双向保活：定时 ping（如 infrlo agent） ----------
if (PING_URLS) {
  const urls = PING_URLS.split(',').map((s) => s.trim()).filter(Boolean);
  setInterval(async () => {
    for (const u of urls) {
      try {
        const r = await fetch(u, { signal: AbortSignal.timeout(15000) });
        console.log(`[keepalive] ${u} -> ${r.status}`);
      } catch (e) { console.error(`[keepalive] ${u} failed: ${e.message}`); }
    }
  }, Number(PING_INTERVAL_MS)).unref?.();
  console.log(`keepalive pinger on: ${urls.join(', ')}`);
}

console.log(AGENT_URL ? `agent enabled: ${AGENT_URL}` : 'agent disabled (no AGENT_URL)');

// ---------- 调试钩子：模拟一条用户消息（仅本地测试用） ----------
if (TEST_MESSAGE) {
  setTimeout(() => {
    handleMessage({ key: 'u:test', authorId: 'test', text: TEST_MESSAGE, msgId: `test_${Date.now()}`, sendPath: '/v2/users/test/messages' })
      .catch((e) => console.error('test message error:', e.message))
      .finally(() => console.log('TEST_MESSAGE done'));
  }, 3000);
}

connect().catch(onConnErr);
