# claude-subscription-proxy

把 **Claude Code 订阅账号（Max / Pro）** 包装成一个本地的
**Anthropic / OpenAI 兼容 HTTP API**，让脚本 / IDE 插件 / LiteLLM
通过标准 API 调用 Claude，而 Anthropic 把这些请求当作 **Claude
Code CLI 的交互式调用**，按订阅配额计费 —— 不走 API/SDK 配额。

```
你的应用 / LiteLLM / SDK                    本服务                          Anthropic
──────────────────────                     ─────                          ─────────
  curl ────┐                              ┌──────────────┐
  Anthropic SDK ──────► HTTP/JSON ───────►│  FastAPI     │
  OpenAI SDK ───┘                         │  :8787       │
  LiteLLM ─────►                          └──────┬───────┘
                                                 │ JSON-lines IPC
                                                 ▼
                                          ┌──────────────────────┐
                                          │ per-user worker      │
                                          │  · mitmproxy 18000+N │  劫持 + 改写
                                          │  · PTY → claude code │──► HTTPS ──► api.anthropic.com
                                          │  (独立 asyncio loop)  │   (cc_entrypoint=cli)
                                          └──────────────────────┘
```

---

## 目录

1. [实现原理](#1-实现原理)
2. [Docker 部署](#2-docker-部署)
3. [配置说明](#3-配置说明)
4. [API 使用](#4-api-使用)

---

## 1. 实现原理

### 1.1 为什么不能直接拼请求

Anthropic 服务器用一组**请求级身份指纹**判定本次调用是订阅配额还是 API
配额。最关键的几类：

| 指纹 | 内容 |
|---|---|
| `system[0]` 文本块 | `x-anthropic-billing-header: cc_entrypoint=cli; cc_version=2.1.x; ...` |
| `User-Agent` | `claude-cli/2.1.x (external, cli)` |
| 请求头 | `anthropic-beta` / `x-stainless-*` / `x-app` 等十多项 |
| 请求体其它字段 | 完整 `tools` 列表（11 个内建工具）、`metadata`、`anthropic_version` 等 |

任意一项缺失或不匹配就会被路由到 API 配额。手工拼请求难以保证全套指纹
长期稳定（CLI 升级常改 beta token、header 组合、tools schema），所以本
项目的核心思路是：

> **让真实的 `claude` CLI 在受控环境里发请求，用 mitm 在它出门前把 body
> 里业务字段换成 API 调用方发来的内容**。

### 1.2 整体数据流

```
        ┌─────────────────────── 主进程：FastAPI ──────────────────────┐
        │  /v1/messages   /v1/chat/completions   SessionManager       │
        └────┬─────────────────────────────────────────────────┬──────┘
             │ stdin JSON 行（请求 body）           stdout JSON 行 │
             ▼                                                  ▲
        ┌─────────────────── 子进程：worker（每用户一个）─────────────┐
        │                                                            │
        │   ┌──────────┐                       ┌──────────────────┐  │
        │   │ claude   │  HTTPS_PROXY=18000+N  │   mitmproxy      │  │
        │   │  TUI     │ ─────────────────────►│   HijackAddon    │  │
        │   │  (PTY)   │   trust mitm CA       │                  │  │
        │   └────▲─────┘                       └────┬─────────────┘  │
        │        │ ① 占位符 "say hi\r"               │ ② 改写 body     │
        │   trigger()                                 │ ③ tap 响应     │
        │                                             ▼               │
        └─────────────────────────────────────────────│───────────────┘
                                                      ▼ HTTPS
                                                api.anthropic.com
```

一次调用的步骤：

1. HTTP 请求落到 FastAPI，鉴权后从 user 池里挑一个空闲 worker
2. 主进程把请求 body 以 JSON 行写给 worker 的 stdin
3. worker 把 body 挂在 `session.pending` 槽位上，然后往 claude PTY 写
   `say hi\r`，触发 claude 发出一个真实的 `POST /v1/messages?beta=true`
4. mitm 拦截这个出站请求，识别出是用户触发的那一个（看 pending 槽位），
   按白名单合并 body（§1.5）后转发
5. Anthropic 用 SSE 流回应，mitm 的 `_tap` 回调把每个 chunk **同时**
   forward 给 claude（保持 TUI 状态机一致）和塞进 `ResponseChannel`
6. worker 把 channel 里的字节 base64 后写 stdout，主进程读出来转发给
   FastAPI 的 `StreamingResponse`

### 1.3 mitm 怎么物理上拦下 HTTPS

claude CLI 直发 `https://api.anthropic.com/v1/messages` 时，普通 HTTPS
握手有一道防护：客户端只信任系统 / 受信 CA 签的证书，任何中间人冒充
api.anthropic.com 都签不出能验证通过的证书。

mitmproxy 的做法是**让客户端事先信任 mitmproxy 自己的 CA**：

1. mitmproxy 首次运行生成 CA 私钥 + 自签证书 `mitmproxy-ca-cert.pem`
2. 通过环境变量告诉 claude（Node 应用）信任这个 CA：

```python
# src/pty_driver.py:37-42
env["HTTPS_PROXY"]         = f"http://127.0.0.1:{port}"   # 流量物理上走代理
env["HTTP_PROXY"]          = f"http://127.0.0.1:{port}"
env["NODE_EXTRA_CA_CERTS"] = str(ca_cert)                 # 信任 mitm 自签 CA
```

Node 看到 `NODE_EXTRA_CA_CERTS` 后把 mitm CA 加进可信列表 —— 跟
DigiCert / Let's Encrypt **平起平坐**。流量经过 mitm 的物理路径是 HTTP
标准的 `CONNECT` 隧道：

```
1. claude 跟 mitm 建明文 TCP 连接
2. claude 发 → CONNECT api.anthropic.com:443 HTTP/1.1
3. mitm  答 → HTTP/1.1 200 Connection Established
4. 之后这条 TCP 通道按规范应当是"裸 TCP 隧道"
```

但 mitmproxy 不按规矩走 —— 它从 ClientHello 里读出 SNI =
`api.anthropic.com`，现场用自己 CA 私钥**签一张假证书**塞回去：

```
claude  ←TLS(假证书)→  mitmproxy  ←TLS(真证书)→  api.anthropic.com
         明文给 mitm                明文给 mitm
                       mitm 持两端密钥
                       双向都能看明文 / 改字节
```

mitmproxy 是**两端独立 TLS 会话的端点**（不是字节转发），所以能解明文、
改 body、tap 响应字节。整套机制依赖四件事齐全：

```
1. HTTPS_PROXY → 让流量物理上经过 mitm
2. NODE_EXTRA_CA_CERTS → 让 Node 信任 mitm 假证书
3. mitm 持 CA 私钥 → 能现场签任意域名证书
4. mitmproxy 的 request / responseheaders hook → 在两端中间改字节
```

每一环单独看都很无聊，**凑齐这四个，TLS 加密的安全前提就被打破了**。
注意这套机制**只对设了上述环境变量的客户端有效**：同机器上其它
HTTPS 程序（系统 curl 等）连 anthropic.com 仍然会拒绝 mitm 假证书 ——
hijack 只在 worker 内 claude 这条特定路径上发生。

### 1.4 worker 怎么"借身份"发请求

worker 进程里跑着 mitmproxy + claude PTY + 一个 IPC 读循环。它对外提供
的接口（`src/worker.py:79-90`）很简单：

```python
async def call(self, body):
    async with self.lock:
        self.response = ResponseChannel()        # 给 mitm 准备的响应字节 queue
        self.pending  = PendingRequest(body=body)# 待替换的用户 body 挂在槽位上
        await self.pty.trigger()                 # 写 "say hi\r" 给 claude PTY
        await pending.consumed.wait()            # 等 mitm 来认领（最多 30s）
        return self.response
```

`session.pending` 是 worker 和 mitm addon 之间的**共享内存信号**。addon
持有 session 反向引用（`src/mitm/addon.py:66`），随时能读这个槽位。

`pty.trigger()` 往 claude TUI 的伪终端写入 7 个字节 `"say hi\r"`
（`src/pty_driver.py:106-119`）。claude 不知道这是程序写的，它把它当
"用户在键盘上敲了一句话"处理 —— 走自己的代码路径，生成一个
**完整的、带订阅身份指纹的** `POST /v1/messages?beta=true` 包，准备发
给 api.anthropic.com。

这个包穿过 `HTTPS_PROXY` 落到本进程的 mitm。

### 1.5 mitm 怎么"掉包"业务字段

mitmproxy 11 在请求 body 已经收齐、还没转发去 anthropic 之前，调用所有
addon 的 `request(flow)` 钩子。我们的 `HijackAddon.request`
（`src/mitm/addon.py:73-134`）做这些事：

```python
def request(self, flow):
    # ① host/path 过滤
    if flow.request.host not in ANTHROPIC_HOSTS: return        # 不是 anthropic
    if bare_path != "/v1/messages":              return        # 不是模型调用

    # ② 槽位检查
    pending = self.session.pending
    if pending is None:
        return  # 没人挂便签 = claude bootstrap 自己发的杂请求，原样放行

    # ③ 合并 body（白名单覆盖，详见 1.6）
    merged = self._merge_body(flow.request.get_text() or "{}", pending.body)
    merged["stream"] = True                       # 强制流式才能 tap
    flow.request.set_text(json.dumps(merged))
    flow.request.headers["accept-encoding"] = "identity"   # 关 gzip 简化 tap

    # ④ 剥掉 context-1m beta（订阅不覆盖长上下文 pay-as-you-go）
    beta = flow.request.headers.get("anthropic-beta")
    kept = [t for t in beta.split(",") if not t.strip().startswith("context-1m")]
    flow.request.headers["anthropic-beta"] = ",".join(kept)

    # ⑤ 认领 flow，唤醒 worker
    self._active_flow_id = flow.id
    pending.consumed.set()           # ← §1.4 里的 wait() 在这里被唤醒
    self.session.pending = None      # ← 关闭槽位，避免 claude 后续请求被误劫持
```

**三个关键设计**：

**(a) "槽位认领"区分用户请求和噪音流量。** worker 进程里 mitm 同时
能看到几十个 flow：claude 自己的 telemetry、npm registry、mcp-registry、
datadog 心跳……单靠 host/path 过滤都会误伤。pending 槽位是用户请求的
显式信号 —— pending=None 就放行不动，pending≠None 才是"等我们替换内容
的那一封信"。槽位用一次清一次，避免后续杂请求被错误识别。

**(b) `_active_flow_id` 跟踪同一 flow 的请求/响应两阶段。** 请求阶段记下
`flow.id`，响应阶段（§1.7）只对 `flow.id == _active_flow_id` 的那个流挂
tap。其它响应原样放行。

**(c) 强制 `stream=true`。** mitmproxy 的 `flow.response.stream` 机制
只对流式响应有 chunk 级回调；非流响应到 mitm 时已经是完整 body 了，没法
在 chunk 级 tap。所以无论客户端要不要流式，对邮局**强制声明流式**。
最终用户拿到的格式由 FastAPI 层决定 —— 客户端要非流式，FastAPI 把流式
字节攒齐重组成 JSON（§1.8）。

### 1.6 body 合并的规则表

`_merge_body`（`src/mitm/addon.py:136-187`）从 claude 原始 body 开始，
按"白名单"覆盖字段：

| 字段类别 | 谁说了算 | 原因 |
|---|---|---|
| `messages` / `model` / `max_tokens` / `temperature` / `top_p` / `top_k` / `stop_sequences` / `tool_choice` / `tools` / `stream` | 用户 | 业务输入 |
| `system` | **混合** | 见下文 `_merge_system` |
| `metadata` / `anthropic_version` / `thinking` / `output_config` / `context_management` / ...其它所有 | claude 原值 | identity 指纹 |
| 所有 `User-Agent` / `anthropic-*` / `x-stainless-*` / `x-app` 等 header | claude 原值 | identity 指纹 |

> 注：客户端**没传** `tools` 时，claude 内置的 11 个工具（Bash/Read/Edit/...）
> 会原样留在请求里 —— 模型可能去调它们。要彻底禁工具：传
> `"tool_choice": {"type": "none"}`，或传你自己的 `tools: [...]` 覆盖。

`system` 字段的特殊处理（`_merge_system`，`src/mitm/addon.py:189-227`）：

claude 原始 `system` 是个数组，长这样：

```
system: [
  ★ 块 0: "x-anthropic-billing-header: cc_entrypoint=cli; cc_version=2.1.x; ..."
    块 1: "You are Claude Code, Anthropic's official CLI..."
    块 2: "<persona / instructions / 内置工具说明>"
    ...
]
```

两层角色完全不同：

| | 块 0（计费头） | 块 1+（人设） |
|---|---|---|
| 给谁看 | Anthropic 计费系统 | **模型本身**（影响生成） |
| 内容形式 | 机器可读元数据 | 自然语言指令 |
| 撤掉的后果 | 账单从订阅切到 API 配额 | 模型不再扮演 Claude Code |

合并逻辑：

```
保留块 0（计费指纹）+ 丢弃块 1+（人设）+ 追加用户的 system 块
            ↓
[ 块 0: "cc_entrypoint=cli; ..."           ,
  块 N: <user_system>, "cache_control":{"type":"ephemeral"} ]
```

效果：

- 计费头还在 → Anthropic 看到 VIP → 计费走订阅 ✓
- claude 人设被丢 → 模型不会以为自己在做编程 ✓
- 用户 system 加进去 → 用户指令真正生效 ✓
- 用户 system 带 `cache_control` → Anthropic 把它缓存起来，后续同
  system 的调用 `cache_read_input_tokens` 增长，便宜 10 倍

> 客户端**没传** `system` 时，claude 原 system（全部块）原样保留 ——
> 模型会按 Claude Code 人设回答（爱用 Bash/Read/Edit 等）。

合并里还有一段**模型耦合参数清理**：claude CLI 会塞 `output_config`
`thinking` `context_management` 这类跟它当前 model 绑定的字段（例如
`output_config={"effort":"xhigh"}` 是 opus 级、sonnet 拒绝接受）。
当用户覆盖 model 时，这些字段会被丢掉让新 model 用自己的默认，避免
"sonnet 收到 opus 专用参数"导致 400 错误。

### 1.7 响应回程：双向喂字节

mitmproxy 11 在响应头到达时调用 `responseheaders(flow)` 钩子
（`src/mitm/addon.py:231-264`）：

```python
def responseheaders(self, flow):
    if flow.id != self._active_flow_id:
        return    # 不是我们认领的那个 flow，不挂 tap

    channel = self.session.response   # §1.4 worker 挂的那个 queue

    def _tap(data: bytes) -> bytes:
        if data:
            channel.queue.put_nowait(bytes(data))  # ← 塞进 worker channel
            return data                            # ← 同时让 mitm 继续转发给 claude
        # data == b"" → 流结束
        channel.queue.put_nowait(None)             # ← 哨兵
        return b""

    flow.response.stream = _tap   # mitmproxy 每个 chunk 调用一次
```

`flow.response.stream = callable` 是 mitmproxy 11 的契约：mitm 每收到
一个响应 chunk 都调用一次 `_tap(data)`，传 `b""` 表示流结束。

**为什么字节要双向喂？**

- **给 channel.queue**：这是流向用户的路径（worker → 主进程 → FastAPI）
- **return data 给 mitm**：让 mitm 继续把这个 chunk **转发给 claude TUI**

claude TUI 内部有完整会话状态机：它发了一次请求，期望看到完整 SSE 响应
（`message_start` → 多个 `content_block_delta` → `message_stop`）。如果
它"瞎了"，下一次 keystroke 处理会触发错误恢复路径，可能阻塞或重连。
让它看见响应字节是最简单的同步方式 —— 字节透传给它内部消化，但 TUI 输出
渲染我们用 `_drain` 全部丢弃（`src/pty_driver.py:121-148`），不读不看。

### 1.8 IPC：worker → 主进程 → 客户端

worker 子进程拿到响应字节后，按 `src/worker.py:100-118` `_handle`
处理：

```python
async def _handle(session, req_id, body):
    channel = await session.call(body)
    async for chunk in channel.iter():    # 从 _tap 塞进来的字节里循环
        await _send({"type": "chunk", "id": req_id,
                     "data": base64.b64encode(chunk).decode("ascii")})
    await _send({"type": "end", "id": req_id})
```

`_send` 写一行 JSON 到 stdout。base64 是因为 SSE 字节有换行 / 控制字符，
JSON 字符串包不住。完整 IPC 协议（worker.py 文件头注释）：

```
─→ {"type":"request","id":N,"body":{...}}            主进程 → worker stdin
←─ {"type":"chunk","id":N,"data":"<base64 SSE>"}     worker stdout → 主进程
←─ {"type":"end","id":N}
←─ {"type":"error","id":N,"msg":"..."}
启动时：
←─ {"type":"ready"}
```

主进程的 `ClaudeSession` spawn 出 worker 时启动一个常驻协程读 worker
stdout（`src/session/session.py:187-228`）：

```python
async def _read_loop(self):
    while True:
        line = await self.proc.stdout.readline()
        msg  = json.loads(line)
        req_id = msg.get("id")
        channel = self._channels.get(req_id)   # 主进程侧 channel
        if msg["type"] == "chunk":
            channel.queue.put_nowait(base64.b64decode(msg["data"]))
        elif msg["type"] == "end":
            channel.queue.put_nowait(None)
            self._channels.pop(req_id, None)
```

注意：**worker 里有一个 ResponseChannel，主进程也有一个 ResponseChannel**
—— 两边都是同一个 `asyncio.Queue` 包装，但实例不同。worker 那个被 mitm
`_tap` 喂；主进程那个被 `_read_loop` 喂。中间靠 JSON 行 IPC 连起来。

FastAPI handler 把主进程 channel 的字节转 HTTP 响应
（`src/api/anthropic.py:18-36`）：

```python
@router.post("/v1/messages")
async def messages(req, pool: list[str] = Depends(auth_dep)):
    body = await req.json()
    sess = await manager.pick(pool)
    channel = await sess.call(body)

    if body.get("stream"):
        # 流式：StreamingResponse 直接 yield 字节
        async def gen():
            async for chunk in channel.iter(): yield chunk
        return StreamingResponse(gen(), media_type="text/event-stream")

    # 非流式：_collapse_stream 把 SSE 事件重组回单个 Message JSON
    return JSONResponse(await _collapse_stream(channel))
```

`_collapse_stream`（`src/api/anthropic.py:41-153`）顺序消费 SSE 事件
（`message_start` / `content_block_delta` / `message_delta` / `message_stop` /
`error`），合并 `content_block_delta` 的 text 增量，保留 `usage`，捕获
`error` 事件转成 `stop_reason: "error"` 的合法 Message，让客户端不会因
为空响应而崩。

### 1.9 多 worker pool：单 key 服务端负载均衡

一个 token 可以映射到一个 user_id **池**（`src/session/manager.py:41-66`
`pick()`）：

```yaml
users:
  sk-internal-xxx:                 # 一个 key
    - litellm-0                    # 三个 worker 并行
    - litellm-1
    - litellm-2
```

LiteLLM / 客户端只配一个 key，本服务收到请求后从池里挑一个 worker：

```python
async def pick(self, pool):
    if len(pool) == 1:
        return await self.get_or_create(pool[0])
    sessions = [await self.get_or_create(u) for u in pool]
    idle = [s for s in sessions if not s._channels]
    if idle:
        # 空闲 worker 之间 round-robin
        idx = self._rr.get(tuple(pool), 0) % len(idle)
        self._rr[tuple(pool)] = idx + 1
        return idle[idx]
    # 全忙：选 _channels 最少的，多个同 min 时 RR
    min_inflight = min(len(s._channels) for s in sessions)
    candidates = [s for s in sessions if len(s._channels) == min_inflight]
    idx = self._rr.get(tuple(pool), 0) % len(candidates)
    self._rr[tuple(pool)] = idx + 1
    return candidates[idx]
```

选址策略：

1. **优先 idle**：扫池，挑没有 in-flight 请求的 worker
2. **多个 idle 之间 round-robin**：避免请求总集中到 pool[0]
3. **全忙 fallback**：选 in-flight 最少的；同 min 时 RR

容器启动时所有池成员都会 serial 预热（每个 ~10s）。

### 1.10 凭据共享：一份 OAuth，所有 worker 共用

每个 user 有自己的 HOME `/data/users/<user_id>`，避免 claude CLI 写
`.claude.json` 时互相冲突。但 OAuth 凭据是一份共用的 ——
`_seed_home`（`src/session/session.py:88-139`）把每个 user 的
`$HOME/.claude` **整个目录**软链接到操作员的 `/home/coder/.claude/`：

```
/data/users/litellm-0/.claude → /home/coder/.claude   (symlink)
/data/users/litellm-1/.claude → /home/coder/.claude
/data/users/litellm-2/.claude → /home/coder/.claude
                                      │
                                      └── 容器 bind mount → 宿主机
                                          /data/shared-auth/claude/
```

为什么是**目录级**而不是文件级 symlink：claude CLI 刷新 token 用的是
"写 tmp + rename" 的原子写法，文件级 symlink 会被 rename 直接覆盖成
真文件，导致 refresh 写不回操作员源。目录级 symlink 让 rename 发生在
共享目录内部，新 token 直接落到 `/home/coder/.claude/.credentials.json`
（也就是宿主机 `/data/shared-auth/claude/.credentials.json`），所有
worker 立即看到新 token，容器重启也还在。

per-user transcripts / sessions 仍然天然隔离 —— claude 把 sessions 存在
`.claude/projects/<encoded-cwd>/sessions/<id>.jsonl`，每个 worker 的 cwd
是自己 HOME，encoded-cwd 不同，子目录就分开。

**OAuth refresh_token 轮换竞态** —— claude CLI 自己拿 in-memory RT 去刷
新 token 时，多 worker 撞同一秒过期会产生竞争：A 用 RT0 刷成功拿 RT1，
B 用 RT0 去刷收 `invalid_grant`，B 此后所有请求 401。本服务用
**主进程集中刷新**（`src/oauth_refresh.py` `OAuthRefresher`）解决：

- 主进程后台任务每 5 分钟检查 `.credentials.json` 的 `expiresAt`
- 距离过期 < 1 小时就发 `POST https://api.anthropic.com/v1/oauth/token`
  （CLIENT_ID 是 claude code 公开的 `9d1c250a-…`，从 CLI binary 里反编
  译出来），原子写回共享目录
- worker 的 `claude.restart_interval_seconds` 默认 4 小时（< 8 小时
  token 寿命），保证 worker 内存里 token 一定还没过期就被替换 ——
  worker 永远不需要自己刷
- **唯一 /v1/oauth/token 写入者就是主进程** → 物理上不可能撞 RT 轮换

`oauth_refresh.enabled` 默认 true。设为 false 后 worker 回退到自刷新，
竞态会重现 —— 仅用于排障，生产不应关闭。

详见 §3.7。

### 1.11 可靠性 / 自愈机制

claude TUI + 订阅 OAuth 这条路径有一组反复出现的失败模式：TUI 卡错误屏
吞了 keystroke，上游 429 给了小错误信封但 TCP 不关，账号被限流让
请求悄悄挂起永不回，等等。Proxy 不能假设每发请求都健康完成，必须能自
己接住。当前的兜底层：

| 层 | 触发条件 | 行为 |
|---|---|---|
| **mitm intercept timeout**<br>（`worker.py` `WorkerSession.call`） | PTY 触发后 30s 内 mitm 没拦到 `/v1/messages` | 关闭 channel + 清 pending 槽位。覆盖故障 A：TUI 卡 modal/错误屏吞了 keystroke |
| **stall watchdog**<br>（`mitm/addon.py` `_FlowState`） | hijack 成功后 90s 内没有任何上游 chunk（或两个 chunk 间隔 > 90s） | 强制给 channel 发 None，唤醒 worker 的 `_handle`。覆盖故障 B：上游沉默不回 response headers；故障 C：上游回了小错误信封后挂起 |
| **error hook**<br>（`mitm/addon.py` `error`） | mitm 检测到 flow 级错误（TLS 失败、连接 reset、服务端早期 hangup） | 立即关闭 channel，不等 watchdog |
| **bootstrap prewarm**<br>（`session/manager.py` `_prewarm_bootstrap`） | worker 启动 / 重启 / revive 后立刻 | 发一发 haiku/max_tokens=1 假请求，把 claude CLI 的 lazy bootstrap（eval/grove/penguin_mode/mcp-registry 等 6 个 sibling HTTP 调用）打完。否则首发用户请求会跟 bootstrap 一起在 ~30ms 内发 7+ 个调用，触发 OAuth per-account 限速 |
| **auto-revive dead worker**<br>（`session/manager.py` `get_or_create`） | 下一发请求到来时检测到 `proc.returncode is not None` | 复用同 mitm 端口就地 restart + prewarm。覆盖 claude CLI crash / OOM kill / mitm 故障 |
| **scheduled restart**<br>（`session/manager.py` `_restarter`） | worker age > `restart_interval_seconds`（默认 4h） | 等 in-flight 排空（最多 60s）→ restart → prewarm。清掉 Ink 缓冲、内存 transcript、缓存的访问 token |
| **OAuthRefresher**<br>（`oauth_refresh.py`） | 主进程后台任务，每 5 min 检查 token 寿命 < 1h | 集中刷新写回共享 .credentials.json。详见 §1.10 |
| **`/admin/workers/{id}/restart`**<br>（`api/admin.py`） | 运维手动调用 | drain in-flight → restart → prewarm，单 worker 立即恢复 |

**关键认知**：watchdog 只**解开当前那一发卡住的请求**，让 worker 重新
显示 IDLE，**不重启 worker 进程**。如果 claude TUI 卡在 error state，
下一发请求过来 PTY 触发可能再 stall 一次 —— 真正修 TUI 状态要靠定时
restart（4h）或手动 `POST /admin/workers/{id}/restart`。

**根因 vs 兜底**：这些机制是兜底，不是根因修复。多数 stuck 的根因是
**单 OAuth 账号被多 worker 打超并发限**（订阅大约 2-3 并发上限）。如果
你 pool 配 10 worker 跑一个账号，大部分请求会被 429，watchdog 让代理
不死但请求实际是空响应。彻底解决方向：降 worker 数到 ≤ 3，或加多账号
池（暂未实现）。

### 1.12 几个关键设计决策

**为什么 worker 要独立子进程？**
mitmproxy 11 的 DumpMaster + ptyprocess 跟 FastAPI 主循环放一起会出现
`flow.response.stream` 回调调度异常 + PTY 写字节静默丢失。每个用户一个
独立 `asyncio.run()` 子进程绕开，副作用是顺带获得崩溃隔离。

**为什么用 PTY 而不是直接拼请求？**
让真 CLI 跑一遍能保证十几类身份指纹都是它当前版本会发的真值，CLI 升级
后自动跟上，proxy 代码不用动。

**为什么不直接在 mitm 里造响应、跳过 claude？**
claude TUI 内部有完整状态机（会话历史、tool 调用栈），如果它发了请求
却没收到响应，下一次 keystroke 会触发错误恢复路径，可能阻塞或重连。
让它"看见"响应字节是最简单的同步方式。

**双重 lock 串行化**
- 主进程 `ClaudeSession.lock`（session.py）：只在 stdin 提交那一瞬间
  持有，保证 IPC 写入不交错
- worker `WorkerSession.lock`（worker.py）：从 trigger 一直持到
  pending.consumed 触发，保证同一 claude PTY 一次只挂一个 PendingRequest

### 1.13 关键代码索引

| 想看什么 | 文件 |
|---|---|
| body 合并规则 | `src/mitm/addon.py` `_merge_body` |
| system 字段合并 | `src/mitm/addon.py` `_merge_system` |
| 响应流 tap + stall watchdog | `src/mitm/addon.py` `_FlowState` / `responseheaders` / `_tap` / `error` |
| 用户可覆盖的 body 字段白名单 | `src/mitm/addon.py` `USER_OWNED_BODY_FIELDS` |
| worker 主循环 + IPC 协议 | `src/worker.py` |
| claude TUI 启动 + 等 `❯` | `src/pty_driver.py` `_wait_until_ready` |
| 触发占位符让 claude 发请求 | `src/pty_driver.py` `trigger` |
| 每用户 HOME seed（共享 `.claude/` 目录） | `src/session/session.py` `_seed_home` |
| 主进程 session：spawn worker + 读 stdout + 拆 `_submit`/`call` | `src/session/session.py` |
| user 池负载均衡 + 自动 revive + scheduled restart | `src/session/manager.py` `pick` / `get_or_create` / `_restarter` |
| bootstrap prewarm（启动 / revive / restart 三处都跑） | `src/session/manager.py` `_safe_prewarm` / `_prewarm_bootstrap` |
| 集中 OAuth refresh | `src/oauth_refresh.py` `OAuthRefresher` |
| SSE → 完整 Message 重组（含 error 兜底） | `src/api/anthropic.py` `_collapse_stream` |
| OpenAI ↔ Anthropic 字段转换（含 usage 终态 chunk） | `src/api/translate.py` `anthropic_sse_to_openai_sse` |
| `/healthz` `/status` `/admin/*` | `src/main.py` + `src/api/admin.py` |
| token → user 池 + 头部解析 | `src/auth.py` + `src/config.py` |
| 所有 timeout 配置项 | `src/config.py` `TimeoutConfig` + `OAuthRefreshConfig` |

---

## 2. Docker 部署

部署后的关键事实：

| 项 | 值 |
|---|---|
| 对外端口 | **`18787`**（宿主机）→ 容器内 `8787` |
| 容器运行用户 | `coder`，**uid/gid 1000**（compose 里 `user: "1000:1000"`） |
| 凭据来源 | 宿主机 `/data/shared-auth/claude` → bind 到容器 `/home/coder/.claude`（**读写**，token 刷新要写回） |
| per-user 状态 | 项目目录下 `./users/` → bind 到容器 `/data/users` |
| mitm CA | 具名卷 `mitm-ca` → 容器 `/home/coder/.mitmproxy`，重启保留 |
| 配置文件 | 项目目录下 `./config.yaml` → bind 到容器 `/data/config.yaml`（只读） |

### 2.1 前置条件

部署机器：

- Docker 24+ 和 Docker Compose v2
- 宿主机端口 `18787` 可用
- 容器内端口段 `18000..18000+N`（N = 用户数）保留给 mitm，不对外暴露

另需一台已经 `claude /login` 完成 OAuth 的机器（凭据可以拷过去）。

### 2.2 准备 operator 凭据目录

整个 `~/.claude/` 目录会被 bind 进容器，所以**拷整个目录**，不是单文件。

```bash
# 在部署机上
sudo mkdir -p /data/shared-auth

# 从已登录机器把整个 .claude 目录拷过来
sudo cp -r <已登录用户的 HOME>/.claude  /data/shared-auth/claude

# 确认凭据文件在
ls -l /data/shared-auth/claude/.credentials.json
```

> **凭据共享机制**：容器以 uid 1000(`coder`) 运行，
> `/data/shared-auth/claude` 整个目录 bind 到容器的 `/home/coder/.claude`
> （**读写**）。每个 API 用户的 worker 启动时，`_seed_home` 把
> `$HOME/.claude` **整个目录**软链接到这个共享目录。所有 user 共用同一
> 份 OAuth 凭据；claude CLI 刷新 token 时（写 tmp + rename）会原子地
> 写回这个共享目录 —— 新 token 对所有 worker 和容器重启都立即生效。

如果你在源头**重新登录**（refresh token 被轮换作废），需要把新的
`.claude/` 重新同步到 `/data/shared-auth/claude`。

### 2.3 写 config.yaml

```bash
cp config.example.yaml config.yaml
# 或者 cp docker/config.example.yaml config.yaml
# 两个 example 都已经是容器内绝对路径，可以直接 cp 后只改 users 字段
```

生成 API key：

```bash
echo "sk-internal-$(openssl rand -hex 24)"
```

最终 `config.yaml`（**Docker 部署必须用绝对路径**）：

```yaml
listen_host: 0.0.0.0
listen_port: 8787

mitm:
  port_base: 18000
  ca_cert: /home/coder/.mitmproxy/mitmproxy-ca-cert.pem

claude:
  binary: claude
  home_template: /data/users/{user_id}
  # 4h: worker 周期性就地重启清掉 CLI 状态。必须 < OAuth token 寿命
  # (~8h)，否则 worker 内存里 token 过期会尝试自刷新跟主进程 refresher
  # 撞 RT 轮换（详见 §1.10 / §3.7）。
  restart_interval_seconds: 14400

  # 所有 timeout 都有合理默认；只在确实看到对应失败时再调（详见 §3.6）
  # timeouts:
  #   mitm_intercept_seconds: 30
  #   status_stall_seconds: 30
  #   response_stall_seconds: 90
  #   restart_drain_seconds: 60
  #   worker_ready_seconds: 60
  #   prewarm_seconds: 60
  #   restart_check_interval_seconds: 60

# 集中 OAuth 刷新；关掉的话 worker 自刷新，RT 轮换 race 会重现（详见 §3.7）
# oauth_refresh:
#   enabled: true
#   check_interval_seconds: 300
#   refresh_when_expires_within_seconds: 3600

# bearer token -> user_id (scalar) 或 user_id 列表 (pool)
users:
  sk-internal-abc...: alice                      # 单 worker
  sk-internal-def...:                            # 3 worker 池，自动负载均衡
    - litellm-0
    - litellm-1
    - litellm-2
```

> `ca_cert` 和 `home_template` **必须是绝对路径**。写成 `./` 或 `~`
> 开头，entrypoint 会拒绝启动 —— worker 子进程的 CWD 是 `/app`
> （root 所有、不可写），相对路径会被解析到那里然后崩。

> **pool 大小要保守**：Anthropic 订阅有 per-OAuth 隐含并发限制（约 2-3
> 并发）。pool 配 10 个 worker 共用一个账号 = 7-8 倍超限，大部分请求
> 会被 429 而 stuck。建议单账号 pool ≤ 3。

### 2.4 宿主机目录权限

compose 里 `user: "1000:1000"` 让容器**直接以 uid 1000 启动** ——
所以容器要写的 bind 目录必须在宿主机侧就归 uid 1000：

```bash
cd claude-subscription-proxy

# per-user 状态目录
mkdir -p ./users
sudo chown -R 1000:1000 ./users

# 凭据目录：claude CLI 刷新 token 要写回
sudo chown -R 1000:1000 /data/shared-auth/claude
```

| 宿主机目录 | 容器内路径 | 权限要求 |
|---|---|---|
| `./users` | `/data/users` | uid 1000 **可写** |
| `/data/shared-auth/claude` | `/home/coder/.claude` | uid 1000 **可读写** |
| `mitm-ca`（具名卷） | `/home/coder/.mitmproxy` | 首次自动从镜像填充 |
| `./config.yaml` | `/data/config.yaml` | uid 1000 可读即可 |

> Docker bind mount **不做 uid 翻译**：宿主机文件的数字 uid 原样出现在
> 容器里。容器内的 `coder` 是 uid 1000，所以宿主机目录必须也归 1000。

不想管这些权限：去掉 compose 里 `user: "1000:1000"` 那行，让容器以
root 启动；entrypoint 会自动 `chown` 好后 `gosu` 降权到 `coder`。

### 2.5 构建并启动

```bash
docker compose up --build -d
docker compose logs -f proxy
```

首次构建 4–6 分钟（下载 Node 20 + claude code npm + python 包）。
之后 `docker compose up -d` 秒级启动。

健康检查：

```bash
curl -s http://127.0.0.1:18787/healthz
# → {"ok":true,"sessions":["litellm-0","litellm-1","litellm-2"],
#    "claude_version":"2.1.139 (Claude Code)"}
```

启动后容器会**预热所有配置的 user**（每个 ~10s，serial），日志里能看到：

```
oauth refresher started credentials=/home/coder/.claude/.credentials.json check_interval=300s refresh_when_expires_within=3600s
linked /data/users/litellm-0/.claude -> /home/coder/.claude (shared with operator)
session up user=litellm-0 worker_pid=N mitm_port=18000
bootstrap prewarm starting user=litellm-0
bootstrap prewarm complete user=litellm-0
prewarmed user=litellm-0
... (其它 worker 同样)
```

`bootstrap prewarm` 这一步会发一发 haiku/max_tokens=1 假请求，让 claude
CLI 把 lazy bootstrap（6+ 个 sibling HTTP 调用）打完，避免首发真实请求
跟它们撞同一秒触发 OAuth per-account 限速（详见 §1.11）。

### 2.6 发第一个请求

```bash
KEY=sk-internal-...
curl -sS -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-opus-4-7",
    "max_tokens": 256,
    "messages": [{"role":"user","content":"用一句话介绍你自己"}]
  }'
```

**预期**：

- 首次请求 ~7s（worker 冷启动：mitm + claude TUI + Ink 稳定 2.5s）
- 后续请求 1–2s TTFB
- 响应 JSON 含 `"stop_reason": "end_turn"` 和 `cache_read_input_tokens > 0`
  （命中 CLI 系统 prompt 缓存的证据，说明走的是订阅配额）

### 2.7 验证 user 池负载均衡

并发打三发，看哪几个 worker 接到：

```bash
KEY=sk-internal-...
for i in 1 2 3; do
  curl -sS -X POST http://127.0.0.1:18787/v1/messages \
    -H "Authorization: Bearer $KEY" \
    -d '{"model":"claude-sonnet-4-6","max_tokens":16,
         "messages":[{"role":"user","content":"hi"}]}' >/dev/null &
done
wait
docker compose logs --tail=300 proxy | grep -E "submitted req|hijacked outbound"
```

期望看到三行不同 user：

```
INFO src.session.session   user=litellm-0 submitted req id=N
INFO src.session.session   user=litellm-1 submitted req id=M
INFO src.session.session   user=litellm-2 submitted req id=K
INFO worker:src.mitm.addon user=litellm-0 hijacked outbound /v1/messages flow=...
INFO worker:src.mitm.addon user=litellm-1 hijacked outbound /v1/messages flow=...
INFO worker:src.mitm.addon user=litellm-2 hijacked outbound /v1/messages flow=...
```

### 2.8 停止 / 重启 / 加用户

```bash
docker compose down              # 停止；./users/ 里的 per-user 状态保留
docker compose restart proxy     # 改了 config.yaml 后热重启
docker compose down -v           # 同时清除 mitm CA 卷（下次重新生成）
```

**加用户**：在 `config.yaml` 的 `users:` 下加一行 / 加进 list，然后
`docker compose restart proxy`。新 user 首次请求时 `./users/<id>/`
自动创建，`.claude/` symlink 自动建立到操作员源。

### 2.9 暴露给其他主机

`docker-compose.yml` 默认把 `18787` 绑到 `0.0.0.0`，**局域网内其他主机
可以直接访问**（用来接 LiteLLM 等）。强烈建议：

- 仅在受信任的内网开放
- 或前置一层反向代理（nginx / caddy）做 TLS + IP 白名单
- **绝不要暴露到公网** —— 项目无速率限制，且违反 ToS

仅本机访问：把 `ports` 改成 `"127.0.0.1:18787:8787"`。
换端口：改 `ports` 左边的 `18787`，容器内的 `8787` 保持不动。

### 2.10 运维：`/status` 和 `/admin/*`

`/healthz` 保持开放（给 docker / k8s liveness probe 用）。`/status` 和
所有 `/admin/*` **需要鉴权** —— 复用 §3.2 配置的任何一个 API key。

**`/status`** —— 每 worker 实时状态，区分 IDLE / WORKING / STUCK：

```bash
KEY=sk-internal-...
curl -s -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/status | jq -r '
  .workers[] |
  if .in_flight == 0 then
    "🟢 IDLE     \(.user_id)  idle=\(.idle_seconds)s  total=\(.total_requests)"
  elif .stuck then
    "🔴 STUCK    \(.user_id)  busy=\(.in_flight)  stalled=\(.in_flight_detail[0].stalled_seconds)s  bytes=\(.in_flight_detail[0].bytes_received)"
  else
    "🟡 WORKING  \(.user_id)  busy=\(.in_flight)  age=\(.in_flight_detail[0].age_seconds)s  bytes=\(.in_flight_detail[0].bytes_received)"
  end
'
```

关键字段：

| 字段 | 含义 |
|---|---|
| `worker_count` / `alive_count` / `busy_count` / `stuck_count` | 顶层汇总。**真在干活 = busy_count - stuck_count** |
| `claude_version` | 启动时 `claude --version` 的输出（升级后第一时间核对版本是否真的 pin 上了） |
| `workers[].in_flight` / `stuck` | 当前挂了几个请求 / 任一 stalled > status_stall_seconds |
| `workers[].in_flight_detail[].bytes_received` | 已收到上游字节数；0 = 上游还没回过；150-170 = 典型错误信封大小 |
| `workers[].in_flight_detail[].stalled_seconds` | 距上次收到字节多久；> 30s 标 stuck，> 90s watchdog 自动回收 |
| `workers[].in_flight_detail[].body.last_user_preview` | 当前请求 last user message 前 80 字符（便于定位"所有 worker 卡同一个 prompt"的模式） |

**`/admin/*`** —— 运维不重启容器做配置和 worker 控制：

```bash
KEY=sk-internal-...

# 改 config.yaml 后热重载（详见 §3.5）
curl -X POST -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/admin/reload

# 单独重启一个 worker（不用等 4h 定时；drain in-flight → restart → prewarm）
curl -X POST -H "Authorization: Bearer $KEY" \
  http://127.0.0.1:18787/admin/workers/litellm-0/restart

# 立即强刷一次 OAuth token（跳过 5min 周期）
curl -X POST -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/admin/refresh-now
```

返回示例：

```json
// /admin/reload
{
  "ok": true,
  "changes": [
    "claude.timeouts.status_stall_seconds: 30.0 -> 60.0",
    "users tokens: +1 -0",
    "users added (prewarmed): [\"litellm-3\"]"
  ],
  "warnings": [
    "listen_port: 8787 -> 9000 (ignored — requires container restart)"
  ]
}

// /admin/refresh-now
{"ok": true, "result": "refreshed"}    // 或 "not_needed" / "failed"
```

---

## 3. 配置说明

### 3.1 config.yaml 字段总览

| 字段 | 含义 | 热重载？ |
|---|---|---|
| `listen_host` | FastAPI 绑定地址。Docker 内固定 `0.0.0.0` | ❌ 需重启容器 |
| `listen_port` | FastAPI 容器内端口；固定 `8787` | ❌ 需重启容器 |
| `mitm.port_base` | 第一个 mitm 监听端口；user 1→18000，user 2→18001… | ❌ 需重启容器 |
| `mitm.ca_cert` | mitm CA PEM 路径；首次启动自动生成。Docker 里 `/home/coder/.mitmproxy/mitmproxy-ca-cert.pem` | ❌ 需重启容器 |
| `claude.binary` | `claude` 二进制；镜像里已在 `$PATH` | ❌ 需重启容器 |
| `claude.home_template` | 每用户隔离 `$HOME` 路径模板。Docker 里 `/data/users/{user_id}` | ❌ 需重启容器 |
| `claude.restart_interval_seconds` | 定时重启间隔。默认 14400（4h）。必须 < OAuth token 寿命 (~8h)（详见 §3.7） | ✅ `/admin/reload` |
| `claude.timeouts.*` | 7 个 timeout 微调（详见 §3.6） | ✅ 部分立即生效，部分下次 spawn 才生效 |
| `oauth_refresh.*` | 主进程集中刷 OAuth token（详见 §3.7） | ✅ `enabled` 切换会立即起/停后台任务 |
| `users` | `bearer_token → user_id` 映射；值可以是字符串（单 worker）或字符串列表（pool） | ✅ 新增自动 prewarm，删除自动 stop worker |

### 3.2 users 字段两种形式

**单 worker**（请求串行执行）：

```yaml
users:
  sk-internal-abc: alice
```

**user 池**（一个 token 自动并行到多个 worker）：

```yaml
users:
  sk-internal-def:
    - litellm-0
    - litellm-1
    - litellm-2
```

混用也可以：

```yaml
users:
  sk-internal-abc: alice                    # 单 worker
  sk-internal-def:                          # 3 worker 池
    - litellm-0
    - litellm-1
    - litellm-2
  sk-internal-ghi: [shared-0, shared-1]     # 2 worker 池（行内 list）
```

调度逻辑：

1. 客户端用某个 token 发请求 → 鉴权拿到对应的 user 列表
2. 优先挑列表里**没有 in-flight 请求**的 worker
3. 多个候选时 round-robin（避免总集中到 list[0]）
4. 全忙时挑 in-flight 最少的；同 min 时 RR

每个 user_id 都会占一个 mitm 端口（`port_base + N`），所以 3 worker 池
占 3 个端口；预热时间约 N × 10s。

### 3.3 docker-compose.yml 关键项

| 项 | 当前值 | 说明 |
|---|---|---|
| `user` | `"1000:1000"` | 容器直接以 uid 1000 启动；宿主机 bind 目录需归 1000（§2.4） |
| `ports` | `"0.0.0.0:18787:8787"` | 宿主 18787 → 容器 8787 |
| `volumes` | `./config.yaml:/data/config.yaml:ro` | 配置，只读 |
| | `/data/shared-auth/claude:/home/coder/.claude` | 凭据目录，**读写**（token 刷新写回） |
| | `mitm-ca:/home/coder/.mitmproxy` | mitm CA 具名卷 |
| | `./users:/data/users` | per-user 隔离 HOME |

### 3.4 配额与并发

- **同一个 user 的请求串行执行**（claude TUI worker 一次只跑一个）
- **同一个 token 可以挂一个 user 池**（§3.2），服务端自动负载均衡
- **所有 user 共享一个订阅配额**（同一个 OAuth 账号）；池只解决本服务的
  worker 并行度，**不放大 Anthropic 端的限速**
- Anthropic 订阅有 per-OAuth 隐含并发限制（约 2-3 并发）；pool 配过大
  （如 10 worker / 1 账号）会触发大面积 429 / 上游沉默，请求会 stuck
  90s 才被 watchdog 解开 —— 实际就是空响应。**单账号 pool 建议 ≤ 3**
- 多账号池 / 配额追踪不在当前实现范围内

### 3.5 热重载 `/admin/reload`

改完 `config.yaml` 后**不用重启容器**：

```bash
curl -X POST -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/admin/reload
```

服务会重读 `config.yaml`，diff 后应用所有**可热重载**的字段，对**需要
重启**的字段输出 warning 不去碰。可热重载字段见 §3.1 表格的"热重载？"
列。

`users` 字段的热增删特殊处理：

- **新增 user_id** → 自动 spawn + prewarm
- **删除 user_id** → 立即 stop 该 worker（in-flight 请求被 None 哨兵关掉），从 manager.sessions 弹出
- **改 pool 成员** → 按差集处理（新增的 prewarm，移除的 stop）

返回示例：

```json
{
  "ok": true,
  "changes": [
    "claude.timeouts.status_stall_seconds: 30.0 -> 60.0",
    "claude.restart_interval_seconds: 14400 -> 10800",
    "users tokens: +1 -0",
    "users added (prewarmed): [\"litellm-3\"]"
  ],
  "warnings": [
    "listen_port: 8787 -> 9000 (ignored — requires container restart)"
  ]
}
```

某些 timeout（`mitm_intercept_seconds` / `response_stall_seconds`）是
worker subprocess 启动时通过 CLI argv 注入的，热重载后**只对新 spawn
的 worker 生效**。要让立刻生效，对每个 worker 跑一次
`POST /admin/workers/{id}/restart`。

### 3.6 `claude.timeouts.*` 微调项

7 个 timeout 都有合理默认；只在确实看到对应失败时再调。

| 字段 | 默认 | 说明 |
|---|---|---|
| `mitm_intercept_seconds` | 30 | PTY 触发后 mitm 多久没拦到 `/v1/messages` 就放弃。覆盖故障 A：TUI 卡 modal/错误屏吞了 keystroke。worker 启动时通过 argv 注入，**reload 只对新 spawn 生效** |
| `status_stall_seconds` | 30 | `/status` 把 in-flight 标为 `stuck` 的阈值（没收到字节多久）。仅显示用，不触发动作 |
| `response_stall_seconds` | 90 | mitm watchdog 触发阈值。hijack 完后多久没收到 chunk 就强制关闭 channel。覆盖故障 B（上游沉默不回 headers）+ 故障 C（上游回了错误信封后挂起）。**reload 只对新 spawn 生效** |
| `restart_drain_seconds` | 60 | 定时重启 / admin restart 时等 in-flight 排空的最长时间 |
| `worker_ready_seconds` | 60 | 新 worker 多久没在 stdout 喊 `{"type":"ready"}` 就 kill 重来 |
| `prewarm_seconds` | 60 | bootstrap prewarm 上限。超时不致命（worker 仍可用，首发请求可能要重试一次） |
| `restart_check_interval_seconds` | 60 | `_restarter` 巡检间隔 |

### 3.7 `oauth_refresh.*` 集中刷新

| 字段 | 默认 | 说明 |
|---|---|---|
| `enabled` | true | 关掉的话 worker 回退到自刷新，多 worker 撞 RT 轮换的 race 会重现（详见 §1.10）。**仅用于排障** |
| `check_interval_seconds` | 300 | 主进程后台任务每多久检查一次 `.credentials.json` 的 `expiresAt` |
| `refresh_when_expires_within_seconds` | 3600 | token 距离过期 < 这个窗口就主动刷 |

设计要点（详见 §1.10）：

- 主进程是 `/v1/oauth/token` 的**唯一写入者** → 物理上不可能撞 RT 轮换
- worker 内存里 token 在 `claude.restart_interval_seconds`（默认 4h）
  内一定被替换，**worker 永远不需要自刷新**
- `enabled=false` 时启动 log 会喊 warning；通过 `/admin/reload` 改成
  true 会立即起后台任务并跑一次 initial check

强制立即刷一次（不等 5 分钟周期）：

```bash
curl -X POST -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/admin/refresh-now
# {"ok":true,"result":"refreshed"}  // 或 "not_needed" / "failed"
```

### 3.8 dev 模式（不走 Docker）

本机直跑要把 `config.yaml` 里两条路径改回相对：

```yaml
mitm:
  ca_cert: ~/.mitmproxy/mitmproxy-ca-cert.pem
claude:
  home_template: ./users/{user_id}
```

启动：

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

# 预生成 mitm CA
timeout 5 mitmdump --listen-port 19998 -q

# HOME 指向有 .claude/.credentials.json 的家目录
HOME=/home/coder CONFIG=config.yaml LOG_LEVEL=INFO python3 -m src.main
```

---

## 4. API 使用

两种鉴权方式任选：`Authorization: Bearer <token>` 或 `x-api-key: <token>`。
下面例子都用对外端口 `18787`。

### 4.1 Anthropic 原生 — `POST /v1/messages`

```bash
KEY=sk-internal-...

# 非流
curl -sS -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-opus-4-7",
    "max_tokens": 512,
    "messages": [{"role":"user","content":"TCP 三次握手用 3 行解释"}]
  }'

# 流式
curl -sS -N -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{"model":"claude-opus-4-7","max_tokens":512,"stream":true,
       "messages":[{"role":"user","content":"hi"}]}'
```

**带自定义 system**（替换 claude code 原人设，只保留计费头）：

```bash
curl -sS -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{
    "model": "claude-opus-4-7",
    "max_tokens": 256,
    "system": "You are a terse code reviewer. Reply in at most three bullets.",
    "messages": [{"role":"user","content":"评审：`eval(user_input)` 有问题吗？"}]
  }'
```

你的 system 带自动 `cache_control: ephemeral`，重复用便宜很多
（响应里 `usage.cache_read_input_tokens` 增长 = 命中缓存）。

**完全禁工具**（避免 claude code 内置工具污染输出）：

```bash
curl -sS -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{
    "model": "claude-opus-4-7",
    "max_tokens": 256,
    "tool_choice": {"type": "none"},
    "messages": [{"role":"user","content":"翻译成英文：今天天气真好"}]
  }'
```

或者传你自己的 `tools: [...]` 完全覆盖内置 11 个。

### 4.2 OpenAI 兼容 — `POST /v1/chat/completions`

```bash
curl -sS -X POST http://127.0.0.1:18787/v1/chat/completions \
  -H "Authorization: Bearer $KEY" \
  -d '{
    "model": "claude-opus-4-7",
    "stream": true,
    "messages": [
      {"role":"system","content":"Be concise."},
      {"role":"user","content":"What is HTTP/2 multiplexing?"}
    ]
  }'
```

字段转换在 `src/api/translate.py`：OpenAI 的 `messages[].role=system` 会
被提取成 Anthropic 的 `system` 字段，`tools` / `tool_calls` 双向映射，
响应里 Anthropic 的 `content[].type=tool_use` 转成 OpenAI 的
`message.tool_calls`。

### 4.3 Python SDK

**Anthropic SDK**：

```python
from anthropic import Anthropic
client = Anthropic(base_url="http://127.0.0.1:18787", api_key=KEY)
resp = client.messages.create(
    model="claude-opus-4-7", max_tokens=256,
    messages=[{"role": "user", "content": "Hi"}],
)
print(resp.content[0].text)
```

**OpenAI SDK**：

```python
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:18787/v1", api_key=KEY)
resp = client.chat.completions.create(
    model="claude-opus-4-7",
    messages=[{"role": "user", "content": "Hi"}],
)
print(resp.choices[0].message.content)
```

### 4.4 LiteLLM 接入

`PROXY_HOST` 换成部署机 IP，端口用对外的 `18787`。配合 §3.2 的 user
池写法，**LiteLLM 侧只配一个 key**，本服务自动并行到池里多个 worker。

**LiteLLM Proxy（YAML）**：

```yaml
model_list:
  # Anthropic 原生路径（推荐：少一层转换）
  - model_name: claude-opus-sub              # 下游用这个名字
    litellm_params:
      model: anthropic/claude-opus-4-7       # 真实模型 ID（必须！）
      api_base: http://PROXY_HOST:18787      # 不带 /v1，不要尾斜杠
      api_key: sk-internal-...               # 单 key 即可，本服务侧 pool 做并发

  # OpenAI 兼容路径（如果下游只发 /v1/chat/completions）
  - model_name: claude-opus-openai
    litellm_params:
      model: openai/claude-opus-4-7
      api_base: http://PROXY_HOST:18787/v1   # 注意带 /v1
      api_key: sk-internal-...
```

启动 + 验证：

```bash
litellm --config proxy_config.yaml --port 4000

curl -sS http://127.0.0.1:4000/v1/chat/completions \
  -H 'Authorization: Bearer anything' \
  -d '{"model":"claude-opus-sub","messages":[{"role":"user","content":"hi"}]}'
```

**LiteLLM Admin Web UI — Add Model**：

| 字段 | 填什么 |
|---|---|
| **Provider** | `Anthropic` |
| **LiteLLM Model** / **Model ID**（发给 provider 的字段） | `claude-opus-4-7` ← 必须是真实模型 ID |
| **Public Model Name** / **Model Name**（对外别名） | `claude-opus-sub`（随便起） |
| **API Key** | `sk-internal-...` |
| **API Base**（在 Advanced 里展开） | `http://PROXY_HOST:18787` 不带 /v1 |

> **最常见的错配**：把"Public Model Name"和"LiteLLM Model"两个字段填成
> 一样，结果 LiteLLM 把 `claude-opus-sub` 当真实模型 ID 发给 Anthropic，
> Anthropic 返回 error 事件 → LiteLLM 解析时 `KeyError: 'stop_reason'`。

**透传 LiteLLM 用户身份到 /status 和 /ui**（可选）：

LiteLLM Proxy 默认用自己配置里的 `api_key` 鉴权到我们，所以我们只看到
LiteLLM 的"出口身份"，不知道终端用户是谁。打开下面这个开关后 LiteLLM
会自动给上游请求加 `x-litellm-user-id` / `x-litellm-org-id` / `x-litellm-team-id`
等 header，我们这边自动捕获并展示到 `/status` 的 `in_flight_detail[].body.litellm`
和 `/ui` 监控页的"当前任务"一栏：

```yaml
# 加在 LiteLLM proxy_config.yaml 顶层
litellm_settings:
  add_user_information_to_llm_headers: true
```

不开也完全能用，只是 /status 看不到上游用户归属。详见
[LiteLLM 文档](https://docs.litellm.ai/docs/proxy/forward_client_headers#user-information-headers-optional)。

**LiteLLM Python SDK 直调**：

```python
from litellm import completion

# Anthropic 原生
resp = completion(
    model="anthropic/claude-opus-4-7",
    api_base="http://PROXY_HOST:18787",       # 不带 /v1
    api_key="sk-internal-...",
    messages=[{"role": "user", "content": "hi"}],
)

# OpenAI 兼容
resp = completion(
    model="openai/claude-opus-4-7",
    api_base="http://PROXY_HOST:18787/v1",    # 带 /v1
    api_key="sk-internal-...",
    messages=[{"role": "user", "content": "hi"}],
)
```

### 4.5 支持的模型 ID

任何 Anthropic 接受的模型 ID 都能用 —— proxy 不维护白名单。常用：

- `claude-opus-4-7`
- `claude-sonnet-4-6`
- `claude-haiku-4-5-20251001`

### 4.6 响应里的关键字段

- `stop_reason: "end_turn"` —— 正常结束
- `stop_reason: "max_tokens"` —— 达到长度上限
- `stop_reason: "tool_use"` —— 模型想调工具（如果你启用了 tools）
- `stop_reason: "error"` —— 上游异常，`content[0].text` 含 `[upstream xxx]` 错误描述
- `usage.input_tokens` / `usage.output_tokens` —— 本次 token 用量
- `usage.cache_read_input_tokens` —— 命中 prompt cache 的 input token 数
  （> 0 说明走的是订阅配额，CLI 系统 prompt 被缓存了）

非流式响应有空内容的情况兜底：`_collapse_stream` 会保证返回的 message
一定有 `stop_reason` 字段，并把上游 error 内容塞进 `content[0].text`，
下游 LiteLLM / SDK 不会因解析失败而崩。
