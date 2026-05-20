# claude-subscription-proxy

把 **Claude Code 订阅账号（Max / Pro）** 包装成一个本地的
**Anthropic / OpenAI 兼容 HTTP API**，让你的脚本、IDE 插件、LiteLLM
等可以通过标准 API 调用 Claude，而 Anthropic 把这些请求当作 **Claude
Code CLI 的交互式调用**，按订阅配额计费 —— 不走 API/SDK 配额。

```
你的应用 / LiteLLM / SDK                    本服务                          Anthropic
──────────────────────                     ─────                          ─────────
  curl  ─────┐                            ┌──────────────┐
  Anthropic SDK ──────► HTTP/JSON ───────►│  FastAPI     │
  OpenAI SDK ───┘                         │  :8787       │
  LiteLLM ─────►                          └──────┬───────┘
                                                 │ JSON-lines IPC
                                                 ▼
                                          ┌──────────────────────┐
                                          │ per-user worker      │
                                          │  · mitmproxy 18000+N │  劫持+改写
                                          │  · PTY → claude code │──► HTTPS ──► api.anthropic.com
                                          │  (独立 asyncio loop)  │   (cc_entrypoint=cli)
                                          └──────────────────────┘
```

> **⚠️ 法律说明**：在多个用户之间共享同一个 Claude 订阅席位违反
> Anthropic 的服务条款，账号有被封停的风险。本项目仅供
> **个人单席位多路复用**（让自己的多个脚本共用同一个订阅）以及
> **学术 / 安全研究**用途。详见 [§8](#8-局限性--法律说明)。

---

## 目录

1. [实现原理](#1-实现原理)
2. [Docker 部署（推荐）](#2-docker-部署推荐)
3. [配置说明](#3-配置说明)
4. [API 使用](#4-api-使用)
5. [接入 LiteLLM](#5-接入-litellm)
6. [测试与验证](#6-测试与验证)
7. [故障排查](#7-故障排查)
8. [局限性 与 法律说明](#8-局限性--法律说明)

---

## 1. 实现原理

### 1.1 核心思路

Anthropic 用一组**请求级身份指纹**判定这次调用是订阅配额还是 API 配额。
最关键的三类：

| 指纹 | 内容 |
|---|---|
| `system[0]` 文本块 | `x-anthropic-billing-header: cc_entrypoint=cli; cc_version=2.1.x; ...` |
| `User-Agent` | `claude-cli/2.1.x (external, cli)` |
| body 其它字段 | 完整 `tools` 列表（11 个内建工具）、`metadata`、`anthropic_version`、`anthropic-beta` 头等 |

任意一项缺失或不匹配就会被路由到 API 配额。手工拼请求难以保证全套指纹
长期稳定，所以本项目的做法是：

> **让真实的 `claude` CLI 在受控环境里发请求，用 mitm 在它出门前把 body
> 里业务字段换成 API 调用方发来的内容**。

### 1.2 整体数据流

```
        ┌─────────────────────── 主进程：FastAPI ──────────────────────┐
        │  /v1/messages    /v1/chat/completions    SessionManager     │
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

1. HTTP 请求落到 FastAPI，鉴权后查/建该 user 的 `worker` 子进程
2. 主进程把请求 body 以 JSON 行写给 worker 的 stdin
3. worker 往 claude PTY 写 `say hi\r`，触发 claude 发出真实的
   `POST /v1/messages?beta=true`
4. mitm 拦截这个出站请求，**白名单合并** body（见 §1.4）后转发
5. Anthropic 用 SSE 流回应，mitm 的 `_tap` 回调把每个 chunk **同时**
   forward 给 claude（保持 TUI 状态一致）和塞进 `ResponseChannel`
6. worker 把 channel 里的字节编码后写 stdout，主进程读出来转发给
   FastAPI 的 `StreamingResponse`

### 1.3 mitm 是怎么劫持 claude 的

#### 走通信道

claude TUI 是 Node 应用，启动时 `pty_driver.py` 给它注入：

```python
env["HTTPS_PROXY"] = "http://127.0.0.1:18000+N"     # 强制走代理
env["NODE_EXTRA_CA_CERTS"] = "<mitm-ca>.pem"        # 信任 mitm 自签 CA
```

Node 标准的 `https`/`undici` 模块识别 `HTTPS_PROXY`，所有出站 HTTPS
请求都会 CONNECT 到 mitm 端口；`NODE_EXTRA_CA_CERTS` 让 TLS 握手不
报错。mitm 端 `mode=["regular"]` 跑普通正向代理。

#### 认领目标 flow

worker 进程里每用户**同时最多一个 PendingRequest**（双重 lock 串行
化）。mitm 看到任何出站 `POST /v1/messages` 时（`src/mitm/addon.py:61`）：

```python
if self.session.pending is None:
    return        # 不是我们触发的（可能是 claude bootstrap），原样放行
# 否则：这就是 trigger() 引发的请求，拿来劫持
```

劫持成功后记下 `_active_flow_id = flow.id`，在 `responseheaders`
阶段确认是同一个 flow 才挂 tap，避免误伤 claude 的其它后台请求
（telemetry / mcp-registry / npm 等）。

#### 改写 body（白名单合并）

`_merge_body` 的策略不是"替换"而是"合并"。从 claude 原始 body 开始，
**只覆盖几个用户业务字段**，其余原样保留：

| 字段 | 谁说了算 | 原因 |
|---|---|---|
| `messages` / `model` / `max_tokens` / `temperature` / `top_p` / `top_k` / `stop_sequences` / `tool_choice` | 用户 | 业务输入 |
| `stream` | **强制 true** | 我们需要 SSE 才能 tap；非流响应在 FastAPI 层重组 |
| `system` | **混合** | 保留 CLI 的 `system[0]`（计费头），把用户的 system 作为新 cached block 追加 |
| `tools` / `metadata` / `thinking` / `anthropic_version` / `output_config` / ... | CLI 原样 | identity 指纹 |
| 所有 `User-Agent` / `anthropic-*` / `x-stainless-*` 等请求头 | CLI 原样 | identity 指纹 |

`system` 的合并细节（`_merge_system`，`src/mitm/addon.py:147`）：

```
原 CLI:    [billing-header, "You are Claude Code...", persona, instructions]
用户传:    "Be terse."
合并结果:  [billing-header, {"type":"text","text":"Be terse.","cache_control":{"type":"ephemeral"}}]
```

billing-header 保留 → 计费走订阅；CLI 自己的人设块丢掉 → 不和用户指令
冲突；用户 system 带 `cache_control` → Anthropic 把它缓存起来，后续同
system 的调用响应里能看到 `cache_read_input_tokens` 增加。

#### 双向 tap 响应

mitm 的 `flow.response.stream` 可以设成一个 callable，每个 chunk
经过时调用一次。我们的 `_tap`（`src/mitm/addon.py:203`）做两件事：

```python
def _tap(data: bytes) -> bytes:
    if data:
        channel.queue.put_nowait(bytes(data))   # → 给用户的 SSE 流
        return data                             # → 也回给 claude TUI
    channel.queue.put_nowait(None)              # b"" = 流结束
    return b""
```

字节同时去两个地方：一份给 claude（让它 TUI 状态机消化，否则下次发请求
会异常），一份给 ResponseChannel（最终发回 API 调用方）。

### 1.4 几个关键设计决策

**为什么 worker 要独立子进程？**
mitmproxy 11 的 `DumpMaster` + ptyprocess 跟 FastAPI 主循环放一起会
出现 `flow.response.stream` 回调调度异常 + PTY 写字节静默丢失。每个
用户一个独立 `asyncio.run()` 子进程绕开，副作用是顺带获得崩溃隔离。

**为什么用 PTY 而不是直接拼请求？**
让真 CLI 跑一遍能保证三类指纹都是它当前版本会发的真值，CLI 升级后
自动跟上，proxy 代码不用动。

**为什么强制 stream=True？**
Anthropic 的非流响应是一次性 JSON，无法在 chunk 级 tap。强制流式 +
在 FastAPI 层用 `_collapse_stream` 重组成非流 JSON，对用户透明。

**为什么 mitm 不直接生成响应、跳过 claude？**
claude TUI 内部有状态机（会话历史、tool 调用栈），如果它发了请求却
没收到响应，下一次 keystroke 会触发它的错误处理路径，可能阻塞或
重连。让它"看见"响应字节是最简单的同步方式。

### 1.5 关键代码索引

| 想看什么 | 文件:行 |
|---|---|
| body 合并规则 | `src/mitm/addon.py:108` `_merge_body` |
| system 字段合并 | `src/mitm/addon.py:147` `_merge_system` |
| 响应流 tap | `src/mitm/addon.py:188` `responseheaders` / `_tap` |
| 用户可覆盖的 body 字段白名单 | `src/mitm/addon.py:24` `USER_OWNED_BODY_FIELDS` |
| worker 主循环 + IPC 协议 | `src/worker.py` |
| claude TUI 启动 + 等 `❯` | `src/pty_driver.py:66` `_wait_until_ready` |
| 触发占位符让 claude 发请求 | `src/pty_driver.py:106` `trigger` |
| seed 用户 HOME（拷 `.claude.json` / **软链** `.credentials.json`） | `src/session/session.py:87` `_seed_home` |
| SSE → 完整 Message 重组（含 error 兜底） | `src/api/anthropic.py:41` `_collapse_stream` |
| OpenAI ↔ Anthropic 字段转换 | `src/api/translate.py` |

---

## 2. Docker 部署（推荐）

部署后的关键事实，先记住：

| 项 | 值 |
|---|---|
| 对外端口 | **`18787`**（宿主机）→ 容器内 `8787` |
| 容器运行用户 | `coder`，**uid/gid 1000**（compose 里 `user: "1000:1000"`） |
| 凭据来源 | 宿主机目录 `/data/shared-auth/claude` → bind 到容器 `/home/coder/.claude`（读写） |
| per-user 状态 | 项目目录下 `./users/` → bind 到容器 `/data/users` |
| mitm CA | 具名卷 `mitm-ca` → 容器 `/home/coder/.mitmproxy`，重启保留 |
| 配置文件 | 项目目录下 `./config.yaml` → bind 到容器 `/data/config.yaml`（只读） |

### 2.1 前置条件

**部署机器需要**：

- Docker 24+ 和 Docker Compose v2
- 宿主机端口 `18787` 可用（可改，见 §2.9）
- 容器内端口段 `18000..18000+N`（N = 用户数）保留给 mitm，不对外暴露

**另需一台有 Claude Code 登录的机器**（OAuth 一次即可，凭据可以拷过去）：

- 装好 `claude` 并 `claude /login` 完成 OAuth
- 验证：本地 `claude` 进入 TUI 不再问 onboarding（无主题选择器）

### 2.2 获取项目

```bash
git clone <this-repo> claude-subscription-proxy
cd claude-subscription-proxy
```

### 2.3 步骤一：准备 operator 凭据目录

整个 `~/.claude/` 目录会被 bind 进容器，所以**拷整个目录**，不是单文件。

在一台已 `claude /login` 的机器上，把它的 `~/.claude/` 镜像到部署机的
`/data/shared-auth/claude`：

```bash
# 在部署机上
sudo mkdir -p /data/shared-auth

# 从已登录机器把整个 .claude 目录拷过来（scp / rsync / 本机 cp 均可）
sudo cp -r <已登录用户的 HOME>/.claude  /data/shared-auth/claude

# 确认凭据文件在
ls -l /data/shared-auth/claude/.credentials.json
```

> **凭据怎么被用**：容器以 uid 1000(`coder`)运行，`/data/shared-auth/claude`
> 整个目录 bind 到容器的 `/home/coder/.claude`（**读写**挂载）。每个 API
> 用户的 worker 启动时，`_seed_home`（`src/session/session.py:87`）把这个
> 目录里的 `.credentials.json` **软链接**进自己的隔离 HOME —— 所有用户
> **共用同一份 OAuth 凭据，不再产生副本**。claude CLI 刷新 OAuth token
> 时会顺着软链接写回这一份（所以挂载必须读写），刷新结果对所有用户、
> 以及共享这个目录的其它容器都立即生效。

OAuth token 会过期：claude CLI 在 token 没过期时能自己用 refresh token
续期。但如果你在源头**重新登录**（refresh token 被轮换作废），就需要把
新的 `.claude/` 重新同步到 `/data/shared-auth/claude` —— 见 §7.10。

### 2.4 步骤二：写 config.yaml

```bash
cp docker/config.example.yaml config.yaml
```

编辑 `config.yaml`，生成真实 API key，列出你的用户：

```bash
# 生成一个 key
echo "sk-internal-$(openssl rand -hex 24)"
```

最终 `config.yaml` 应该长成这样（**Docker 部署必须用绝对路径**）：

```yaml
listen_host: 0.0.0.0
listen_port: 8787                                    # 容器内端口，别改

mitm:
  port_base: 18000
  ca_cert: /home/coder/.mitmproxy/mitmproxy-ca-cert.pem

claude:
  binary: claude
  home_template: /data/users/{user_id}
  restart_interval_seconds: 43200   # 12h: worker recycled in place to clear CLI state

# bearer token -> user_id
users:
  sk-internal-29f894...331e: litellm
```

> ⚠️ `ca_cert` 和 `home_template` **必须是绝对路径**。写成 `./` 或 `~/`
> 开头，entrypoint 会拒绝启动并报错（见 §7.5）。容器里 worker 子进程的
> CWD 是 `/app`（root 所有、不可写），相对路径会被解析到那里然后崩。

### 2.5 步骤三：宿主机目录权限

compose 里 `user: "1000:1000"` 让容器**直接以 uid 1000 启动** ——
entrypoint 不再用 root 自动修卷权限，所以**容器要写的 bind 目录必须在
宿主机侧就归 uid 1000**：

```bash
cd claude-subscription-proxy

# per-user 状态目录：worker 要在里面建 /data/users/<user>
mkdir -p ./users
sudo chown -R 1000:1000 ./users

# 凭据目录：claude CLI 刷新 token 要写回
sudo chown -R 1000:1000 /data/shared-auth/claude
```

| 宿主机目录 | 容器内路径 | 权限要求 |
|---|---|---|
| `./users` | `/data/users` | uid 1000 **可写** |
| `/data/shared-auth/claude` | `/home/coder/.claude` | uid 1000 **可读写** |
| `mitm-ca`（具名卷） | `/home/coder/.mitmproxy` | 自动 —— 首次从镜像内 `coder` 所有的目录填充 |
| `./config.yaml` | `/data/config.yaml` | uid 1000 可读即可 |

> Docker bind mount **不做 uid 翻译**：宿主机文件的数字 uid 原样出现在
> 容器里。容器内的 `coder` 是 uid 1000，所以宿主机目录必须也归 1000。
> 如果不想管这些权限，可以去掉 `user: "1000:1000"`，让容器以 root 启动，
> entrypoint 会自动 `chown` 好再 `gosu` 降权到 `coder`（见 §7.7）。

### 2.6 步骤四：构建并启动

```bash
docker compose up --build -d
docker compose logs -f proxy
```

首次构建 4–6 分钟（下载 Node 20 + claude code npm + python 包）。
之后 `docker compose up -d` 秒级启动。

健康检查：

```bash
curl -s http://127.0.0.1:18787/healthz
# → {"ok":true,"sessions":[]}
```

启动日志里这几行代表链路就绪：

```
[entrypoint] launching: python3 -m src.main
INFO  Uvicorn running on http://0.0.0.0:8787
```

第一个请求进来后还会看到：

```
linked /data/users/litellm/.claude/.credentials.json -> /home/coder/.claude/.credentials.json
session up user=litellm worker_pid=... mitm_port=18000
```

### 2.7 步骤五：发第一个请求

```bash
KEY=sk-internal-29f894...331e
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
- 响应 JSON 含 `"stop_reason": "end_turn"` 和 `cache_read_input_tokens`
  字段（命中 CLI 系统 prompt 缓存的证据）

### 2.8 停止 / 重启 / 加用户

```bash
docker compose down              # 停止；./users/ 里的 per-user 状态保留
docker compose restart proxy     # 改了 config.yaml 后热重启
docker compose down -v           # 同时清除 mitm CA 卷（下次重新生成）
```

**加用户**：在 `config.yaml` 的 `users:` 下加一行 `sk-internal-...: charlie`，
然后 `docker compose restart proxy`。charlie 首次请求时 `./users/charlie/`
自动创建并 seed 凭据。不需要每个用户单独登录 —— 所有 user 共用同一个
operator OAuth 账号（同一个订阅配额）。

### 2.9 暴露给其他主机

`docker-compose.yml` 默认把 `18787` 绑到 `0.0.0.0`，**局域网内其他主机
可以直接访问**（用来接 LiteLLM 等）。强烈建议：

- 仅在受信任的内网开放
- 或前置一层反向代理（nginx / caddy）做 TLS + IP 白名单
- **绝不要暴露到公网** —— 项目无速率限制，且违反 ToS

仅本机访问（更安全）：把 `ports` 改成 `"127.0.0.1:18787:8787"`。
换端口：改 `ports` 左边的 `18787`，容器内的 `8787` 保持不动。

---

## 3. 配置说明

### 3.1 config.yaml 字段

| 字段 | 含义 |
|---|---|
| `listen_host` | FastAPI 绑定地址。Docker 内固定 `0.0.0.0`，对外暴露范围由 compose 的 `ports` 决定。 |
| `listen_port` | FastAPI 容器内端口；固定 `8787`，对外端口在 compose 里映射。 |
| `mitm.port_base` | 第一个 mitm 监听端口；user 1→18000，user 2→18001… 需要 N 个连续端口空闲（仅容器内部用）。 |
| `mitm.ca_cert` | mitm CA PEM 路径；首次启动自动生成。Docker 里固定 `/home/coder/.mitmproxy/mitmproxy-ca-cert.pem`。 |
| `claude.binary` | `claude` 二进制；镜像里已在 `$PATH`。 |
| `claude.home_template` | 每用户隔离 `$HOME` 路径模板。Docker 里 `/data/users/{user_id}`。 |
| `claude.restart_interval_seconds` | 定时重启间隔。worker 不再因 idle 被回收；到达 age 后**就地重启**（同 session、同端口），用来清掉 CLI 累积状态。优雅等待最多 60s 让在飞请求结束。默认 43200（12h）。 |
| `users` | `bearer_token → user_id` 映射。 |

### 3.2 docker-compose.yml 关键项

| 项 | 当前值 | 说明 |
|---|---|---|
| `user` | `"1000:1000"` | 容器直接以 uid 1000 启动；宿主机 bind 目录需归 1000（§2.5） |
| `ports` | `"0.0.0.0:18787:8787"` | 宿主 18787 → 容器 8787 |
| `volumes` | `./config.yaml:/data/config.yaml:ro` | 配置，只读 |
| | `/data/shared-auth/claude:/home/coder/.claude` | 凭据目录，**读写**（token 刷新写回） |
| | `mitm-ca:/home/coder/.mitmproxy` | mitm CA 具名卷 |
| | `./users:/data/users` | per-user 隔离 HOME / transcript |

### 3.3 配额与并发

- **同一个 user 的请求串行执行**（`ClaudeSession.lock`）；要并发就配多个 user
- **所有 user 共享一个订阅配额**（同一个 OAuth 账号）
- 多账号池 / 配额追踪不在当前实现范围内

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

带自定义 system：

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

> proxy 会保留 CLI 的 `system[0]`（计费头），把你的 `system` 作为新的
> cached text block **附加**进去。你的 prompt 生效，调用照样计入订阅配额。

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

### 4.3 Python SDK

```python
# anthropic SDK
from anthropic import Anthropic
client = Anthropic(base_url="http://127.0.0.1:18787", api_key=KEY)
resp = client.messages.create(
    model="claude-opus-4-7", max_tokens=256,
    messages=[{"role": "user", "content": "Hi"}],
)
print(resp.content[0].text)

# openai SDK
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:18787/v1", api_key=KEY)
resp = client.chat.completions.create(
    model="claude-opus-4-7",
    messages=[{"role": "user", "content": "Hi"}],
)
print(resp.choices[0].message.content)
```

### 4.4 支持的模型 ID

任何 Anthropic 接受的模型 ID 都能用 —— proxy 不维护白名单。常用：

- `claude-opus-4-7`
- `claude-sonnet-4-6`
- `claude-haiku-4-5-20251001`

---

## 5. 接入 LiteLLM

`PROXY_HOST` 换成部署机 IP，端口用对外的 `18787`。

### 5.1 LiteLLM Proxy（推荐：YAML 配置）

`proxy_config.yaml`：

```yaml
model_list:
  # Anthropic 原生路径（推荐：少一层转换）
  - model_name: claude-opus-sub                  # 你的下游用这个名字
    litellm_params:
      model: anthropic/claude-opus-4-7           # 真实模型 ID（必须！）
      api_base: http://PROXY_HOST:18787          # 不带 /v1，不要尾斜杠
      api_key: sk-internal-...

  - model_name: claude-sonnet-sub
    litellm_params:
      model: anthropic/claude-sonnet-4-6
      api_base: http://PROXY_HOST:18787
      api_key: sk-internal-...

  # OpenAI 兼容路径（如果下游只发 /v1/chat/completions）
  - model_name: claude-opus-openai
    litellm_params:
      model: openai/claude-opus-4-7
      api_base: http://PROXY_HOST:18787/v1       # 注意带 /v1
      api_key: sk-internal-...
```

启动：

```bash
litellm --config proxy_config.yaml --port 4000
```

验证：

```bash
curl -sS http://127.0.0.1:4000/v1/chat/completions \
  -H 'Authorization: Bearer anything' \
  -d '{"model":"claude-opus-sub","messages":[{"role":"user","content":"hi"}]}'
```

### 5.2 LiteLLM Admin Web UI — Add Model 表单

| 字段 | 填什么 |
|---|---|
| **Provider** | `Anthropic` |
| **LiteLLM Model** / **Model ID**（发给 provider 的字段） | `claude-opus-4-7` **← 必须是真实模型 ID** |
| **Public Model Name** / **Model Name**（对外别名） | `claude-opus-sub`（随便起） |
| **API Key** | `sk-internal-...` |
| **API Base**（在 Advanced 里展开） | `http://PROXY_HOST:18787` 不带 /v1 |
| 其他字段 | 留空 |

提交后列表里应该显示：
```
claude-opus-sub  →  anthropic/claude-opus-4-7  at http://PROXY_HOST:18787
```

> ⚠️ **最常见的错配**：把"Public Model Name"和"LiteLLM Model"两个
> 字段填成一样（比如都填 `claude-opus-sub`），结果 LiteLLM 把
> `claude-opus-sub` 当作真实模型 ID 发给 Anthropic，Anthropic 返回
> error 事件 → LiteLLM 解析时 `KeyError: 'stop_reason'`。

### 5.3 LiteLLM Python SDK 直调

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
    api_base="http://PROXY_HOST:18787/v1",   # 带 /v1
    api_key="sk-internal-...",
    messages=[{"role": "user", "content": "hi"}],
)
```

---

## 6. 测试与验证

### 6.1 健康检查

```bash
curl -s http://127.0.0.1:18787/healthz
# {"ok":true,"sessions":["litellm"]}    ← sessions 显示活跃 worker
```

### 6.2 真实调用（最关键）

```bash
KEY=sk-internal-...
curl -sS -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{"model":"claude-opus-4-7","max_tokens":64,
       "messages":[{"role":"user","content":"ping"}]}' \
  | python3 -m json.tool
```

**应包含的字段**（判断"是否成功"）：

- ✅ `stop_reason: "end_turn"`
- ✅ `cache_read_input_tokens > 0`（命中 CLI 系统 prompt 缓存，说明走的是订阅配额）
- ✅ `content[].text` 有实际内容
- ❌ 如果 `stop_reason: "error"` 且 `content[0].text` 是 `[upstream ...]`
  开头，那是上游 Anthropic 的真实错误（见 §7.10）

### 6.3 流式调用

```bash
curl -sS -N -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{"model":"claude-opus-4-7","max_tokens":128,"stream":true,
       "messages":[{"role":"user","content":"count to 5"}]}'
```

会逐字 yield `event: content_block_delta` 事件。

### 6.4 容器日志解读

```bash
docker compose logs -f proxy
```

每个成功的请求会留下这几行：

```
INFO src.session.session   linked /data/users/litellm/.claude/.credentials.json -> /home/coder/.claude/.credentials.json
INFO src.session.session   session up user=litellm worker_pid=N mitm_port=18000
INFO worker:src.pty_driver pty trigger wrote 7 bytes via ptyprocess
INFO worker:src.mitm.addon user=litellm saw outbound POST /v1/messages?beta=true
INFO worker:src.mitm.addon user=litellm hijacked outbound /v1/messages flow=xxx model=claude-opus-4-7
INFO worker:mitmproxy.proxy Streaming response from api.anthropic.com.
INFO worker:src.mitm.addon user=litellm response stream complete flow=xxx
INFO     172.x.x.x:xxxx     - "POST /v1/messages HTTP/1.1" 200 OK
```

**噪音提示**：worker 起来后会周期性出现连到 `registry.npmjs.org`、
`api.anthropic.com/api/event_logging/v2/batch`、`datadoghq.com` 的日志
—— 那是 claude CLI 的版本检查 / 遥测心跳，**不是你的请求**。真正的模型
调用是 `hijacked outbound /v1/messages` 那行。

### 6.5 单元测试

不依赖真实 claude / 网络：

```bash
docker compose exec proxy python3 -m tests.test_merge
docker compose exec proxy python3 -m tests.test_translate
docker compose exec proxy python3 -m tests.test_api_e2e
```

或在 host 上：

```bash
python3 tests/test_merge.py
python3 tests/test_translate.py
python3 tests/test_api_e2e.py
```

### 6.6 抓真实流量做对照（调试用）

`recon/` 目录里有几个工具，用来对比真实 Claude Code 流量和 proxy
劫持后的流量：

```bash
python3 recon/capture.py            # mitmdump addon，dump 所有 flow
python3 recon/diff.py A.json B.json # 对比两份 flow
python3 recon/probe_worker_logic.py # 独立复现 worker 行为
```

---

## 7. 故障排查

### 7.1 `404 Not Found`

LiteLLM / 客户端打到了一条 proxy 没注册的路径。proxy 只暴露三条：

- `GET /healthz`
- `POST /v1/messages`
- `POST /v1/chat/completions`

最常见原因：`api_base` 后缀和 provider 不匹配：

| Provider | api_base 应该 |
|---|---|
| anthropic | `http://HOST:18787` **不带** /v1 |
| openai | `http://HOST:18787/v1` **必须带** /v1 |

### 7.2 `KeyError: 'stop_reason'`（LiteLLM 报）

LiteLLM 拿到了空响应或 error 响应。看 proxy 日志的
`hijacked outbound /v1/messages ... model=XXX`：

- `model=claude-opus-sub`（你自己的别名） → 客户端配错，把别名当真实
  模型 ID 发了 → 改成 `claude-opus-4-7` 等真实 ID
- `model=claude-opus-4-7`（真实 ID） → 看下一行 proxy 日志的 WARNING
  `upstream Anthropic ... error` —— 那是 Anthropic 的真实错误

> 修复后的 `_collapse_stream` 会保证返回的 message 一定有 `stop_reason`
> 字段，并把上游错误内容塞进 `content[0].text`，LiteLLM 不会再崩。

### 7.3 `401 invalid token`

config.yaml 里没找到这个 token。检查：

- 重启了 proxy 吗？config 改完要 `docker compose restart proxy`
- token 是不是带了多余的空白 / 引号
- 客户端发的真的是这个 key 吗（容器日志会显示请求源 IP）

### 7.4 容器 `[entrypoint] ERROR: missing /home/coder/.claude/.credentials.json`

凭据目录没挂进来或里面没有 `.credentials.json`。回去做 [§2.3](#23-步骤一准备-operator-凭据目录)：
确认宿主机 `/data/shared-auth/claude/.credentials.json` 存在，且
`docker-compose.yml` 的 volume 指向同一路径。

### 7.5 容器 `[entrypoint] ERROR: config has relative path(s)`

`config.yaml` 里 `ca_cert` 或 `home_template` 用了 `./` 或 `~`。Docker
部署必须用绝对路径：

```yaml
mitm:
  ca_cert: /home/coder/.mitmproxy/mitmproxy-ca-cert.pem
claude:
  home_template: /data/users/{user_id}
```

注意：项目根目录的 `config.yaml` 才是被挂载的那份；`docker/config.example.yaml`
是绝对路径模板，`config.example.yaml`（根目录）是 dev 模式用的相对路径版。
别拷错。

### 7.6 `PermissionError: [Errno 13] Permission denied`

容器以 uid 1000 运行，但某个 bind 目录在宿主机侧不是 1000 所有。
按报错路径对症：

| 报错路径 | 修复（宿主机上执行） |
|---|---|
| `/data/users/...` | `sudo chown -R 1000:1000 ./users` |
| `/home/coder/.claude/.credentials.json` | `sudo chown -R 1000:1000 /data/shared-auth/claude` |
| `/home/coder/.mitmproxy/...` | `docker compose down -v && docker compose up -d`（重建具名卷） |

根因见 [§2.5](#25-步骤三宿主机目录权限)。`sudo cp` 拷过来的目录默认是
root 所有，bind mount 不翻译 uid，所以容器里 uid 1000 读不了。

### 7.7 不想反复 chown：去掉 `user: "1000:1000"`

把 `docker-compose.yml` 里 `user: "1000:1000"` 那行删掉，容器就会以
root 启动。entrypoint 的 root 阶段会**自动** `chown` 好 `/data/users`、
`/home/coder/.mitmproxy`，再 `gosu` 降权到 `coder`(uid 1000) 跑服务 ——
最终进程仍是非 root，只是启动瞬间是 root，省掉手动 chown。

代价：启动瞬间有 root 权限。安全要求严格的环境保留 `user: "1000:1000"`，
按 §2.5 自己摆平宿主机目录权限。

### 7.8 worker 卡 60s 后 `did not signal ready`

claude TUI 启动失败。最常见：

- mitm CA 没有 / 路径不对 → claude 信任不了自签 HTTPS → npm registry /
  mcp-registry 等 bootstrap 请求超时
- OAuth 凭据过期 → claude 进入 onboarding → 等不到 `❯`
- 系统时钟漂移过大 → TLS 校验失败

进容器手动启动 claude 看 TUI 输出：

```bash
docker compose exec proxy bash -c '
  export HOME=/data/users/litellm
  export HTTPS_PROXY=http://127.0.0.1:18000
  export NODE_EXTRA_CA_CERTS=/home/coder/.mitmproxy/mitmproxy-ca-cert.pem
  claude
'
```

### 7.9 调用响应里 `cache_read_input_tokens = 0`

CLI 系统 prompt 缓存没命中 → 大概率请求里的 identity 字段被破坏了
→ 调用可能被 Anthropic 路由到 SDK 配额。检查：

- `User-Agent` 是不是被反向代理改写了？
- 是不是把 mitm 监听端口 (18000+) 也透出来了？客户端只能用对外的 18787
- docker-compose 上加了奇怪的 environment / labels 影响了 claude TUI 环境？

### 7.10 响应 `[upstream authentication_error] Invalid authentication credentials`

上游 Anthropic 拒绝了请求 —— OAuth token 失效。通常是源凭据过期、
且重新登录后旧的 refresh token 被轮换作废。修复：

1. 在已登录 claude 的机器上跑一次 `claude`（自动续期），或 `claude /login`
   重新登录
2. 把新的 `.claude/` 同步回部署机：

   ```bash
   cp -r <已登录 HOME>/.claude/. /data/shared-auth/claude/
   sudo chown -R 1000:1000 /data/shared-auth/claude
   ```

3. `docker compose restart proxy`

> 因为 `_seed_home` 现在是把 per-user 的 `.credentials.json` **软链接**
> 到 `/home/coder/.claude/.credentials.json`（不再拷贝），更新源凭据后
> worker 一重启就读到新的，**不需要再手动清 `./users/`**。如果你是从
> 旧版本升级上来的，`./users/` 里可能还留着旧的真实凭据文件 —— 新代码
> 启动时会自动 unlink 重建成软链接，但保险起见可以清一次 `rm -rf ./users/*`。

### 7.11 端口冲突

```
bind: address already in use
```

宿主机 18787 被占。改 `docker-compose.yml` 的 `ports` 左值
（`"0.0.0.0:<新端口>:8787"`），容器内 8787 不动。

### 7.12 构建时拉镜像 / 装包超时

`docker build` 卡在拉 `ubuntu:22.04`、`apt-get`、NodeSource、npm 或 pip。
国内网络配镜像源：

- Docker Hub：`/etc/docker/daemon.json` 加 `registry-mirrors`，重启 docker
- apt / npm / pip 换国内源（在 Dockerfile 对应步骤前插入镜像配置）

### 7.13 性能 / 限流

- 首次请求 ~7s（worker 冷启动）；后续 1–2s TTFB
- 同 user 串行；并发用多个 user
- worker 默认每 12h 就地重启一次（清 CLI 累积状态）；重启期间该 user 的新请求会等新 worker 起来再走，在飞请求最多等 60s 排空，超时被截断
- 想改：`claude.restart_interval_seconds`

---

## 8. 局限性 与 法律说明

### 8.1 已知局限

| 限制 | 缓解 |
|---|---|
| 不传 `tools` 时 CLI 内建工具（`Bash`/`Read`/`Edit`/...）会保留在请求里，模型可能去调它们 | 传你自己的 `tools=[...]`（会完全覆盖 CLI 的），或请求里加 `"tool_choice": {"type": "none"}` 完全禁用工具 |
| Anthropic 上游 4xx/5xx 会以 SSE error event 或非 SSE JSON 形式返回；HTTP 状态码不传递 | 已缓解：collapse 后的 message 含 `stop_reason: "error"` 和错误文本 |
| 单 OAuth 账号 = 单一订阅配额，所有 user 共享 | 多账号池待实现 |
| `assistant.content` 可能包含 `thinking` blocks（自适应思考模式） | 客户端兼容性问题；可在 API 层 strip |
| 无 metrics / 结构化日志 | 待加 Prometheus 导出 |

### 8.2 兼容性

强耦合 `claude` CLI 当前版本。任何 CLI 升级若改动以下任一项，链路就会坏：

- `system[0]` 计费头位置或文本
- `User-Agent` 模板
- `anthropic-beta` 取值
- TUI 提示符（`❯`）

如果新版 claude 改了上述任一字段，需要改 `src/mitm/addon.py` 里的合并
策略和 `src/pty_driver.py` 里的 ready marker。

### 8.3 法律说明

Anthropic 的 Acceptable Use Policy 和订阅 Terms of Service **禁止**：

- 把订阅席位分给多个真人用户
- 编程批量访问
- 任何形式的转售

跑这个 proxy 用于上述任一场景，账号会被封停，可能伴随支付方式黑名单。

本项目仅作为：

- **个人单席位多路复用**（让你自己的多个脚本共用同一个订阅）
- **学术 / 安全研究**（研究订阅 vs API 配额的传输层差异）

**不要**：

- 暴露到公网
- 发放给非授权用户
- 商业 / 生产负载使用
- 配合代理链 / IP 轮换等"账号共享"工具

完全自担风险使用。

---

## 附：开发模式（不走 Docker）

直接在宿主机跑，凭据用本机 `~/.claude/`，配置用相对路径。

```bash
# 装依赖
python3 -m venv .venv
source .venv/bin/activate
pip install -e .

# 配置：根目录的 config.example.yaml 用相对路径，本地开发不用改
cp config.example.yaml config.yaml
# 编辑 users:，加你的 key

# 预生成 mitm CA（HOME 指向当前已登录 claude 的家目录）
timeout 5 mitmdump --listen-port 19998 -q

# 启动（HOME 指向有 .claude/.credentials.json 的家目录）
HOME=/home/coder CONFIG=config.yaml LOG_LEVEL=INFO python3 -m src.main
```

dev 模式下 `config.yaml` 的 `ca_cert` / `home_template` 可以用相对路径
（`./users/{user_id}`、`~/.mitmproxy/...`）；只有 Docker 部署强制绝对路径。
