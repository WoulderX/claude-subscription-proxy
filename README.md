# claude-subscription-proxy

把 **Claude Code 订阅账号（Max / Pro）** 包装成本地的 **Anthropic / OpenAI 兼容 HTTP API**——脚本、IDE 插件、LiteLLM 都按标准 API 调用，Anthropic 把请求计入订阅配额而不是 API 配额。

```
你的应用 / LiteLLM / SDK         本服务            Anthropic
──────────────────────          ─────            ─────────
  curl ─┐                    ┌───────────┐
  SDK ──┼──► HTTP/JSON ────►│  FastAPI   │──► HTTPS ──► api.anthropic.com
  LiteLLM ┘                  │   :8787   │    （挂着真 claude CLI 作"身份代笔"）
                             └───────────┘
```

核心点：每个账号跑一个或多个 worker，worker 里运行真实的 `claude` CLI；用户请求通过 mitmproxy 注入到 CLI 即将发出的 `/v1/messages` 请求体中，让 Anthropic 看到的是带完整订阅指纹的真 CLI 调用。

---

## 目录

1. [快速开始](#1-快速开始)
2. [配置](#2-配置)
3. [API 使用](#3-api-使用)
4. [运维与监控](#4-运维与监控)
5. [工作原理（简版）](#5-工作原理简版)
6. [常见问题](#6-常见问题)

---

## 1. 快速开始

### 前置

- Docker 24+，Docker Compose v2
- 至少一台已经 `claude /login` 完成的机器（拷贝凭据用）
- 宿主机端口 `18787` 可用

### Step 1 — 准备账号凭据

每个 Claude 账号一个目录，含**两个文件**：

| 文件 | 来源 |
|---|---|
| `.credentials.json` | `~/.claude/.credentials.json`（login 后生成） |
| `.claude.json` | `~/.claude.json`（**`.claude/` 的兄弟文件，不在里面**） |

```bash
# 在已登录的机器上打包
tar -C ~ -cf /tmp/acc1.tar .claude .claude.json

# 在部署机上摆好（目录名约定为 claude-N）
sudo mkdir -p /data/shared-auth/claude-1
sudo tar -C /data/shared-auth/claude-1 -xf /tmp/acc1.tar --strip-components=1
sudo mv /data/shared-auth/claude-1/.claude/* /data/shared-auth/claude-1/
sudo mv /data/shared-auth/claude-1/.claude/.[!.]* /data/shared-auth/claude-1/ 2>/dev/null
sudo rmdir /data/shared-auth/claude-1/.claude
sudo chown -R 1000:1000 /data/shared-auth
```

> `cp -r ~/.claude X` 不会带上 `.claude.json`——它是兄弟文件，必须单独拷。少了它 TUI 显示 `Not logged in`，worker 永远 prewarm 失败。

### Step 2 — 写 `config.yaml`

```bash
cp config.example.yaml config.yaml
echo "sk-internal-$(openssl rand -hex 24)"   # 生成对外 key
```

最小可用配置：

```yaml
listen_host: 0.0.0.0
listen_port: 8787
mitm:
  port_base: 18000
  ca_cert: /home/coder/.mitmproxy/mitmproxy-ca-cert.pem
claude:
  binary: claude
  home_template: /data/users/{user_id}
  restart_interval_seconds: 14400        # 4h，必须 < OAuth 寿命（~8h）

accounts:
  claude-1:
    dir: /data/shared-auth/claude-1
    workers: 5                            # 单账号 5 worker 是经验上限
  claude-2:
    dir: /data/shared-auth/claude-2
    workers: 5

api_key: sk-internal-...                 # 对外单 key，pool 所有 worker
```

> 路径必须**绝对**——worker 子进程 CWD 是 `/app`，相对路径会解析错位置。

### Step 3 — 改 docker-compose 的 volumes

每个账号一行 bind mount（漏了会启动时报 `Missing: /data/shared-auth/claude-N/.credentials.json`）：

```yaml
volumes:
  - ./config.yaml:/data/config.yaml:ro
  - /data/shared-auth/claude-1:/data/shared-auth/claude-1
  - /data/shared-auth/claude-2:/data/shared-auth/claude-2
  - mitm-ca:/home/coder/.mitmproxy
  - ./users:/data/users
  - ./data:/data/proxy                   # usage.db 落这里
```

### Step 4 — 启动

```bash
sudo mkdir -p ./users ./data
sudo chown -R 1000:1000 ./users ./data

docker compose up --build -d
docker compose logs -f proxy
```

启动需要 ~30s（账号间并行 bootstrap，账号内串行预热每个 worker）。`bootstrap prewarm complete` 出现就可以打了。

### Step 5 — 第一发请求

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

响应里 `usage.cache_read_input_tokens > 0` 说明走的是订阅配额（CLI 系统 prompt 命中缓存）。

监控面板：`http://YOUR-HOST:18787/ui`，输入 API key 即可。

---

## 2. 配置

### 2.1 字段总览

| 字段 | 默认 | 热重载 | 说明 |
|---|---|---|---|
| `listen_port` | 8787 | ❌ | FastAPI 端口；Docker 里固定 8787，宿主映射到 18787 |
| `mitm.port_base` | 18000 | ❌ | mitm 端口起点，user N 用 `port_base + N` |
| `mitm.ca_cert` | — | ❌ | mitm CA 路径；首次启动自动生成 |
| `claude.restart_interval_seconds` | 14400 | ✅ | worker 定时重启间隔；必须 < OAuth 寿命（~8h） |
| `claude.session_affinity` | true | ✅ | 同一会话路由到同一账号，保 prompt cache |
| `claude.session_affinity_ttl_seconds` | 600 | ✅ | 空闲会话绑定的 TTL |
| `claude.rate_limit_base_cooldown_seconds` | 120 | ✅ | 裸 429（无 reset 头）首次冷却 |
| `claude.rate_limit_max_cooldown_seconds` | 600 | ✅ | 指数退避封顶 |
| `claude.timeouts.*` | 见下 | ✅ | 7 个细分 timeout |
| `oauth_refresh.enabled` | true | ✅ | 主进程集中刷 OAuth；关掉只用于排障 |
| `oauth_refresh.check_interval_seconds` | 300 | ✅ | 刷新检查周期 |
| `oauth_refresh.refresh_when_expires_within_seconds` | 3600 | ✅ | 距过期 < 此值就刷 |
| `usage.enabled` | true | ❌ | token 用量记录（sqlite + /admin/usage + UI 面板） |
| `usage.db_path` | `/data/proxy/usage.db` | ❌ | sqlite 文件路径，务必落在 bind 挂载内 |
| `accounts` | — | ❌ | 账号 → `{dir, workers, priority}` 映射 |
| `api_key` | — | ❌ | 对外单 key，pool 所有 worker |
| `users` | — | ✅ | `token → user_id` 映射；可替代 `api_key` 做细粒度路由 |

**timeouts 子项**（默认值，仅在确实看到对应失败时调）：

| 字段 | 默认 | 说明 |
|---|---|---|
| `mitm_intercept_seconds` | 90 | PTY 触发后 mitm 多久没拦到 `/v1/messages` 就放弃 |
| `status_stall_seconds` | 30 | `/status` 标 `stuck` 的阈值 |
| `response_stall_seconds` | 90 | mitm watchdog 强制关闭 channel 的阈值 |
| `restart_drain_seconds` | 60 | 重启时等 in-flight 排空的最长时间 |
| `worker_ready_seconds` | 60 | 新 worker `{"type":"ready"}` 超时 |
| `prewarm_seconds` | 60 | bootstrap prewarm 上限 |
| `restart_check_interval_seconds` | 60 | `_restarter` 巡检间隔 |

### 2.2 账号路由优先级

`accounts.<name>.priority`（默认 100，**越小越优先**）。picker 在同 tier 内空闲优先 + 最低 burn + 防扎堆；只有当上层 tier 全部 cool-idle 都没有时才下沉到下层。

```yaml
accounts:
  claude-max-1: { dir: /data/shared-auth/claude-max-1, workers: 5, priority: 100 }
  claude-max-2: { dir: /data/shared-auth/claude-max-2, workers: 5, priority: 100 }
  claude-pro-1: { dir: /data/shared-auth/claude-pro-1, workers: 3, priority: 200 }
```

Max 满了才溢出到 Pro，避免低单价账号被过早烧。

### 2.3 限流与并发

- **单账号 worker 数建议 ≤ 5**——Anthropic 订阅有 per-OAuth 并发限制（实测 2-3），再多撞 429
- **配额按车道分离**：sonnet 子配额 / 5h / 7d unified 各自独立；某账号 sonnet 满了，opus 仍可路由到同账号（lane-aware routing）
- **会话亲和优先**：默认开。同会话固定到同账号保持 prompt cache 命中（cache miss = 重新写 50k+ tokens，比限流损失更大）
- **冷却策略**：
  - 有权威 reset header → 用真实窗口
  - 裸 429 无 reset → per-account 指数退避（120 → 240 → 480 → 600 封顶）
  - 推断的 weekly_limit 上限 2h（防止单次 429 锁死账号一周）
  - 5h pinned 标记允许 success-demote（5h 滑动窗常常比 reset_at 提前释放）

### 2.4 多 key 路由（少数场景）

`accounts:` + `users:` 同时存在，`users:` 把自动生成的 user_id 分成几组：

```yaml
accounts:
  claude-1: { dir: /data/shared-auth/claude-1, workers: 5 }
  claude-2: { dir: /data/shared-auth/claude-2, workers: 5 }
users:
  sk-team-a: [claude-1-0, claude-1-1, claude-1-2, claude-1-3, claude-1-4]
  sk-team-b: [claude-2-0, claude-2-1, claude-2-2, claude-2-3, claude-2-4]
```

### 2.5 热重载

```bash
curl -X POST -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/admin/reload
```

可热重载字段见 §2.1 表"热重载"列。`mitm_intercept_seconds` / `response_stall_seconds` 通过 worker argv 注入，只对新 spawn 的 worker 生效——立即生效要 `POST /admin/workers/{id}/restart`。

---

## 3. API 使用

两种鉴权方式任选：`Authorization: Bearer <token>` 或 `x-api-key: <token>`。

### 3.1 Anthropic 原生 — `POST /v1/messages`

```bash
# 非流
curl -sS -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{"model":"claude-opus-4-7","max_tokens":512,
       "messages":[{"role":"user","content":"hi"}]}'

# 流式
curl -sS -N -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{"model":"claude-opus-4-7","max_tokens":512,"stream":true,
       "messages":[{"role":"user","content":"hi"}]}'
```

**自定义 system**（替换 claude code 原人设，保留计费头）：

```bash
curl -sS -X POST http://127.0.0.1:18787/v1/messages \
  -H "Authorization: Bearer $KEY" \
  -d '{"model":"claude-opus-4-7","max_tokens":256,
       "system":"You are a terse code reviewer. Reply in at most three bullets.",
       "messages":[{"role":"user","content":"评审：eval(user_input)"}]}'
```

你的 system 自动带 `cache_control: ephemeral`，重复用便宜很多。

**禁用 claude code 内置工具**：

```bash
# 法 1：不调用任何工具
"tool_choice": {"type": "none"}
# 法 2：传你自己的 tools[] 完全覆盖内置 11 个
```

### 3.2 OpenAI 兼容 — `POST /v1/chat/completions`

```bash
curl -sS -X POST http://127.0.0.1:18787/v1/chat/completions \
  -H "Authorization: Bearer $KEY" \
  -d '{"model":"claude-opus-4-7","stream":true,
       "messages":[
         {"role":"system","content":"Be concise."},
         {"role":"user","content":"What is HTTP/2 multiplexing?"}
       ]}'
```

字段转换由 `src/api/translate.py` 完成（OpenAI `system` → Anthropic `system`，`tools` / `tool_calls` 双向映射）。

### 3.3 Python SDK

```python
# Anthropic SDK
from anthropic import Anthropic
client = Anthropic(base_url="http://127.0.0.1:18787", api_key=KEY)
resp = client.messages.create(
    model="claude-opus-4-7", max_tokens=256,
    messages=[{"role":"user","content":"hi"}])

# OpenAI SDK
from openai import OpenAI
client = OpenAI(base_url="http://127.0.0.1:18787/v1", api_key=KEY)
resp = client.chat.completions.create(
    model="claude-opus-4-7",
    messages=[{"role":"user","content":"hi"}])
```

### 3.4 LiteLLM 接入

LiteLLM 侧**只配一个 key**，本服务自动 pool 并发。

```yaml
model_list:
  # Anthropic 原生路径（推荐，少一层转换）
  - model_name: claude-opus-sub
    litellm_params:
      model: anthropic/claude-opus-4-7         # 真实模型 ID（必须！）
      api_base: http://PROXY_HOST:18787        # 不带 /v1
      api_key: sk-internal-...

  # OpenAI 兼容路径
  - model_name: claude-opus-openai
    litellm_params:
      model: openai/claude-opus-4-7
      api_base: http://PROXY_HOST:18787/v1     # 带 /v1
      api_key: sk-internal-...
```

> 常见错配：把 `model_name` 和 `model` 填一样，LiteLLM 把别名当真实 ID 发出去 → `KeyError: 'stop_reason'`。

**透传 LiteLLM 用户身份到 /ui**（可选，**需要 LiteLLM ≥ v1.85.0**）：

```yaml
# proxy_config.yaml 顶层
litellm_settings:
  add_user_information_to_llm_headers: true
```

打开后 LiteLLM 会发 `x-litellm-user-api-key-*` 头，本服务自动捕获显示到 dashboard 的「LiteLLM 用户」列。v1.84.x 及更早有 [#27458](https://github.com/BerriAI/litellm/issues/27458) bug，**所有请求 500**。

### 3.5 支持的模型 ID

任何 Anthropic 接受的 ID 都行，proxy 不维护白名单。常用：

- `claude-opus-4-7` / `claude-opus-4-6` / `claude-opus-4-5-20251101`
- `claude-sonnet-4-6` / `claude-sonnet-4-5-20250929`
- `claude-haiku-4-5-20251001`

---

## 4. 运维与监控

### 4.1 Dashboard `/ui`

浏览器打开 `http://YOUR-HOST:18787/ui`，输入 API key。包含：

- **系统概览**——worker / 账号 健康度汇总
- **订阅配额**——按账号优先级分组的配额卡（5h / 7d 合计 / 7d sonnet 三条 bar）
- **账号路由状态**——按优先级分组的可折叠列表，每行显示状态徽章 + lane pills（`5h / 7d / snt` 利用率）+ 剩余冷却；支持「全部 / 限流中 / 健康」过滤
- **用量统计**——按账号 / worker / LiteLLM 用户聚合，估算 USD；按 lifecycle / today / 7d 切换
- **Worker 运行状态**——每 worker 一行，含状态徽章 / 当前任务 preview / 重启按钮

状态徽章会精确反映**车道**："sonnet 限流" / "全部限流" / "5h 限流"——比单纯说"限流"信息密度高一档。

### 4.2 `/status` 实时状态

```bash
curl -s -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/status | jq
```

关键字段：

| 字段 | 含义 |
|---|---|
| `worker_count` / `alive_count` / `busy_count` / `stuck_count` | 顶层汇总，**真在干活 = busy - stuck** |
| `workers[].in_flight` / `stuck` | 当前挂了几个请求 / 任一 stalled > 30s |
| `workers[].in_flight_detail[].bytes_received` | 上游字节数；0 = 还没回过；150-170 = 典型错误信封 |
| `workers[].in_flight_detail[].stalled_seconds` | 距上次收字节多久；> 90s watchdog 自动回收 |
| `accounts[].lanes` | 按 lane 分的 RL 状态（`5h` / `unified` / `sonnet` / `degraded`） |

### 4.3 `/admin/*` 运维端点

```bash
# 改完 config.yaml 后热重载
curl -X POST -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/admin/reload

# 单独重启 worker（drain in-flight → restart → prewarm）
curl -X POST -H "Authorization: Bearer $KEY" \
  http://127.0.0.1:18787/admin/workers/claude-1-0/restart

# 强刷一次 OAuth（跳过 5min 周期）
curl -X POST -H "Authorization: Bearer $KEY" http://127.0.0.1:18787/admin/refresh-now

# token 用量查询
curl -s -H "Authorization: Bearer $KEY" \
  "http://127.0.0.1:18787/admin/usage?range=today&group_by=account"

# 手动设置 / 解除限流标记
curl -X POST -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"reset_at":"May 27, 12am UTC","reason":"weekly_limit"}' \
  http://127.0.0.1:18787/admin/accounts/claude-1/set-rate-limit
curl -X POST -H "Authorization: Bearer $KEY" \
  http://127.0.0.1:18787/admin/accounts/claude-1/clear-rate-limit

# Dashboard 添加新账号
curl -X POST -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"name":"claude-3","workers":5,"priority":100}' \
  http://127.0.0.1:18787/admin/accounts/new
```

### 4.4 加账号 / 加用户

**加账号**（三处必须同时改）：

1. 宿主机准备 `/data/shared-auth/claude-N/`，`chown -R 1000:1000`
2. `docker-compose.yml` 的 `volumes:` 加 bind mount
3. `config.yaml` 的 `accounts:` 加一段
4. `docker compose up -d --force-recreate`（新挂载必须重建）

**或者直接走 Dashboard**：`/ui` 右上角「+ 添加账号」会引导 OAuth login，自动写 `data/accounts.runtime.yaml`，热生效不用重启。

### 4.5 日志级别

容器默认 `LOG_LEVEL=INFO`——只打 lifecycle 事件（spawn / restart / 限流 / admin 调用），正常 traffic 不打 per-request 日志。

调试切到 DEBUG：

```yaml
# docker-compose.yml
environment:
  LOG_LEVEL: DEBUG
```

restart 后 mitm hijack / body 改写 / SSE 末态 / 429 详情等都会回来。

### 4.6 暴露给其他主机

默认绑到 `0.0.0.0:18787`，局域网内可直接访问（用来接 LiteLLM）。强烈建议：

- 仅在受信任内网开放
- 或前置 nginx / caddy 做 TLS + IP 白名单
- **绝不要暴露到公网**——无速率限制，且违反 ToS

仅本机访问：把 compose 的 `ports` 改成 `"127.0.0.1:18787:8787"`。

---

## 5. 工作原理（简版）

完整数据流：

```
HTTP 请求
   ↓ FastAPI 鉴权 + 池子选 worker
   ↓ stdin JSON 行 IPC
worker 子进程：
   · mitmproxy 监听 18000+N（HTTPS_PROXY + NODE_EXTRA_CA_CERTS 让 claude 信任 mitm CA）
   · claude CLI 跑在 PTY 里
   · "say hi\r" 触发 claude 发 POST /v1/messages?beta=true（带完整订阅指纹）
   · mitm 拦下，按白名单合并用户 body 进去，转发
   · 响应字节经 mitm `_tap` 双向喂：①送给 channel 流回主进程 ②回灌给 claude 让 TUI 不卡
   ↓ stdout JSON 行 IPC
主进程 StreamingResponse → 客户端
```

**为什么不直接拼请求？** Anthropic 用 10+ 项指纹（`system[0]` 计费头、`User-Agent`、`anthropic-beta`、`x-stainless-*`、完整 `tools[]`、`metadata` 等）判定订阅 vs API 配额。手工拼难以长期跟上 CLI 升级。让真实 CLI 发包是最稳的"借身份"。

**body 合并规则**（`src/mitm/addon.py:_merge_body`）：

| 字段 | 谁说了算 | 原因 |
|---|---|---|
| `messages` / `model` / `max_tokens` / `temperature` / `tools` / `tool_choice` | 用户 | 业务输入 |
| `system` 块 0（计费头）+ 用户的 system 块（自动加 `cache_control: ephemeral`） | **混合** | 保身份 + 用户指令生效 + 后续缓存命中 |
| `User-Agent` / `anthropic-*` / `x-stainless-*` / `metadata` / `thinking` / `output_config` | claude 原值 | identity 指纹 |

> 客户端没传 `tools[]` 时 claude 内置的 11 个工具（Bash/Read/Edit/...）会原样留——模型可能去调它们。要禁工具：`"tool_choice": {"type": "none"}`，或传自己的 `tools[]` 覆盖。

### 5.1 自愈兜底（部分）

| 触发 | 行为 |
|---|---|
| PTY 触发后 90s 无 mitm 拦截 | 关闭 channel + dump PTY screen tail 到日志（覆盖 TUI 卡 modal 吞 keystroke） |
| TUI 弹工具权限对话框 | 自动写 `\x1b` (Esc) dismiss（最常见的卡住根因） |
| hijack 后 90s 无上游 chunk | watchdog 强制关闭 channel 唤醒 worker |
| 流式响应 0 byte / 半截 SSE | 合成完整合法 SSE 序列防客户端崩 |
| primary worker 10s 无首字节 | hedge 并发 backup worker，谁先吐字节用谁 |
| 连续 N 次 intercept 失败 | 强制重启该 worker 自愈 |
| 账号收到 `rate_limit_error` | 标进车道冷却表（5h / unified / sonnet 独立），picker 按 model 跳过 |
| worker age > 4h | 定时 drain + restart + prewarm（清 CLI 内存 + 换 token） |

详见 `src/session/manager.py` 和 `src/worker.py`。

### 5.2 凭据共享

每个账号一个目录 `/data/shared-auth/<name>/`，同账号的 N 个 worker 都把 `$HOME/.claude` **目录级 symlink** 到这里——共享 OAuth 凭据。主进程 `OAuthRefresher` 是该 `.credentials.json` 的**唯一写入者**（每 5min 检查，距过期 < 1h 就刷），物理上排除多 worker 撞 refresh_token 轮换的竞态。

> 必须**目录级 symlink**：claude CLI 写 token 用"写 tmp + rename"，文件级 symlink 会被 rename 直接覆盖成真文件，导致 refresh 写不回源。

---

## 6. 常见问题

**Q: 启动 entrypoint 报 `Missing: /data/shared-auth/claude-N/.credentials.json`？**
A: docker-compose 的 `volumes:` 漏了对应账号的 bind mount。加上后 `docker compose up -d --force-recreate`。

**Q: 启动卡在 `bootstrap prewarm`？**
A: 90% 是 `.claude.json` 没拷上，TUI 显示 `Not logged in`。检查每个账号目录是否同时有 `.credentials.json` 和 `.claude.json` 两个文件。

**Q: 请求一直 429？**
A: 单账号 worker 数太多。订阅 OAuth 实测并发上限 2-3，pool 配 10 个 worker 共用一个账号 = 7-8 倍超限。把 `workers:` 降到 5 以下，或加多账号摊开。

**Q: 监控面板上某个账号显示 `sonnet 限流` 但不是 `全部限流`？**
A: Anthropic 的 sonnet 子配额满了，但 unified 7d 还有空间——proxy 现在按车道分离路由，opus 请求仍能落到这账号，sonnet 自动跳过。

**Q: `usage.cache_read_input_tokens` 一直是 0？**
A: 上游没命中 prompt cache，可能没走订阅配额。检查响应 header 里有没有 `anthropic-billing-tier`，或换台无中间代理直连试。

**Q: token 刷新没生效？**
A: 主进程是唯一刷新者，强刷一次 `POST /admin/refresh-now` 看返回。返回 `failed` 说明 OAuth 端点拒绝了，多半是 refresh_token 已被作废——需要重新 `claude /login` 后同步凭据目录。

**Q: dev 模式本机直跑？**
A: `config.yaml` 把两条路径改回 `~/.mitmproxy/...` 和 `./users/{user_id}`，然后 `HOME=/path/to/.claude-host CONFIG=config.yaml python3 -m src.main`。

**Q: 想看更多实现细节？**
A: 关键文件索引：
- 请求流：`src/api/anthropic.py`（`_open_request`）+ `src/mitm/addon.py`（`_merge_body` / `responseheaders`）
- 路由 + 限流 + affinity：`src/session/manager.py`（`pick` / `_account_rl` / `_select_for_new_session`）
- worker 生命周期：`src/worker.py` + `src/pty_driver.py`
- OAuth 刷新：`src/oauth_refresh.py`
- 配额监控：`src/quota_probe.py`
- Dashboard：`src/static/admin.html`
