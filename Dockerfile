#
# Claude Code subscription proxy — runtime image.
#
# Base: Ubuntu 22.04 (system Python 3.10) + Node.js 20 + claude code CLI.
#
# Operator OAuth credentials are NEVER baked into the image. They are
# bind-mounted at runtime into /home/coder/.claude/ from a host directory
# (default: /data/share-auth/claude) containing a real claude CLI's
# .claude/* state. See docker-compose.yml.
#

FROM ubuntu:22.04 AS base

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NODE_VERSION=20 \
    TZ=UTC

# Pin claude code CLI to a known-good version. The proxy depends on
# private CLI internals (system[0] billing header, anthropic-beta token
# layout, `❯` prompt symbol, lazy-bootstrap endpoint list, OAuth client
# id at `9d1c250a-…` and token endpoint `/v1/oauth/token`); any of those
# could break across CLI releases without notice. Bumping is a manual
# act: change this default, rebuild, run the smoke test, ship.
# Override at build time when validating a new upstream:
#   docker build --build-arg CLAUDE_CODE_VERSION=2.1.140 .
ARG CLAUDE_CODE_VERSION=2.1.139

# --- system deps ---
# python3 / python3-pip:        Ubuntu 22.04 自带 Python 3.10
# curl ca-certificates gnupg:   装 NodeSource 仓库 + claude 网络调用
# gosu:                          entrypoint root 修卷权限后降权用
# procps:                        容器调试 (ps / pgrep)
# tzdata:                        日志时间戳
RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        curl \
        ca-certificates \
        gnupg \
        gosu \
        procps \
        tzdata \
        && rm -rf /var/lib/apt/lists/*

# --- node + claude code CLI ---
RUN curl -fsSL https://deb.nodesource.com/setup_${NODE_VERSION}.x | bash - \
    && apt-get update && apt-get install -y --no-install-recommends nodejs \
    && npm install -g --no-audit --no-fund "@anthropic-ai/claude-code@${CLAUDE_CODE_VERSION}" \
    && claude --version \
    && rm -rf /var/lib/apt/lists/*

# --- non-root user `coder` (uid 1000) ---
# HOME is /home/coder. The host directory holding the live claude CLI
# state (default /data/share-auth/claude) bind-mounts into
# /home/coder/.claude/ at runtime; entrypoint synthesises a minimal
# /home/coder/.claude.json if one is not provided. mitm CA is generated
# under /home/coder/.mitmproxy (named volume in compose so it survives
# restarts). /data/users holds per-API-user isolated HOMEs (one subdir
# per entry in config.yaml `users:`).
RUN useradd --create-home --shell /bin/bash --uid 1000 coder \
    && mkdir -p /home/coder/.claude /home/coder/.mitmproxy /data/users \
    && chown -R coder:coder /home/coder /data

WORKDIR /app

# --- python deps ---
# Ubuntu 22.04 自带 pip 没启用 PEP 668，可以直接 pip install。
RUN python3 -m pip install --no-cache-dir \
        "fastapi==0.128.1" \
        "uvicorn[standard]==0.47.0" \
        "uvloop==0.21.0" \
        "httptools==0.6.4" \
        "mitmproxy==11.0.2" \
        "cryptography==43.0.3" \
        "pyOpenSSL==24.2.1" \
        "ptyprocess==0.7.0" \
        "httpx==0.28.1" \
        "pydantic==2.9.2" \
        "pyyaml==6.0.3"

# --- source ---
COPY --chown=coder:coder src/ ./src/
COPY --chown=coder:coder pyproject.toml ./
COPY --chown=coder:coder docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# HOME=/home/coder → _seed_home reads OAuth creds from this dir
# CONFIG=/data/config.yaml → mounted in at runtime
# DISABLE_AUTOUPDATER=1 → claude CLI's npm self-upgrade would fail
#   anyway (global npm prefix is root-owned, container runs as uid 1000)
#   but it hangs the TUI for tens of seconds while it tries. Belt-and-
#   braces with the per-spawn setting in src/pty_driver.py for anyone
#   who runs `docker compose exec proxy claude ...` to debug.
ENV HOME=/home/coder \
    CONFIG=/data/config.yaml \
    LOG_LEVEL=INFO \
    DISABLE_AUTOUPDATER=1

# Stay root for entrypoint so we can chown runtime-mounted volumes; entrypoint
# gosu-drops to `coder` before exec'ing the server.

EXPOSE 8787

ENTRYPOINT ["/entrypoint.sh"]
CMD ["python3", "-m", "src.main"]
