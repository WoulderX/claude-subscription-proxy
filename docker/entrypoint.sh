#!/bin/sh
# Container entrypoint — runs as root long enough to fix volume ownership,
# then `gosu`-drops to the unprivileged `coder` user before exec'ing the server.
set -eu

HOME="${HOME:-/home/coder}"
CONFIG="${CONFIG:-/data/config.yaml}"

# Phase 1: root-only fixups. Docker-managed named volumes + bind mounts
# typically come in root-owned regardless of the image's USER directive,
# which breaks per-user state writes and mitm CA generation.
if [ "$(id -u)" = "0" ]; then
    # /home/coder/.claude is bind-mounted from a shared host path; we
    # deliberately leave its ownership alone (other containers share it).
    # The dirs that MUST be writable: $HOME/.mitmproxy
    # (mitm CA), /data/users (per-API-user transcripts), and $HOME itself
    # (entrypoint writes .claude.json there if it's missing).
    for d in "$HOME" "$HOME/.mitmproxy" /data/users; do
        [ -d "$d" ] || mkdir -p "$d"
        chown coder:coder "$d" 2>/dev/null || true
    done
    # Re-exec self as `coder` so the rest of the script runs unprivileged.
    exec gosu coder:coder "$0" "$@"
fi

echo "[entrypoint] HOME=$HOME CONFIG=$CONFIG uid=$(id -u) gid=$(id -g)"

# 1. Operator OAuth tokens come from a bind-mounted directory at
#    $HOME/.claude/ (default source: /data/share-auth/claude on host).
if [ ! -f "$HOME/.claude/.credentials.json" ]; then
    echo "[entrypoint] ERROR: missing $HOME/.claude/.credentials.json"
    echo "[entrypoint]"
    echo "[entrypoint] docker-compose.yml expects host directory"
    echo "[entrypoint]   /data/share-auth/claude/  (contains .credentials.json"
    echo "[entrypoint]                               and the rest of \$HOME/.claude/)"
    echo "[entrypoint] bind-mounted into the container at /home/coder/.claude."
    echo "[entrypoint]"
    echo "[entrypoint] On the host, run \`claude /login\` first, then mirror"
    echo "[entrypoint] ~/.claude/ into /data/share-auth/claude/."
    exit 1
fi

# 2. .claude.json marker. The proxy's _seed_home (src/session/session.py)
#    reads $HOME/.claude.json to skip claude CLI's onboarding (theme picker,
#    etc.) inside per-user workers. If a real one wasn't provided, synthesise
#    a minimal one — claude CLI only strictly requires hasCompletedOnboarding.
if [ ! -f "$HOME/.claude.json" ]; then
    echo "[entrypoint] $HOME/.claude.json absent; writing minimal stub"
    cat > "$HOME/.claude.json" <<'JSON'
{"hasCompletedOnboarding": true, "numStartups": 1}
JSON
fi

# 3. mitm CA — mitmproxy auto-generates it on first run; the directory
#    /home/coder/.mitmproxy is a named volume so the CA persists across
#    `docker compose restart`. Nothing to do here.

# 4. Config file must be mounted.
if [ ! -f "$CONFIG" ]; then
    echo "[entrypoint] ERROR: config file not found at $CONFIG"
    echo "[entrypoint] mount one with: -v ./config.yaml:/data/config.yaml:ro"
    exit 1
fi

# 5. Sanity-check config paths: relative paths blow up inside the container
#    because WORKDIR=/app is root-owned and not writable by uid 1000.
bad=
if grep -qE '^[[:space:]]*home_template:[[:space:]]*\./' "$CONFIG"; then
    bad="$bad home_template"
fi
if grep -qE '^[[:space:]]*ca_cert:[[:space:]]*[~.]' "$CONFIG"; then
    bad="$bad ca_cert"
fi
if [ -n "$bad" ]; then
    echo "[entrypoint] ERROR: config has relative path(s) in:$bad"
    echo "[entrypoint] In Docker the worker subprocess runs with CWD=/app (root-"
    echo "[entrypoint] owned, not writable). Use absolute paths:"
    echo "[entrypoint]   home_template: /data/users/{user_id}"
    echo "[entrypoint]   ca_cert: /home/coder/.mitmproxy/mitmproxy-ca-cert.pem"
    echo "[entrypoint] (see docker/config.example.yaml for a working template)"
    exit 1
fi

# 6. Per-user state dir.
mkdir -p /data/users

echo "[entrypoint] launching: $*"
exec "$@"
