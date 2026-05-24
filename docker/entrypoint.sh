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

# 1. Operator OAuth tokens. Two layouts:
#    - Multi-account: config has an `accounts:` block mapping each
#      account name to a `dir:` (e.g. /data/shared-auth/claude-1).
#      Each dir must contain .credentials.json.
#    - Legacy single-account: $HOME/.claude/ is bind-mounted from the
#      host (default: /data/shared-auth/claude) and must contain
#      .credentials.json.
#    Dispatch via cheap grep; no YAML parser inside the entrypoint.
if grep -qE '^accounts:[[:space:]]*$' "$CONFIG"; then
    missing=
    unreadable=
    # Extract `dir:` lines that sit under the accounts: block. The awk
    # range starts at `accounts:` and ends at the next top-level key
    # (a line with non-whitespace as its first character).
    # shellcheck disable=SC2013
    for dir in $(awk '
        /^accounts:[[:space:]]*$/ { in_block = 1; next }
        /^[^[:space:]]/           { in_block = 0 }
        in_block && $1 == "dir:"  { gsub(/"|'\''/, "", $2); print $2 }
    ' "$CONFIG"); do
        f="$dir/.credentials.json"
        if [ ! -e "$f" ]; then
            missing="$missing\n  $f"
        elif [ ! -r "$f" ]; then
            # Exists but uid 1000 can't read it (typical: file is
            # root-owned 0600 because operator ran claude /login as
            # root or sudo cp dropped root ownership). This is the
            # mode that historically caused phantom "rate limit"
            # behaviour — credentials silently unread, claude CLI
            # boots into "Not logged in" state.
            unreadable="$unreadable\n  $f"
        fi
    done
    if [ -n "$missing" ] || [ -n "$unreadable" ]; then
        echo "[entrypoint] ERROR: per-account credentials check failed."
        if [ -n "$missing" ]; then
            echo "[entrypoint] Missing inside the container:"
            printf "[entrypoint]%b\n" "$missing"
            echo "[entrypoint]"
            echo "[entrypoint] Most likely cause: account was added to"
            echo "[entrypoint] config.yaml without a matching bind mount"
            echo "[entrypoint] in docker-compose.yml. Each account in"
            echo "[entrypoint] config.yaml's accounts: block needs one"
            echo "[entrypoint] line under volumes::"
            echo "[entrypoint]   - /data/shared-auth/<name>:/data/shared-auth/<name>"
            echo "[entrypoint] Then: docker compose up -d --force-recreate"
        fi
        if [ -n "$unreadable" ]; then
            echo "[entrypoint] Unreadable by uid $(id -u):"
            printf "[entrypoint]%b\n" "$unreadable"
            echo "[entrypoint] Fix on the host:"
            echo "[entrypoint]   sudo chown -R 1000:1000 /data/shared-auth"
        fi
        echo "[entrypoint]"
        echo "[entrypoint] Each accounts[<name>].dir must contain a"
        echo "[entrypoint] .credentials.json readable by uid 1000."
        exit 1
    fi
elif [ ! -r "$HOME/.claude/.credentials.json" ]; then
    echo "[entrypoint] ERROR: missing $HOME/.claude/.credentials.json"
    echo "[entrypoint]"
    echo "[entrypoint] Single-account legacy layout expects"
    echo "[entrypoint]   /data/shared-auth/claude/  (contains .credentials.json)"
    echo "[entrypoint] bind-mounted into the container at /home/coder/.claude."
    echo "[entrypoint] For multi-account, set \`accounts:\` in $CONFIG"
    echo "[entrypoint] (see docker/config.example.yaml)."
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
