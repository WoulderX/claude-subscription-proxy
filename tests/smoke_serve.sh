#!/usr/bin/env bash
# Manual smoke test: launch the API on a temp port and curl /healthz.
# Doesn't spawn real claude — it tries to, but if claude isn't on PATH or
# can't OAuth-login, the manager has no user sessions yet so /healthz still
# works. Real per-user calls require a logged-in claude in users/<id>/.claude.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=${PORT:-18787}
export CONFIG=${CONFIG:-config.example.yaml}
export LOG_LEVEL=${LOG_LEVEL:-INFO}

python3 -m src.main &
PID=$!
trap "kill $PID 2>/dev/null || true; wait 2>/dev/null || true" EXIT

# Wait up to 5 seconds for /healthz to respond.
for i in {1..50}; do
    if curl -sf "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then
        echo "ok: server up after ${i}*0.1s"
        break
    fi
    sleep 0.1
done

curl -sS "http://127.0.0.1:${PORT}/healthz"
echo
echo
echo "auth rejection:"
curl -sS -o /dev/null -w "  HTTP %{http_code}\n" \
    -X POST "http://127.0.0.1:${PORT}/v1/messages" \
    -H "Content-Type: application/json" \
    -d '{"model":"x","messages":[]}'
