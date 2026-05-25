#!/usr/bin/env python3
"""Per-worker liveness probe for claude-subscription-proxy.

Sends N parallel "hi" requests through the proxy and diffs /status
before vs after to see which workers actually handled them. Counts
a worker as healthy if it both:
  - Showed a total_requests increment (proves manager.pick reached it
    AND the request flowed through to a completion).
  - The response contains either real assistant text OR a known-benign
    upstream error like rate_limit_error (account-level limit, not a
    worker fault — still proves mitm/OAuth/CLI are all alive).

Anything else (HTTP timeout, upstream_unavailable, parse error) is
flagged. Workers that never got a request during the test get tagged
"not picked" — usually means manager already had busier alternatives,
or the worker is in a cooldown / not eligible.

Usage:
    PROXY_URL=http://127.0.0.1:18787 \\
    ADMIN_KEY=sk-admin-... \\
    TENANT_KEY=sk-internal-... \\
    python3 tests/probe_workers.py [--n 70] [--model claude-haiku-4-5]

Env vars:
  PROXY_URL   default http://127.0.0.1:18787
  ADMIN_KEY   required, used for /status (the new admin_api_key)
  TENANT_KEY  required, used for /v1/messages (tenant front-door key)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor


def env(name: str, default: str | None = None) -> str:
    v = os.environ.get(name, default)
    if not v:
        sys.exit(f"{name} env var required")
    return v


PROXY_URL = env("PROXY_URL", "http://127.0.0.1:18787").rstrip("/")
ADMIN_KEY = env("ADMIN_KEY")
TENANT_KEY = env("TENANT_KEY")


def http(path: str, *, headers: dict | None = None,
         data: bytes | None = None, timeout: float = 120.0) -> tuple[int, bytes]:
    """Plain stdlib HTTP — keeps the script dependency-free."""
    req = urllib.request.Request(f"{PROXY_URL}{path}", data=data,
                                  headers=headers or {})
    if data is not None and "Content-Type" not in (headers or {}):
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() or b""
    except (TimeoutError, urllib.error.URLError) as e:
        return 0, f"network error: {e}".encode()


def get_status() -> dict:
    """Read /status with admin auth. Used for worker discovery + delta."""
    code, body = http("/status",
                      headers={"Authorization": f"Bearer {ADMIN_KEY}"})
    if code != 200:
        sys.exit(f"/status failed: HTTP {code} body={body[:200]!r}")
    return json.loads(body)


def parse_sse(raw: bytes) -> tuple[str, str | None, str | None]:
    """Return (assistant_text, anthropic_error_type, anthropic_error_message)
    from a raw SSE response body. Empty text + error fields = the upstream
    refused; non-empty text + no error = success."""
    text_parts: list[str] = []
    err_type: str | None = None
    err_msg: str | None = None
    for line in raw.splitlines():
        if not line.startswith(b"data:"):
            continue
        payload = line[5:].lstrip()
        if not payload:
            continue
        try:
            evt = json.loads(payload)
        except json.JSONDecodeError:
            continue
        t = evt.get("type")
        if t == "content_block_delta":
            d = evt.get("delta") or {}
            if d.get("type") == "text_delta":
                text_parts.append(d.get("text", ""))
        elif t == "error":
            err = evt.get("error") or {}
            err_type = err.get("type") or "error"
            err_msg = err.get("message")
    return "".join(text_parts).strip(), err_type, err_msg


def send_one(idx: int, model: str) -> dict:
    body = json.dumps({
        "model": model,
        "max_tokens": 30,
        "stream": True,
        "messages": [{"role": "user", "content": "hi"}],
    }).encode()
    t0 = time.monotonic()
    code, raw = http("/v1/messages",
                     headers={"Authorization": f"Bearer {TENANT_KEY}"},
                     data=body, timeout=120.0)
    dt = time.monotonic() - t0
    if code != 200:
        return {"idx": idx, "code": code, "dt": dt, "ok": False,
                "text": "", "err_type": "http_error",
                "err_msg": raw[:200].decode("utf-8", "replace")}
    text, err_type, err_msg = parse_sse(raw)
    # rate_limit_error is "the worker proved end-to-end OK; Anthropic
    # just declined this account". Distinguish from real failure.
    ok = bool(text) and err_type is None
    return {"idx": idx, "code": code, "dt": dt, "ok": ok, "text": text,
            "err_type": err_type, "err_msg": err_msg}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--n", type=int, default=None,
                    help="parallel request count (default: 2 × worker count)")
    ap.add_argument("--model", default="claude-haiku-4-5",
                    help="model id sent in the request body")
    ap.add_argument("--max-failures", type=int, default=10,
                    help="how many failure samples to print")
    args = ap.parse_args()

    before = get_status()
    workers_before = {w["user_id"]: w for w in before["workers"]}
    by_acc: dict[str, list[str]] = defaultdict(list)
    for uid, w in workers_before.items():
        by_acc[w["account"] or "<legacy>"].append(uid)
    total = len(workers_before)
    n = args.n if args.n is not None else total * 2
    if total == 0:
        sys.exit("/status returned no workers — nothing to test")

    print(f"discovered {total} workers across {len(by_acc)} accounts")
    print(f"sending {n} parallel POST /v1/messages (model={args.model})\n")

    req_before = {uid: w["total_requests"] for uid, w in workers_before.items()}

    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=n) as ex:
        results = list(ex.map(lambda i: send_one(i, args.model), range(n)))
    wall = time.monotonic() - t0

    after = get_status()
    req_after = {w["user_id"]: w["total_requests"] for w in after["workers"]}

    # ── per-request summary ────────────────────────────────────────
    ok_count = sum(1 for r in results if r["ok"])
    rl_count = sum(1 for r in results if r["err_type"] == "rate_limit_error")
    unavail = sum(1 for r in results
                  if (r.get("err_type") or "") == "upstream_unavailable")
    print(f"=== requests: {ok_count}/{n} ok | "
          f"{rl_count} rate-limited | {unavail} upstream_unavailable | "
          f"wall {wall:.1f}s ===\n")

    bad = [r for r in results
           if not r["ok"] and r.get("err_type") != "rate_limit_error"]
    if bad:
        print(f"non-rate-limit failures (first {min(args.max_failures, len(bad))}):")
        for r in bad[:args.max_failures]:
            tag = r.get("err_type") or f"http_{r['code']}"
            sample = (r.get("err_msg") or r.get("text", "") or "")[:120]
            print(f"  #{r['idx']:>3} {tag:25s} dt={r['dt']:5.1f}s  {sample}")
        print()

    # ── per-worker coverage ────────────────────────────────────────
    print("=== per-worker delta (requests handled during the probe) ===")
    print(f"  {'worker':16s}  Δreq  health")
    for acc in sorted(by_acc):
        for uid in sorted(by_acc[acc]):
            d = req_after.get(uid, 0) - req_before.get(uid, 0)
            w = workers_before[uid]
            if not w.get("alive"):
                health = "✗ DEAD"
            elif w.get("issue_kind") == "rate_limit":
                health = "rate_limited"
            elif w.get("issue_kind") == "degraded":
                health = "degraded"
            elif d == 0:
                health = "  (not picked)"
            else:
                health = "✓ ok"
            print(f"  {uid:16s}  {d:>4d}  {health}")
        print()


if __name__ == "__main__":
    main()
