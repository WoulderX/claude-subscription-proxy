"""Unit tests for HijackAddon._merge_body — the whitelist merge that
preserves claude's identity-bearing fields while overlaying user content."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.mitm.addon import HijackAddon


def make_addon():
    session = MagicMock()
    session.user_id = "alice"
    return HijackAddon(session)


def test_merge_preserves_claude_identity_fields():
    addon = make_addon()
    claude_body = json.dumps({
        "model": "claude-opus-4-7",
        "system": [{"type": "text", "text": "You are Claude Code, Anthropic's CLI..."}],
        "tools": [{"name": "Read"}, {"name": "Bash"}],
        "metadata": {"user_id": "anon_claude_code_user"},
        "anthropic_version": "vertex-2023-10-16",
        "messages": [{"role": "user", "content": "placeholder"}],
        "max_tokens": 1024,
    })
    user_body = {
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "hi"}],
        "max_tokens": 2048,
    }
    merged = addon._merge_body(claude_body, user_body)

    # user-owned fields win
    assert merged["model"] == "claude-sonnet-4-6"
    assert merged["messages"] == [{"role": "user", "content": "hi"}]
    assert merged["max_tokens"] == 2048

    # claude identity preserved
    assert merged["system"][0]["text"].startswith("You are Claude Code")
    assert {t["name"] for t in merged["tools"]} == {"Read", "Bash"}
    assert merged["metadata"] == {"user_id": "anon_claude_code_user"}
    assert merged["anthropic_version"] == "vertex-2023-10-16"


def test_merge_user_system_appended_after_billing_header():
    """When user supplies a system prompt, claude's billing header block
    (system[0] with cc_entrypoint=cli) is preserved and the user's system
    is appended as a new cached block. Claude's persona/instructions
    blocks (system[1..]) are dropped."""
    addon = make_addon()
    claude_body = json.dumps({
        "system": [
            {"type": "text",
             "text": "x-anthropic-billing-header: cc_version=2.1.144; cc_entrypoint=cli; cch=46e68;"},
            {"type": "text",
             "text": "You are Claude Code, Anthropic's official CLI for Claude."},
            {"type": "text",
             "text": "Long instructions...",
             "cache_control": {"type": "ephemeral", "ttl": "1h"}},
        ],
        "messages": [{"role": "user", "content": "x"}],
        "tools": [{"name": "Bash"}],
    })
    user_body = {
        "messages": [{"role": "user", "content": "hi"}],
        "system": "You are a weather assistant.",
    }
    merged = addon._merge_body(claude_body, user_body)

    # system is now [billing_block, user_block]
    assert isinstance(merged["system"], list)
    assert len(merged["system"]) == 2
    # billing header preserved verbatim
    assert "cc_entrypoint=cli" in merged["system"][0]["text"]
    # user system appended with cache_control
    assert merged["system"][1]["text"] == "You are a weather assistant."
    assert merged["system"][1].get("cache_control") == {"type": "ephemeral"}
    # claude's persona block is GONE
    persona_texts = [b.get("text", "") for b in merged["system"]]
    assert not any("You are Claude Code" in t for t in persona_texts)
    # tools still claude's default (option A keeps tools as claude's)
    assert merged["tools"] == [{"name": "Bash"}]


def test_merge_no_user_system_keeps_claude_system_intact():
    addon = make_addon()
    claude_system = [
        {"type": "text", "text": "x-anthropic-billing-header: cc_entrypoint=cli;"},
        {"type": "text", "text": "You are Claude Code, ..."},
    ]
    claude_body = json.dumps({
        "system": claude_system,
        "messages": [{"role": "user", "content": "x"}],
    })
    user_body = {"messages": [{"role": "user", "content": "hi"}]}
    merged = addon._merge_body(claude_body, user_body)
    assert merged["system"] == claude_system


def test_merge_empty_claude_body():
    addon = make_addon()
    user_body = {"model": "x", "messages": [{"role": "user", "content": "hi"}]}
    merged = addon._merge_body("", user_body)
    assert merged["model"] == "x"
    assert merged["messages"] == [{"role": "user", "content": "hi"}]


def test_merge_malformed_claude_body():
    addon = make_addon()
    user_body = {"model": "x", "messages": []}
    merged = addon._merge_body("not json {{{", user_body)
    assert merged["model"] == "x"


def test_merge_drops_model_coupled_knobs_on_model_override():
    """The claude CLI tunes output_config/thinking/context_management for
    the model IT runs (e.g. output_config={"effort":"xhigh"} is Opus-tier;
    sonnet rejects "xhigh"). When the caller overrode `model`, those CLI
    knobs must be dropped so the new model uses its own defaults."""
    addon = make_addon()
    claude_body = json.dumps({
        "model": "claude-opus-4-7",
        "output_config": {"effort": "xhigh"},
        "thinking": {"type": "adaptive"},
        "context_management": {"edits": [{"type": "clear_thinking_20251015"}]},
        "messages": [{"role": "user", "content": "x"}],
    })
    # model overridden -> knobs dropped
    merged = addon._merge_body(claude_body, {
        "model": "claude-sonnet-4-6",
        "messages": [{"role": "user", "content": "hi"}],
    })
    assert merged["model"] == "claude-sonnet-4-6"
    assert "output_config" not in merged
    assert "thinking" not in merged
    assert "context_management" not in merged


def test_merge_keeps_model_coupled_knobs_when_model_matches():
    """Same model as the CLI -> the CLI's knobs are valid, keep them."""
    addon = make_addon()
    claude_body = json.dumps({
        "model": "claude-opus-4-7",
        "output_config": {"effort": "xhigh"},
        "messages": [{"role": "user", "content": "x"}],
    })
    merged = addon._merge_body(claude_body, {
        "model": "claude-opus-4-7",
        "messages": [{"role": "user", "content": "hi"}],
    })
    assert merged["output_config"] == {"effort": "xhigh"}


def test_path_matching_handles_query_string():
    """Real claude code hits /v1/messages?beta=true. The addon must match
    on the bare path, not the path-with-query."""
    from src.mitm.addon import MESSAGES_PATH_PREFIX
    for path in ("/v1/messages",
                 "/v1/messages?beta=true",
                 "/v1/messages?beta=true&x=1"):
        bare = path.split("?", 1)[0]
        assert bare == MESSAGES_PATH_PREFIX, f"path {path!r} should match"
    for path in ("/v1/messages/count_tokens", "/api/foo", "/healthz"):
        bare = path.split("?", 1)[0]
        assert bare != MESSAGES_PATH_PREFIX, f"path {path!r} must NOT match"


if __name__ == "__main__":
    test_merge_preserves_claude_identity_fields()
    test_merge_user_system_appended_after_billing_header()
    test_merge_no_user_system_keeps_claude_system_intact()
    test_merge_empty_claude_body()
    test_merge_malformed_claude_body()
    test_merge_drops_model_coupled_knobs_on_model_override()
    test_merge_keeps_model_coupled_knobs_when_model_matches()
    test_path_matching_handles_query_string()
    print("all merge tests pass")
