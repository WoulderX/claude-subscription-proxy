"""Unit tests for the multi-account config schema and account ↔ user_id
resolution. These are the only invariants in src/config.py that have
behavior beyond "load YAML, return Pydantic model" — worth pinning
because the runtime _seed_home and OAuthRefresher both depend on
account_for_user() returning the right thing."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import Config


def _yaml_to_config(tmp_path: Path, body: str) -> Config:
    p = tmp_path / "config.yaml"
    p.write_text(body)
    return Config.load(p)


def test_accounts_with_api_key_auto_expands_users(tmp_path):
    cfg = _yaml_to_config(tmp_path, """\
accounts:
  claude-1:
    dir: /data/shared-auth/claude-1
    workers: 3
  claude-2:
    dir: /data/shared-auth/claude-2
    workers: 2
api_key: sk-test
""")
    assert "sk-test" in cfg.users
    assert sorted(cfg.users["sk-test"]) == [
        "claude-1-0", "claude-1-1", "claude-1-2",
        "claude-2-0", "claude-2-1",
    ]


def test_account_for_user_resolves_correctly(tmp_path):
    cfg = _yaml_to_config(tmp_path, """\
accounts:
  claude-1:
    dir: /data/shared-auth/claude-1
    workers: 2
  team-a-prod:
    dir: /data/shared-auth/team-a-prod
    workers: 1
api_key: sk-test
""")
    # Simple name
    acc = cfg.account_for_user("claude-1-0")
    assert acc is not None and acc.dir == "/data/shared-auth/claude-1"
    # Account name containing hyphens must not be split on first '-'
    acc = cfg.account_for_user("team-a-prod-0")
    assert acc is not None and acc.dir == "/data/shared-auth/team-a-prod"
    # Unknown user_id
    assert cfg.account_for_user("claude-1-99") is None
    assert cfg.account_for_user("nope-0") is None
    # Trailing non-digit shouldn't match
    assert cfg.account_for_user("claude-1-x") is None


def test_explicit_users_must_match_generated_ids(tmp_path):
    # OK: every user_id in users[] matches an auto-generated one
    _yaml_to_config(tmp_path, """\
accounts:
  claude-1: { dir: /data/shared-auth/claude-1, workers: 2 }
users:
  sk-a: [claude-1-0, claude-1-1]
""")
    # Bad: a user_id with no matching account
    with pytest.raises(Exception):
        _yaml_to_config(tmp_path, """\
accounts:
  claude-1: { dir: /data/shared-auth/claude-1, workers: 2 }
users:
  sk-a: [claude-1-0, claude-2-0]
""")
    # Bad: out-of-range index
    with pytest.raises(Exception):
        _yaml_to_config(tmp_path, """\
accounts:
  claude-1: { dir: /data/shared-auth/claude-1, workers: 2 }
users:
  sk-a: [claude-1-99]
""")


def test_legacy_users_only_works(tmp_path):
    cfg = _yaml_to_config(tmp_path, """\
users:
  sk-internal-abc: alice
  sk-internal-def: [b-0, b-1, b-2]
""")
    assert cfg.users["sk-internal-abc"] == ["alice"]
    assert cfg.users["sk-internal-def"] == ["b-0", "b-1", "b-2"]
    # Legacy mode: account_for_user always returns None
    assert cfg.account_for_user("alice") is None
    assert cfg.account_for_user("b-0") is None
    # credentials_paths falls back to single operator path
    paths = cfg.credentials_paths()
    assert len(paths) == 1
    assert paths[0].name == ".credentials.json"


def test_credentials_paths_multi_account(tmp_path):
    cfg = _yaml_to_config(tmp_path, """\
accounts:
  claude-1: { dir: /data/shared-auth/claude-1, workers: 2 }
  claude-2: { dir: /data/shared-auth/claude-2, workers: 2 }
api_key: sk-test
""")
    paths = cfg.credentials_paths()
    assert len(paths) == 2
    # Sorted by account name for deterministic refresh order
    assert paths[0] == Path("/data/shared-auth/claude-1/.credentials.json")
    assert paths[1] == Path("/data/shared-auth/claude-2/.credentials.json")


def test_empty_config_rejected(tmp_path):
    # Neither accounts+api_key nor users → useless deployment
    with pytest.raises(Exception):
        _yaml_to_config(tmp_path, "listen_port: 8787\n")


def test_explicit_users_without_api_key(tmp_path):
    # accounts: + users: (no api_key:) — finer-grained routing scenario
    cfg = _yaml_to_config(tmp_path, """\
accounts:
  claude-1: { dir: /data/shared-auth/claude-1, workers: 2 }
  claude-2: { dir: /data/shared-auth/claude-2, workers: 2 }
users:
  sk-team-a: [claude-1-0, claude-1-1]
  sk-team-b: [claude-2-0, claude-2-1]
""")
    assert cfg.users["sk-team-a"] == ["claude-1-0", "claude-1-1"]
    assert cfg.users["sk-team-b"] == ["claude-2-0", "claude-2-1"]
