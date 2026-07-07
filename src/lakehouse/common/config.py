"""Layered configuration loader.

Precedence (lowest -> highest):
  1. conf/base.yaml
  2. conf/<env>.yaml            (env taken from LAKEHOUSE_ENV, default 'dev')
  3. process environment vars   (via ${VAR:default} interpolation)
"""

from __future__ import annotations

import os
import re
from functools import cache
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Z0-9_]+)(?::(?P<default>[^}]*))?\}")


def _interpolate(value: Any) -> Any:
    """Resolve ${VAR:default} placeholders recursively."""
    if isinstance(value, str):

        def repl(match: re.Match[str]) -> str:
            return os.environ.get(match.group("name"), match.group("default") or "")

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def _deep_merge(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _conf_dir() -> Path:
    override = os.environ.get("LAKEHOUSE_CONF_DIR")
    if override:
        return Path(override)
    # repo layout: <root>/conf ; installed wheel: cwd/conf
    for candidate in (Path(__file__).resolve().parents[3] / "conf", Path.cwd() / "conf"):
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("Could not locate conf/ directory; set LAKEHOUSE_CONF_DIR.")


class Config(dict):
    """dict with attribute-style *read* access for ergonomic call sites."""

    def __getattr__(self, item: str) -> Any:
        try:
            value = self[item]
        except KeyError as exc:  # pragma: no cover - defensive
            raise AttributeError(item) from exc
        return Config(value) if isinstance(value, dict) else value


@cache
def load_config(env: str | None = None) -> Config:
    env = env or os.environ.get("LAKEHOUSE_ENV", "dev")
    conf_dir = _conf_dir()
    with open(conf_dir / "base.yaml", encoding="utf-8") as fh:
        merged: dict[str, Any] = yaml.safe_load(fh)
    env_file = conf_dir / f"{env}.yaml"
    if env_file.exists():
        with open(env_file, encoding="utf-8") as fh:
            merged = _deep_merge(merged, yaml.safe_load(fh) or {})
    merged["environment"] = merged.get("environment", env)
    return Config(_interpolate(merged))
