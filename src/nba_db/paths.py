"""Path helpers shared across the lightweight nba_db implementation."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

try:  # pragma: no cover - optional dependency in lightweight envs
    import yaml
except ModuleNotFoundError:  # pragma: no cover - test fallback path
    yaml = None  # type: ignore[assignment]

CONFIG_FILENAME = "config.yaml"


def project_root() -> Path:
    """Return the repository root for repo-relative path resolution."""

    return Path(__file__).resolve().parents[2]


def _parse_yaml_fallback(text: str) -> dict[str, object]:
    """Very small YAML parser used when PyYAML is unavailable."""

    config: dict[str, object] = {}
    stack: list[tuple[int, dict[str, object]]] = [(-1, config)]
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip())
        line = raw_line.strip()
        if ":" not in line:
            raise ValueError(f"Invalid config line: {raw_line!r}")
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        current = stack[-1][1]
        if not value:
            nested: dict[str, object] = {}
            current[key] = nested
            stack.append((indent, nested))
            continue
        if value[0] in {'"', "'"} and value[-1] == value[0]:
            value = value[1:-1]
        current[key] = value
    return config


def load_config(config_path: Optional[Path | str] = None) -> dict[str, object]:
    """Load the repository configuration file."""

    path = Path(config_path) if config_path is not None else project_root() / CONFIG_FILENAME
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    text = path.read_text(encoding="utf-8")
    if yaml is not None:
        return yaml.safe_load(text) or {}
    return _parse_yaml_fallback(text)


def raw_data_dir(
    *,
    config: Optional[dict[str, object]] = None,
    override: Optional[Path | str] = None,
) -> Path:
    """Resolve the raw data directory, respecting overrides and config."""

    if override is not None:
        override_path = Path(override)
        return override_path if override_path.is_absolute() else project_root() / override_path

    config = config or load_config()
    raw_section = config.get("raw") if isinstance(config, dict) else None
    if not isinstance(raw_section, dict):
        raise KeyError("Configuration is missing 'raw' section")
    raw_dir_value = raw_section.get("raw_dir")
    if raw_dir_value is None:
        raise KeyError("Configuration is missing 'raw.raw_dir'")
    raw_dir_path = Path(raw_dir_value)
    return raw_dir_path if raw_dir_path.is_absolute() else project_root() / raw_dir_path


def game_log_path(
    *,
    config: Optional[dict[str, object]] = None,
    override: Optional[Path | str] = None,
) -> Path:
    """Return the canonical path to the consolidated game log."""

    return raw_data_dir(config=config, override=override) / "game.csv"


__all__ = ["CONFIG_FILENAME", "game_log_path", "load_config", "project_root", "raw_data_dir"]
