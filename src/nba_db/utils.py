"""Miscellaneous helpers for the lightweight NBA data pipeline."""
from __future__ import annotations


def get_proxies() -> list[str]:
    """Return configured HTTP proxies for stats.nba.com requests."""

    return []


__all__ = ["get_proxies"]
