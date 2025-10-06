"""Simple logging helpers mirroring the real ``nba_db`` package."""
from __future__ import annotations

import logging
from typing import Literal


LoggerType = Literal["console"]


def init_logger(logger_type: LoggerType = "console") -> None:
    """Initialise logging to emulate the upstream helper."""

    if logger_type != "console":  # pragma: no cover - defensive branch
        raise ValueError(f"Unsupported logger type: {logger_type}")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )


__all__ = ["init_logger"]
