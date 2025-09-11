from __future__ import annotations

"""Lightweight structured logging helpers.

Provides a tiny utility to emit key=value style log lines so they are
machine-grep friendly while remaining plain text (no external deps).

Usage:
    from logging_utils import log_event
    log_event(logger, logging.INFO, "startup", version="1.0.0", features="telemetry,location")

Rules:
- Always include an 'event' name.
- Additional keyword args become key=value pairs (None values omitted).
- Strings have spaces replaced with underscores to keep single-token values.
- Numeric types are logged as-is.
- Booleans become true/false.
"""

import logging
import itertools
from typing import TypeAlias

# Permissive value type accepted for logging fields (kept broad intentionally)
# Use a broad union plus object; fall back to repr for unsupported types.
LogValue: TypeAlias = str | int | float | bool | None | object  # type: ignore[valid-type]

# Simple counter if a caller wants a transient correlation id
_counter = itertools.count(1)

def new_id() -> int:
    """Return a monotonically increasing integer (starts at 1)."""
    return next(_counter)

def _format_value(v: LogValue) -> str:  # type: ignore[valid-type]
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return str(v)
    s = str(v)
    # collapse whitespace to underscores for grep friendliness
    s = "_".join(s.split())
    return s

def kv_line(event: str, **fields: LogValue) -> str:  # type: ignore[valid-type]
    parts: list[str] = [f"event={event}"]
    for k, v in fields.items():
        if v is None:
            continue
        try:
            parts.append(f"{k}={_format_value(v)}")
        except Exception:
            # Fallback to repr if formatting fails
            parts.append(f"{k}={repr(v)}")
    return " ".join(parts)

def log_event(logger: logging.Logger, level: int, event: str, /, **fields: LogValue) -> None:  # type: ignore[valid-type]
    """Emit a structured key=value log line.

    Example:
        log_event(logger, logging.INFO, "packet_rx", src=src_id, dest=dest_id, port=portnum)
    """
    logger.log(level, kv_line(event, **fields))
