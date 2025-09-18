from __future__ import annotations

"""Lightweight structured logging helpers and logging configuration.

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
import shutil
import re
from typing import Any, Dict
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

def _format_key(k: Any) -> str:
    s = str(k)
    return "_".join(s.split())

def _flatten_fields(prefix: str, value: Any, out: Dict[str, Any]) -> None:
    """Recursively flatten nested dicts/lists into dot/indexed keys.

    Examples:
      prefix={'a': 1, 'b': {'c': 2}} -> {prefix.a:1, prefix.b.c:2}
      prefix=[10, 20] -> {prefix[0]:10, prefix[1]:20}
    """
    # Avoid treating strings/bytes as iterables
    if isinstance(value, dict):
        for k, v in value.items():
            child_key = f"{prefix}.{_format_key(k)}"
            _flatten_fields(child_key, v, out)
        return
    if isinstance(value, (list, tuple)):
        for i, v in enumerate(value):
            child_key = f"{prefix}[{i}]"
            _flatten_fields(child_key, v, out)
        return
    out[prefix] = value

def kv_line(event: str, **fields: LogValue) -> str:  # type: ignore[valid-type]
    parts: list[str] = [f"event={event}"]
    # First flatten nested structures
    flat: Dict[str, Any] = {}
    for k, v in fields.items():
        if v is None:
            continue
        key = _format_key(k)
        _flatten_fields(key, v, flat)
    # Now render flattened pairs
    for k, v in flat.items():
        try:
            parts.append(f"{k}={_format_value(v)}")
        except Exception:
            parts.append(f"{k}={repr(v)}")
    return " ".join(parts)

def log_event(logger: logging.Logger, level: int, event: str, /, **fields: LogValue) -> None:  # type: ignore[valid-type]
    """Emit a structured key=value log line.

    Example:
        log_event(logger, logging.INFO, "packet_rx", src=src_id, dest=dest_id, port=portnum)
    """
    # Use stacklevel so the caller location points to the original site
    # Stack: Logger._log <- Logger.log <- StructuredLogger.log <- log_event <- user
    logger.log(level, kv_line(event, **fields), stacklevel=4)


# --- Structured Logger with auto-kv support ---

class StructuredLogger(logging.Logger):
    """A logger that supports either classic messages or implicit kv logging.

    Usage:
      - Classic: logger.info("hello world")
      - KV-style: logger.info(event="startup", version="1.2.3", debug=True)
      - Works for .debug/.info/.warning/.error/.critical/.log
    """

    _RESERVED: tuple[str, ...] = ("exc_info", "extra", "stack_info", "stacklevel")

    def _maybe_kv(self, level: int, msg: Any | None, args: tuple[Any, ...], kwargs: Dict[str, Any]) -> bool:
        """Return True if we handled the log by emitting kv_line; else False.

        Rules:
        - If 'event' in kwargs and msg is None/empty, use that event.
        - Else if msg is a string AND there are non-reserved kwargs, treat msg as event.
        - Reserved logging kwargs (exc_info, extra, stack_info, stacklevel) are passed through.
        """
        if not isinstance(kwargs, dict):
            return False

        # Separate logging kwargs and candidate kv fields (do not mutate original dict).
        log_kwargs: Dict[str, Any] = {k: kwargs[k] for k in self._RESERVED if k in kwargs}
        fields: Dict[str, Any] = {k: v for k, v in kwargs.items() if k not in self._RESERVED and k != "event"}

        # Ensure correct caller attribution by defaulting stacklevel when we log internally.
        # user -> .info/.debug -> _maybe_kv -> _log, so stacklevel=3 points at user.
        log_kwargs.setdefault("stacklevel", 3)

        # Case 1: Explicit event in kwargs and no message (kv-first style)
        if (msg is None or msg == "") and ("event" in kwargs):
            event = kwargs.get("event")
            try:
                text = kv_line(str(event), **fields)
            except Exception:
                text = f"event={event}"
            self._log(level, text, (), **log_kwargs)
            return True

        # Case 2: Message used as event when additional kv fields are present
        if isinstance(msg, str) and fields:
            try:
                text = kv_line(msg, **fields)
            except Exception:
                text = f"event={msg}"
            self._log(level, text, (), **log_kwargs)
            return True

        return False

    # Override common level methods to accept msg=None (for kv mode)
    def debug(self, msg: Any | None = None, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        if not self.isEnabledFor(logging.DEBUG):
            return
        if self._maybe_kv(logging.DEBUG, msg, args, kwargs):
            return
        if "stacklevel" not in kwargs:
            kwargs["stacklevel"] = 2  # user -> .debug -> _log
        super().debug(msg, *args, **kwargs)

    def info(self, msg: Any | None = None, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        if not self.isEnabledFor(logging.INFO):
            return
        if self._maybe_kv(logging.INFO, msg, args, kwargs):
            return
        if "stacklevel" not in kwargs:
            kwargs["stacklevel"] = 2
        super().info(msg, *args, **kwargs)

    def warning(self, msg: Any | None = None, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        if not self.isEnabledFor(logging.WARNING):
            return
        if self._maybe_kv(logging.WARNING, msg, args, kwargs):
            return
        if "stacklevel" not in kwargs:
            kwargs["stacklevel"] = 2
        super().warning(msg, *args, **kwargs)

    def error(self, msg: Any | None = None, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        if not self.isEnabledFor(logging.ERROR):
            return
        if self._maybe_kv(logging.ERROR, msg, args, kwargs):
            return
        if "stacklevel" not in kwargs:
            kwargs["stacklevel"] = 2
        super().error(msg, *args, **kwargs)

    def critical(self, msg: Any | None = None, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        if not self.isEnabledFor(logging.CRITICAL):
            return
        if self._maybe_kv(logging.CRITICAL, msg, args, kwargs):
            return
        if "stacklevel" not in kwargs:
            kwargs["stacklevel"] = 2
        super().critical(msg, *args, **kwargs)

    def log(self, level: int, msg: Any | None = None, *args: Any, **kwargs: Any) -> None:  # type: ignore[override]
        if not self.isEnabledFor(level):
            return
        if self._maybe_kv(level, msg, args, kwargs):
            return
        # Default to pointing one frame above Logger.log (i.e., the direct caller of .log)
        if "stacklevel" not in kwargs:
            kwargs["stacklevel"] = 2
        super().log(level, msg, *args, **kwargs)


# --- Centralized logging configuration ---

class SensitiveFormatter(logging.Formatter):
    """Formatter that redacts sensitive info and adds ANSI color by level."""
    def __init__(self, fmt: str | None = None, datefmt: str | None = None):
        super().__init__(fmt, datefmt)
        self.sensitive_patterns = [
            (re.compile(r'(https://api\.telegram\.org/bot)([A-Za-z0-9:_-]{35,})(/\w+)'), r'\1[redacted]\3')
        ]

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    blue = "\x1b[34;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    magenta = "\x1b[35;20m"
    reset = "\x1b[0m"

    def format(self, record: logging.LogRecord) -> str:  # pragma: no cover - cosmetic
        # Build caller field: "filename:lineno" padded to at least 30 chars (spaces after lineno)
        caller = f"{getattr(record, 'filename', '?')}:{getattr(record, 'lineno', 0)}"
        # Do not truncate; only ensure a minimum width
        record.caller_pad = caller.ljust(30)

        message = super().format(record)
        # Transform leading 'event=...' into bracketed tag [EVENT] at the start of the log body
        # We only transform the portion after the first ' - ' separator in the format string
        try:
            prefix, sep, body = message.partition(' - ')
            if sep:  # Only if our expected separator is present
                # body is everything after the first ' - '
                m = re.match(r"^event=([^\s]+)(\s+.*)?$", body, flags=re.DOTALL)
                if m:
                    event_name = m.group(1).upper()
                    spacing = " " * 2
                    rest = (m.group(2) or "").lstrip().replace(" ", f"\n{spacing}")
                    tag = f"[{event_name}]"
                    arrow = "\u2192"  # Unicode right arrow
                    message = f"{prefix} {arrow} {tag}\n{spacing}{rest}" if rest else f"{arrow}{prefix} {arrow} {tag}"
        except Exception:
            # If anything goes wrong, fall back to the original message
            pass
        for pattern, replacement in self.sensitive_patterns:
            message = pattern.sub(replacement, message)
        # Append a horizontal line separator spanning the terminal width
        try:
            columns = shutil.get_terminal_size(fallback=(120, 24)).columns
        except Exception:
            columns = 120
        separator = "\u2500" * max(1, columns)  # '─' U+2500
        message = f"{message}\n{separator}"
        # Choose active color by level
        if record.levelno >= logging.CRITICAL:
            active = self.bold_red
        elif record.levelno == logging.ERROR:
            active = self.red
        elif record.levelno == logging.WARNING:
            active = self.yellow
        elif record.levelno == logging.INFO:
            active = self.blue
        elif record.levelno == logging.DEBUG:
            active = self.grey
        else:
            active = ""

        # Color '=' as magenta and then return to active color
        if active:
            message_colored = message.replace("=", f"{self.magenta}={active}")
            return f"{active}{message_colored}{self.reset}"
        else:
            message_colored = message.replace("=", f"{self.magenta}=\x1b[0m")
            return f"{message_colored}{self.reset}"


def _parse_log_level(level: Any) -> int:
    if isinstance(level, str):
        try:
            return getattr(logging, level.upper())
        except AttributeError:
            logging.warning(f"Invalid log level: {level}. Defaulting to INFO.")
            return logging.INFO
    if isinstance(level, int):
        return level
    logging.warning(f"Invalid log level type: {type(level)}. Defaulting to INFO.")
    return logging.INFO


def configure_logging(config: Dict[str, Any]) -> None:
    """Set up root logging handlers/formatters and per-logger levels based on config dict.

    Expects keys under 'logging':
      - level: str/int root level
      - level_telegram: str/int for 'telegram' logger
      - level_httpx: str/int for 'httpx' logger (fallback uses level_telegram when missing)
      - file_log: bool
      - file_path: str
      - use_syslog: bool
      - syslog_host: str
      - syslog_port: int
      - syslog_protocol: 'udp' | 'tcp'
    """
    logging_cfg = config.get('logging', {}) if isinstance(config, dict) else {}

    # Ensure all loggers use our StructuredLogger class
    logging.setLoggerClass(StructuredLogger)
    log_level = _parse_log_level(logging_cfg.get('level', 'INFO'))
    log_level_telegram = _parse_log_level(logging_cfg.get('level_telegram', 'INFO'))
    log_level_httpx = _parse_log_level(logging_cfg.get('level_httpx', logging_cfg.get('level_telegram', 'WARN')))

    # Include fixed-width caller field (filename:lineno padded to 30 chars)
    formatter = SensitiveFormatter('%(asctime)s %(levelname)s %(caller_pad)s - %(message)s')
    handlers: list[logging.Handler] = [logging.StreamHandler()]

    if logging_cfg.get('file_log', False):
        try:
            handlers.append(logging.FileHandler(logging_cfg.get('file_path', 'meshgram.log')))
        except Exception as e:  # pragma: no cover - filesystem dependent
            logging.error(f"Failed to open log file: {e}")

    # Handlers will be assigned the SensitiveFormatter below; avoid passing a format here that may
    # reference fields not yet attached to the record by our formatter.
    logging.basicConfig(level=log_level, handlers=handlers)

    for h in logging.root.handlers:
        h.setFormatter(formatter)

    logging.getLogger('httpx').setLevel(log_level_httpx)
    logging.getLogger('telegram').setLevel(log_level_telegram)

    if logging_cfg.get('use_syslog', False):
        try:
            from logging.handlers import SysLogHandler
            host = str(logging_cfg.get('syslog_host') or 'localhost')
            port = int(logging_cfg.get('syslog_port', 514))
            address = (host, port)
            socktype = SysLogHandler.UDP_SOCKET if str(logging_cfg.get('syslog_protocol', 'udp')).lower() == 'udp' else SysLogHandler.TCP_SOCKET
            syslog_handler = SysLogHandler(address=address, socktype=socktype)
            syslog_handler.setFormatter(SensitiveFormatter('%(caller_pad)s - %(levelname)s - %(message)s'))
            logging.getLogger().addHandler(syslog_handler)
        except Exception as e:  # pragma: no cover - environment dependent
            logging.error(f"Failed to set up syslog handler: {e}")


def get_logger(name: str) -> logging.Logger:
    """Shortcut to get a module logger by name (centralized import site)."""
    return logging.getLogger(name)
