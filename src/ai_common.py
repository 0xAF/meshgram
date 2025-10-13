from __future__ import annotations

import asyncio
import re
import shlex
import asyncio.subprocess as asp
import contextlib
from typing import Any, Dict, List, Optional, Tuple


def strip_thinking_blocks(text: str, *, logger: Any | None = None, instance: Any | None = None) -> str:
    """Remove visible chain-of-thought/thinking sections returned by some models.

    Strips <think>...</think>, ```thinking/```reasoning fenced blocks,
    <reasoning>...</reasoning>, and leading "Thinking:" sections up to the
    first blank line. Collapses excessive blank lines and trims.
    """
    if not text:
        return text
    s = text
    # Prepare logger (optional); prefer provided logger, else create a module logger
    if logger is None:
        try:
            from logging_utils import get_logger as _get_logger  # lazy import
            logger = _get_logger(__name__)
        except Exception:
            logger = None

    patterns: List[Tuple[str, str, int]] = [
        ("xml_think", r"<\s*think\s*>[\s\S]*?<\s*/\s*think\s*>", re.IGNORECASE),
        ("fenced_thinking", r"```\s*(thinking|reasoning)[\s\S]*?```", re.IGNORECASE),
        ("xml_reasoning", r"<\s*reasoning\s*>[\s\S]*?<\s*/\s*reasoning\s*>", re.IGNORECASE),
        ("leading_thinking", r"^(Thinking:|Reasoning:)[\s\S]*?\n\s*\n", re.IGNORECASE),
    ]

    total_removed = 0
    try:
        for kind, pat, flags in patterns:
            for m in re.finditer(pat, s, flags):
                block = m.group(0)
                total_removed += len(block)
                preview = block.strip()
                if len(preview) > 300:
                    preview = preview[:300] + "…"
                try:
                    if logger:
                        logger.info("thinking_strip_block", kind=kind, chars=len(block), preview=preview, instance=instance)
                except Exception:
                    pass
            s = re.sub(pat, "", s, flags=flags)
        if logger:
            try:
                logger.info("thinking_strip_summary", total_chars_removed=total_removed, instance=instance)
            except Exception:
                pass
    except Exception:
        # On any unexpected regex/logging issue, fall back to the simple replacements
        s = re.sub(r"<\s*think\s*>[\s\S]*?<\s*/\s*think\s*>", "", s, flags=re.IGNORECASE)
        s = re.sub(r"```\s*(thinking|reasoning)[\s\S]*?```", "", s, flags=re.IGNORECASE)
        s = re.sub(r"<\s*reasoning\s*>[\s\S]*?<\s*/\s*reasoning\s*>", "", s, flags=re.IGNORECASE)
        s = re.sub(r"^(Thinking:|Reasoning:)[\s\S]*?\n\s*\n", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def tool_definitions() -> List[Dict[str, Any]]:
    """Common tool set exposed to models via tools/function-calling API."""
    return [
        {
            "type": "function",
            "function": {
                "name": "get_local_weather",
                "description": "Get the current local weather from a sensor script (Varna, Bulgaria).",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "unit": {
                            "type": "string",
                            "enum": ["metric", "imperial"],
                            "description": "Units for display. Metric uses °C and m/s; imperial uses °F and mph.",
                        },
                    },
                    "required": [],
                },
            },
        }
    ]


def parse_kv_lines(text: str) -> Dict[str, Any]:
    """Parse simple key: value lines into a dict, coercing numbers when possible."""
    data: Dict[str, Any] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or ":" not in line:
            continue
        key, val = line.split(":", 1)
        k = key.strip()
        v = val.strip()
        try:
            if v.lower() in {"nan", "inf", "+inf", "-inf"}:
                data[k] = v
            elif "." in v or "e" in v.lower():
                data[k] = float(v)
            else:
                data[k] = int(v)
        except Exception:
            data[k] = v
    return data


def format_weather_summary(data: Dict[str, Any]) -> str:
    """Produce a compact human summary from parsed telemetry data.

    Accepts both `temperature` or `temperature_c`, and `wind_speed` or `wind_kmh`.
    """
    # Normalize a few common aliases for better robustness
    if "temperature" not in data and "temperature_c" in data:
        data["temperature"] = data.get("temperature_c")
    if "wind_speed" not in data and "wind_kmh" in data:
        try:
            data["wind_speed"] = round(float(data["wind_kmh"]) / 3.6, 2)
        except Exception:
            pass

    t = data.get("temperature")
    rh = data.get("relative_humidity")
    ws = data.get("wind_speed")
    wd = data.get("wind_direction")
    r24 = data.get("rainfall_24h")
    uv = data.get("uv_lux")
    lx = data.get("lux")
    parts: List[str] = ["Weather in Varna, Bulgaria"]
    if t is not None:
        parts.append(f"T={t}°C")
    if rh is not None:
        parts.append(f"RH={rh}%")
    if ws is not None:
        if wd is not None:
            parts.append(f"Wind={ws} m/s @ {wd}°")
        else:
            parts.append(f"Wind={ws} m/s")
    if r24 is not None:
        parts.append(f"Rain24h={r24} mm")
    if uv is not None:
        parts.append(f"UV_lux={uv}")
    if lx is not None:
        parts.append(f"Lux={lx}")
    return ", ".join(parts)


def convert_units_inplace(data: Dict[str, Any], unit: str) -> None:
    """Optionally add imperial fields based on metric values in-place."""
    if unit != "imperial":
        return
    try:
        if isinstance(data.get("temperature"), (int, float)):
            t_c = float(data["temperature"])  # type: ignore[index]
            data["temperature_f"] = round((t_c * 9 / 5) + 32, 1)
        if isinstance(data.get("wind_speed"), (int, float)):
            ws_ms = float(data["wind_speed"])  # type: ignore[index]
            # Preserve full precision for tests; callers can format for display
            data["wind_speed_mph"] = ws_ms * 2.23693629
    except Exception:
        pass


async def get_local_weather_from_script(
    *, script: Optional[str], timeout: float, logger: Any, instance: Any
) -> Tuple[Dict[str, Any], str]:
    """Run an external script and return parsed data and a summary string.

    Emits standard log events using the provided logger:
    - weather_script_not_configured
    - weather_exec_begin / weather_exec_ok / weather_exec_error / weather_exec_timeout / weather_exec_exception
    - weather_script_not_found
    """
    if not script:
        try:
            logger.info("weather_script_not_configured", instance=instance)
        except Exception:
            pass
        return {}, "[weather_error] environment script not configured"
    try:
        cmd = shlex.split(script)
        try:
            logger.info("weather_exec_begin", cmd=script, instance=instance)
        except Exception:
            pass
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asp.PIPE,
            stderr=asp.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            with contextlib.suppress(ProcessLookupError):
                proc.kill()
            try:
                logger.info("weather_exec_timeout", timeout=timeout, instance=instance)
            except Exception:
                pass
            return {}, "[weather_timeout] environment script timed out"
        rc = proc.returncode
        if rc != 0:
            err = (stderr or b"").decode(errors="replace").strip()
            try:
                logger.warning("weather_exec_error", rc=rc, error=err, instance=instance)
            except Exception:
                pass
            return {}, f"[weather_error] script failed (rc={rc}): {err}"
        out = (stdout or b"").decode(errors="replace")
        data = parse_kv_lines(out)
        summary = format_weather_summary(data)
        try:
            logger.info("weather_exec_ok", keys=len(data.keys()), instance=instance)
        except Exception:
            pass
        return data, summary
    except FileNotFoundError:
        try:
            logger.info("weather_script_not_found", instance=instance)
        except Exception:
            pass
        return {}, "[weather_error] script not found"
    except Exception as e:
        try:
            logger.warning("weather_exec_exception", error=str(e), instance=instance)
        except Exception:
            pass
        return {}, f"[weather_exception] {e}"
