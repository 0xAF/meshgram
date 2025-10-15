from __future__ import annotations

import httpx
import asyncio
from typing import Any, Dict, Optional, List, cast as _cast
import json
from logging_utils import get_logger, StructuredLogger, new_id
from ai_common import (
    strip_thinking_blocks,
    tool_definitions,
    get_local_weather_from_script,
    convert_units_inplace,
)

class OllamaClient:
    """Minimal async client for a local Ollama instance with per-conversation histories.

    Each conversation is keyed (e.g. by mesh shortName or Telegram user id) so that
    different users don't share context. A special key 'global' can still be used
    by callers if they want a shared context.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:11434",
        model: str = "llama3",
        timeout: float = 120.0,
        environment_script: Optional[str] = None,
        enable_thinking_default: bool = False,
        strip_thinking_default: bool = False,
    ) -> None:
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout
        self._histories: dict[str, list[dict[str, str]]] = {}
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()
        # Optional external script for environmental telemetry (used for weather)
        self._environment_script = environment_script
        # Whether to enable model "thinking"
        self._enable_thinking_default = enable_thinking_default
        # Whether to strip visible "thinking"/CoT blocks from assistant output by default
        self._strip_thinking_default = strip_thinking_default
        # Logger
        self.logger: StructuredLogger = _cast(StructuredLogger, get_logger(__name__))
        self.instance_id = new_id()
        self.logger.info(
            "ollama_client_init",
            base_url=self.base_url,
            model=self.model,
            timeout=self.timeout,
            env_script=bool(self._environment_script),
            enable_thinking_default=self._enable_thinking_default,
            strip_thinking_default=self._strip_thinking_default,
            instance=self.instance_id,
        )

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
            self.logger.debug("ollama_http_client_created", timeout=self.timeout, instance=self.instance_id)
        return self._client

    def set_model(self, model: str) -> None:
        self.model = model
        try:
            self.logger.info("ollama_model_set", model=model, instance=self.instance_id)
        except Exception:
            pass

    def set_environment_script(self, script: Optional[str]) -> None:
        """Configure the external script used to gather environment/weather data.

        The script should output lines in the form: "key: value".
        Example:
            voltage: 100
            relative_humidity: 77.0
            wind_speed: 0.5
            wind_direction: 76.0
            rainfall_24h: 166.2
            uv_lux: 23.0
            lux: 0
            temperature: 18.1
        """
        self._environment_script = script
        try:
            self.logger.debug("ollama_env_script_set", configured=bool(script), instance=self.instance_id)
        except Exception:
            pass

    def get_diagnostics(self) -> dict[str, Any]:
        """Expose basic diagnostics for debugging and UX commands.

        Returns keys:
          - provider: 'ollama'
          - base_url: str
          - model: str
          - environment_script_configured: bool
        """
        return {
            "provider": "ollama",
            "base_url": self.base_url,
            "model": self.model,
            "environment_script_configured": bool(self._environment_script),
        }

    def reset(self, conversation_id: str = "global") -> None:
        self._histories.pop(conversation_id, None)
        try:
            self.logger.info("ollama_history_reset", conversation_id=conversation_id, instance=self.instance_id)
        except Exception:
            pass

    def reset_all(self) -> None:
        self._histories.clear()
        try:
            self.logger.info("ollama_history_reset_all", instance=self.instance_id)
        except Exception:
            pass

    def get_history(self, conversation_id: str = "global") -> list[dict[str, str]]:
        hist = list(self._histories.get(conversation_id, []))
        try:
            self.logger.debug("ollama_history_get", conversation_id=conversation_id, turns=len(hist), instance=self.instance_id)  # type: ignore[attr-defined]
        except Exception:
            pass
        return hist

    async def _call_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if name == "get_local_weather":
            try:
                self.logger.info("ollama_tool_invoke", name=name, instance=self.instance_id)
            except Exception:
                pass
            data, summary = await get_local_weather_from_script(
                script=self._environment_script,
                timeout=15.0,
                logger=self.logger,
                instance=self.instance_id,
            )
            unit = (args or {}).get("unit", "metric")
            convert_units_inplace(data, unit)
            try:
                self.logger.info("ollama_tool_result", name=name, keys=len(data.keys()), instance=self.instance_id)
            except Exception:
                pass
            return {
                "tool": name,
                "args": args or {},
                "summary": summary,
                "data": data,
            }
        # Unknown tool: return an error-like payload
        return {"tool": name, "error": "unknown_tool", "args": args or {}}

    async def chat(
        self,
        prompt: str,
        *,
        system: Optional[str] = None,
        keep_history: bool = True,
        conversation_id: str = "global",
        enable_tools: bool = False,
        enable_thinking: Optional[bool] = None,
        strip_thinking: Optional[bool] = None,
    ) -> str:
        """Send a prompt to Ollama and return the response text.

        If keep_history is True, previous turns for this conversation_id are
        included. Otherwise a stateless single-turn call.
        """
        async with self._lock:
            client = await self._ensure_client()
            history = self._histories.get(conversation_id, []) if keep_history else []
            messages: List[Dict[str, str]] = []
            if keep_history and history:
                messages.extend(history)
            if system:
                messages.insert(0, {"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})
            tools = tool_definitions() if enable_tools else None
            try:
                self.logger.info(
                    "ollama_chat_begin",
                    conversation_id=conversation_id,
                    keep_history=keep_history,
                    enable_tools=bool(tools),
                    enable_thinking=self._enable_thinking_default if enable_thinking is None else bool(enable_thinking),
                    user_len=len(prompt or ""),
                    instance=self.instance_id,
                )
            except Exception:
                pass

            # Tool-call loop: allow the model to request tools up to a few rounds
            final_text: Optional[str] = None
            tooling_supported = True if tools else False
            for _ in range(4):
                payload: Dict[str, Any] = {
                    "model": self.model,
                    "messages": messages,
                    "stream": False,
                    "think": self._enable_thinking_default if enable_thinking is None else bool(enable_thinking),
                }
                if tools:
                    payload["tools"] = tools
                try:
                    self.logger.info("ollama_chat_payload", payload=payload, instance=self.instance_id)
                    resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                except httpx.HTTPStatusError as e:
                    # Gracefully fallback if server rejects tool schema (e.g., older Ollama)
                    if tools and e.response is not None and e.response.status_code == 400 and tooling_supported:
                        try:
                            error_msg = ""
                            try:
                                error_json = e.response.json()
                                error_msg = error_json.get("error", "")
                            except Exception:
                                pass
                            self.logger.warning(
                                "ollama_tools_unsupported_fallback",
                                status=e.response.status_code,
                                error=error_msg,
                                instance=self.instance_id
                            )
                        except Exception:
                            pass
                        tools = None
                        tooling_supported = False
                        # retry this round without tools
                        continue
                    return f"[ollama_error] {e}"
                except httpx.HTTPError as e:  # pragma: no cover - generic client error
                    try:
                        self.logger.warning("ollama_http_error", error=str(e), instance=self.instance_id)
                    except Exception:
                        pass
                    return f"[ollama_error] {e}"
                except Exception as e:  # pragma: no cover - defensive
                    try:
                        self.logger.warning("ollama_exception", error=str(e), instance=self.instance_id)
                    except Exception:
                        pass
                    return f"[ollama_exception] {e}"

                msg = data.get("message", {})
                tool_calls = msg.get("tool_calls") or []
                # If the assistant provided content and no tool calls, we're done
                content = msg.get("content", "")
                if not tool_calls:
                    final_text = content if isinstance(content, str) else str(content)
                    # Add assistant to the ongoing conversation
                    messages.append({"role": "assistant", "content": final_text})
                    try:
                        self.logger.info("ollama_chat_complete", chars=len(final_text or ""), instance=self.instance_id)
                    except Exception:
                        pass
                    break

                # Append assistant message that triggered tool calls to the message sequence
                messages.append({"role": "assistant", "content": content or "", "tool_calls": tool_calls})  # type: ignore[typeddict-item]
                try:
                    tool_names = [((tc.get("function") or {}).get("name")) for tc in tool_calls]
                    self.logger.info("ollama_tool_calls", count=len(tool_calls), names=",".join([n for n in tool_names if n]), instance=self.instance_id)
                except Exception:
                    pass

                # Execute each requested tool and add a tool message
                for tc in tool_calls:
                    try:
                        fn = (tc.get("function") or {})
                        name = fn.get("name")
                        raw_args = fn.get("arguments", "{}")
                        args = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
                    except Exception:
                        name = None
                        args = {}
                    if not name:
                        continue
                    result = await self._call_tool(name, args)
                    tc_id = tc.get("id")
                    messages.append({
                        "role": "tool",
                        "content": json.dumps(result, ensure_ascii=False),
                        "name": name,
                        **({"tool_call_id": tc_id} if tc_id else {}),
                    })

            if final_text is None:
                final_text = "[ollama_error] tool loop exceeded limit"
                try:
                    self.logger.warning("ollama_tool_loop_exceeded", instance=self.instance_id)
                except Exception:
                    pass

            # Optionally strip visible chain-of-thought / thinking blocks
            do_strip = self._strip_thinking_default if strip_thinking is None else bool(strip_thinking)
            if do_strip and isinstance(final_text, str):
                final_text = strip_thinking_blocks(final_text, logger=self.logger, instance=self.instance_id)

            # Update stored history if requested
            if keep_history:
                hist = self._histories.setdefault(conversation_id, [])
                # Persist only user and final assistant for compactness
                hist.append({"role": "user", "content": prompt})
                hist.append({"role": "assistant", "content": final_text})
            return final_text

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            try:
                self.logger.info("ollama_http_client_closed", instance=self.instance_id)
            except Exception:
                pass
