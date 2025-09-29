from __future__ import annotations

import asyncio
from gc import enable
import httpx
import json
from ai_common import (
    strip_thinking_blocks,
    tool_definitions,
    get_local_weather_from_script,
    convert_units_inplace,
)
from typing import Any, Dict, List, Optional, Tuple, cast as _cast

from logging_utils import get_logger, StructuredLogger, new_id


class OpenAIClient:
    """Minimal async client for an OpenAI-compatible Chat Completions API with tools.

    Compatible with api.openai.com and many self-hosted OpenAI-compatible servers.
    Provider-agnostic behaviors (tools catalog, weather execution, visible-thinking
    stripping, and simple unit conversion) are centralized in ai_common and reused
    here to keep behavior consistent with the Ollama client.
    """

    def __init__(
        self,
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4o-mini",
        timeout: float = 120.0,
        api_key: Optional[str] = None,
        environment_script: Optional[str] = None,
        enable_thinking_default: bool = False,
        strip_thinking_default: bool = True,
    ) -> None:
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout
        self.api_key = api_key
        self._histories: dict[str, list[dict[str, Any]]] = {}
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()
        self._environment_script = environment_script
        self._enable_thinking_default = enable_thinking_default
        self._strip_thinking_default = strip_thinking_default
        self.logger: StructuredLogger = _cast(StructuredLogger, get_logger(__name__))
        self.instance_id = new_id()
        self.logger.info(
            "openai_client_init",
            base_url=self.base_url,
            model=self.model,
            timeout=self.timeout,
            has_api_key=bool(self.api_key),
            env_script=bool(self._environment_script),
            enable_thinking_default=self._enable_thinking_default,
            strip_thinking_default=self._strip_thinking_default,
            instance=self.instance_id,
        )

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            headers: Dict[str, str] = {
                "Content-Type": "application/json",
            }
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._client = httpx.AsyncClient(timeout=self.timeout, headers=headers)
            self.logger.info(
                "openai_http_client_created",
                timeout=self.timeout,
                has_auth=bool(self.api_key),
                content_type=headers.get("Content-Type"),
                instance=self.instance_id,
            )
        return self._client

    def set_model(self, model: str) -> None:
        self.model = model
        self.logger.info("openai_model_set", model=model, instance=self.instance_id)

    def set_environment_script(self, script: Optional[str]) -> None:
        self._environment_script = script
        self.logger.info("openai_env_script_set", configured=bool(script), instance=self.instance_id)

    def reset(self, conversation_id: str = "global") -> None:
        self._histories.pop(conversation_id, None)
        self.logger.info("openai_history_reset", conversation_id=conversation_id, instance=self.instance_id)

    def reset_all(self) -> None:
        self._histories.clear()
        self.logger.info("openai_history_reset_all", instance=self.instance_id)

    def get_history(self, conversation_id: str = "global") -> list[dict[str, Any]]:
        hist = list(self._histories.get(conversation_id, []))
        self.logger.debug("openai_history_get", conversation_id=conversation_id, turns=len(hist), instance=self.instance_id)  # type: ignore[attr-defined]
        return hist

    # Tool definitions are provided by ai_common.tool_definitions()

    async def _call_tool(self, name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        if name == "get_local_weather":
            self.logger.info("openai_tool_invoke", name=name, instance=self.instance_id)
            data, summary = await get_local_weather_from_script(
                script=self._environment_script,
                timeout=15.0,
                logger=self.logger,
                instance=self.instance_id,
            )
            unit = (args or {}).get("unit", "metric")
            convert_units_inplace(data, unit)
            self.logger.info("openai_tool_result", name=name, keys=len(data.keys()), instance=self.instance_id)
            return {"tool": name, "args": args or {}, "summary": summary, "data": data}
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
        async with self._lock:
            client = await self._ensure_client()
            history = self._histories.get(conversation_id, []) if keep_history else []
            messages: List[Dict[str, Any]] = []
            if keep_history and history:
                messages.extend(history)
            if system:
                messages.insert(0, {"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})
            tools = tool_definitions() if enable_tools else None

            self.logger.info(
                "openai_chat_begin",
                conversation_id=conversation_id,
                keep_history=keep_history,
                enable_tools=bool(tools),
                enable_thinking=self._enable_thinking_default if enable_thinking is None else bool(enable_thinking),
                user_len=len(prompt or ""),
                instance=self.instance_id,
            )

            final_text: Optional[str] = None
            for _ in range(4):
                payload: Dict[str, Any] = {
                    "model": self.model,
                    "messages": messages,
                    "temperature": 0.7,
                    "enable_thinking": enable_thinking if enable_thinking is not None else self._enable_thinking_default,
                    "thinking": "enabled" if (enable_thinking if enable_thinking is not None else self._enable_thinking_default) else "disabled",
                }
                if tools:
                    payload["tools"] = tools
                # Some OpenAI-compatible servers (reasoning models) accept a reasoning hint.
                # This is best-effort and safely ignored by servers that don't support it.
                if self._enable_thinking_default if enable_thinking is None else bool(enable_thinking):
                    payload["reasoning"] = {"effort": "medium"}
                try:
                    resp = await client.post(f"{self.base_url}/chat/completions", json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                except httpx.HTTPError as e:
                    self.logger.info("openai_http_error", error=str(e), instance=self.instance_id)
                    return f"[openai_error] {e}"
                except Exception as e:
                    self.logger.info("openai_exception", error=str(e), instance=self.instance_id)
                    return f"[openai_exception] {e}"

                choice = (data.get("choices") or [{}])[0] or {}
                msg = choice.get("message", {})
                tool_calls = msg.get("tool_calls") or []
                content = msg.get("content", "")
                if not tool_calls:
                    final_text = content if isinstance(content, str) else str(content)
                    messages.append({"role": "assistant", "content": final_text})
                    self.logger.info("openai_chat_complete", chars=len(final_text or ""), instance=self.instance_id)
                    break

                messages.append({"role": "assistant", "content": content or "", "tool_calls": tool_calls})
                try:
                    tool_names = [((tc.get("function") or {}).get("name")) for tc in tool_calls]
                    self.logger.info("openai_tool_calls", count=len(tool_calls), names=",".join([n for n in tool_names if n]), instance=self.instance_id)
                except Exception:
                    pass

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
                    messages.append({"role": "tool", "content": json.dumps(result, ensure_ascii=False), "tool_call_id": tc_id})

            if final_text is None:
                final_text = "[openai_error] tool loop exceeded limit"

            do_strip = self._strip_thinking_default if strip_thinking is None else bool(strip_thinking)
            if do_strip and isinstance(final_text, str):
                final_text = strip_thinking_blocks(final_text, logger=self.logger, instance=self.instance_id)

            if keep_history:
                hist = self._histories.setdefault(conversation_id, [])
                hist.append({"role": "user", "content": prompt})
                hist.append({"role": "assistant", "content": final_text})
            return final_text

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
            self.logger.info("openai_http_client_closed", instance=self.instance_id)

