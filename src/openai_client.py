from __future__ import annotations

import asyncio
import httpx
import json
from ai_common import (
    strip_thinking_blocks,
    tool_definitions,
    get_local_weather_from_script,
    convert_units_inplace,
    is_weather_like,
)
from typing import Any, Dict, List, Optional, cast as _cast

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
        use_responses_api: bool = False,
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
        self._use_responses_api = use_responses_api
        self.logger: StructuredLogger = _cast(StructuredLogger, get_logger(__name__))
        self.instance_id = new_id()
        # Cache of tool support by provider+model. True = supports tools, False = do not send tools.
        self._tool_support_cache: dict[str, bool] = {}
        self.logger.info(
            "openai_client_init",
            base_url=self.base_url,
            model=self.model,
            timeout=self.timeout,
            has_api_key=bool(self.api_key),
            env_script=bool(self._environment_script),
            enable_thinking_default=self._enable_thinking_default,
            strip_thinking_default=self._strip_thinking_default,
            use_responses_api=self._use_responses_api,
            instance=self.instance_id,
        )

    def _is_cloudflare(self) -> bool:
        try:
            return ("cloudflare.com" in (self.base_url or "").lower()) or str(self.model).startswith("@cf/")
        except Exception:
            return False

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

    def get_diagnostics(self) -> Dict[str, Any]:
        """Expose basic diagnostics for debugging and UX commands.

        Returns keys:
          - provider: 'openai' (cloudflare flagged separately)
          - is_cloudflare: bool
          - base_url: str
          - model: str
          - responses_api: bool
          - tools_supported_cached: True|False|None
          - environment_script_configured: bool
        """
        def _tools_key() -> str:
            return f"{self.base_url}|{self.model}"

        return {
            "provider": "openai",
            "is_cloudflare": self._is_cloudflare(),
            "base_url": self.base_url,
            "model": self.model,
            "responses_api": self._use_responses_api,
            "tools_supported_cached": self._tool_support_cache.get(_tools_key()),
            "environment_script_configured": bool(self._environment_script),
        }

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
            context_summary: Optional[str] = None
            if keep_history and history:
                messages.extend(history)
            if system:
                messages.insert(0, {"role": "system", "content": system})
            messages.append({"role": "user", "content": prompt})
            # Respect dynamic tool support cache (per base_url+model)
            def _tools_key() -> str:
                return f"{self.base_url}|{self.model}"

            cached_support = self._tool_support_cache.get(_tools_key())
            allow_tools = bool(enable_tools) and (True if cached_support is None else cached_support)
            tools = tool_definitions() if allow_tools else None

            # If tools are enabled in config but the current model doesn't support them,
            # proactively add minimal local context for common cases (e.g., weather) so
            # the model can still answer without structured tool calls.
            try:
                if enable_tools and not allow_tools:
                    want = (prompt or "")
                    if is_weather_like(want):
                        data, summary = await get_local_weather_from_script(
                            script=self._environment_script,
                            timeout=15.0,
                            logger=self.logger,
                            instance=self.instance_id,
                        )
                        # Attach a compact context note as system message
                        if isinstance(summary, str) and summary.strip():
                            context_summary = summary
                            messages.insert(0, {"role": "system", "content": f"Context: Local weather right now: {summary}"})
                            self.logger.info("openai_added_weather_context", instance=self.instance_id)
            except Exception:
                # Non-fatal: if weather script fails, continue without context
                pass

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
            def _to_cf_input(msgs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                """Best-effort map to Cloudflare Responses API input.

                Prefer simple string content for system/user messages (broadly accepted).
                Only emit typed segments for tool results when present.
                """
                has_tool = any((m.get("role") == "tool") for m in msgs)
                cf_msgs: List[Dict[str, Any]] = []
                for m in msgs:
                    role = m.get("role", "user")
                    content = m.get("content", "")
                    if role == "tool":
                        # Map tool result messages to Cloudflare's tool_result segment
                        tool_call_id = m.get("tool_call_id")
                        seg = {
                            "type": "tool_result",
                            "tool_call_id": tool_call_id,
                            "output": content,
                        }
                        cf_msgs.append({"role": "tool", "content": [seg]})
                    else:
                        if has_tool:
                            # When tools are involved, use typed segments
                            seg = {"type": "text", "text": str(content)}
                            cf_msgs.append({"role": role, "content": [seg]})
                        else:
                            # Simpler, widely compatible: plain string content
                            cf_msgs.append({"role": role, "content": str(content)})
                return cf_msgs

            is_cf = self._is_cloudflare()
            cf_input_mode = "auto"  # auto -> structured messages; string -> plain string prompt fallback
            for _ in range(4):
                payload: Dict[str, Any]
                if self._use_responses_api:
                    # Responses API prefers a unified 'input' field.
                    # For Cloudflare Workers AI, use typed content segments.
                    if is_cf and cf_input_mode == "string":
                        # Fallback: simple string prompt for maximum compatibility
                        try:
                            parts: list[str] = []
                            for m in messages:
                                c = m.get("content", "")
                                if isinstance(c, str) and c.strip():
                                    parts.append(c)
                            input_obj = "\n".join(parts) if parts else str(messages[-1].get("content", ""))
                        except Exception:
                            input_obj = str(messages[-1].get("content", "")) if messages else ""
                    else:
                        input_obj = _to_cf_input(messages) if is_cf else messages
                    payload = {"model": self.model, "input": input_obj, "temperature": 0.7}
                else:
                    payload = {
                        "model": self.model,
                        "messages": messages,
                        "temperature": 0.7,
                        "enable_thinking": enable_thinking if enable_thinking is not None else self._enable_thinking_default,
                        "thinking": "enabled" if (enable_thinking if enable_thinking is not None else self._enable_thinking_default) else "disabled",
                    }
                # Only attach tools when using structures that support them
                if tools and not (is_cf and cf_input_mode == "string"):
                    payload["tools"] = tools
                # Some OpenAI-compatible servers (reasoning models) accept a reasoning hint.
                # This is best-effort and safely ignored by servers that don't support it.
                if (self._enable_thinking_default if enable_thinking is None else bool(enable_thinking)) and not self._is_cloudflare():
                    payload["reasoning"] = {"effort": "medium"}
                try:
                    endpoint = "/responses" if self._use_responses_api else "/chat/completions"
                    url = f"{self.base_url}{endpoint}"
                    resp = await client.post(url, json=payload)
                    resp.raise_for_status()
                    data = resp.json()
                except httpx.HTTPError as e:
                    # Try to include response details when available to aid debugging (e.g., 400 schema errors)
                    status = None
                    text = None
                    try:
                        status = getattr(getattr(e, "response", None), "status_code", None)
                        text = getattr(getattr(e, "response", None), "text", None)
                    except Exception:
                        pass
                    # If Cloudflare complains about invalid_prompt, retry once with plain string input
                    if (
                        status == 400
                        and self._use_responses_api
                        and is_cf
                        and cf_input_mode == "auto"
                        and isinstance(text, str)
                        and "invalid_prompt" in text.lower()
                    ):
                        # Many CF models don't support tools. If error hints at tools/unknown recipient, disable for future calls.
                        try:
                            low = text.lower()
                            if ("unknown_recipient" in low) or ("tool" in low and "recipient" in low) or ("tools not supported" in low):
                                self._tool_support_cache[_tools_key()] = False
                                self.logger.info(
                                    "openai_disable_tools_for_model",
                                    model=self.model,
                                    base_url=self.base_url,
                                    reason="invalid_prompt_unknown_recipient",
                                    instance=self.instance_id,
                                )
                                # If the prompt is about weather, inject local weather context so the model can answer without tools
                                try:
                                    want = (prompt or "")
                                    if is_weather_like(want):
                                        data, summary = await get_local_weather_from_script(
                                            script=self._environment_script,
                                            timeout=15.0,
                                            logger=self.logger,
                                            instance=self.instance_id,
                                        )
                                        if isinstance(summary, str) and summary.strip():
                                            context_summary = summary
                                            messages.insert(0, {"role": "system", "content": f"Context: Local weather right now: {summary}"})
                                            self.logger.info("openai_added_weather_context", instance=self.instance_id)
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        cf_input_mode = "string"
                        self.logger.info(
                            "openai_http_retry_cf_string_input",
                            url=url,
                            instance=self.instance_id,
                        )
                        continue
                    self.logger.info(
                        "openai_http_error",
                        error=str(e),
                        status=status,
                        response=(text[:1024] if isinstance(text, str) else None),
                        url=url,
                        instance=self.instance_id,
                    )
                    # If still failing on CF and the prompt is a weather request, return a local weather summary instead of error
                    try:
                        if (
                            status == 400 and self._use_responses_api and is_cf and isinstance(text, str) and "invalid_prompt" in text.lower()
                        ):
                            want = (prompt or "")
                            if is_weather_like(want):
                                if not (isinstance(context_summary, str) and context_summary.strip()):
                                    _data, _summary = await get_local_weather_from_script(
                                        script=self._environment_script,
                                        timeout=15.0,
                                        logger=self.logger,
                                        instance=self.instance_id,
                                    )
                                    if isinstance(_summary, str) and _summary.strip():
                                        return _summary
                                else:
                                    return context_summary  # type: ignore[return-value]
                    except Exception:
                        pass
                    details = f" {status}" if status else ""
                    return f"[openai_error]{details} {e}"
                except Exception as e:
                    self.logger.info("openai_exception", error=str(e), instance=self.instance_id)
                    return f"[openai_exception] {e}"
                # Parse response across both endpoints
                tool_calls: List[Dict[str, Any]] = []
                content = ""
                if self._use_responses_api:
                    # Try common fields first
                    content = _cast(str, data.get("output_text") or data.get("response") or "")
                    # Attempt to find tool_calls-like structures or nested text in Cloudflare 'output'
                    out = data.get("output")
                    if isinstance(out, list):
                        texts: List[str] = []

                        def _add_tool(seg: Dict[str, Any]) -> None:
                            try:
                                fn = seg.get("function", {}) if isinstance(seg.get("function"), dict) else {}
                                name = fn.get("name") or seg.get("name")
                                # Cloudflare often uses 'parameters' for tool args; also support 'arguments'
                                arguments = (
                                    seg.get("parameters")
                                    or seg.get("arguments")
                                    or (fn.get("arguments") if isinstance(fn, dict) else {})
                                    or {}
                                )
                                tool_calls.append({
                                    "id": seg.get("id") or "tool",
                                    "type": "function",
                                    "function": {
                                        "name": name,
                                        "arguments": json.dumps(arguments) if isinstance(arguments, (dict, list)) else (arguments or "{}"),
                                    },
                                })
                            except Exception:
                                # Best-effort; ignore malformed tool segment
                                pass

                        for item in out:
                            if not isinstance(item, dict):
                                continue
                            t = item.get("type")
                            if t in ("tool_use", "tool_call"):
                                _add_tool(item)
                                continue
                            if t in ("output_text", "text"):
                                txt = item.get("text")
                                if isinstance(txt, str):
                                    texts.append(txt)
                                continue
                            if t == "message":
                                role = item.get("role")
                                if role == "assistant":
                                    content_list = item.get("content")
                                    if isinstance(content_list, list):
                                        for seg in content_list:
                                            if not isinstance(seg, dict):
                                                continue
                                            st = seg.get("type")
                                            if st in ("output_text", "text"):
                                                txt = seg.get("text")
                                                if isinstance(txt, str):
                                                    texts.append(txt)
                                            elif st in ("tool_use", "tool_call"):
                                                _add_tool(seg)
                        if not content and texts:
                            content = "".join(texts)

                    # Fallback to OpenAI-like shape inside 'choices'
                    if not content:
                        choice = (data.get("choices") or [{}])[0] or {}
                        msg = choice.get("message", {})
                        content = _cast(str, msg.get("content", ""))
                        tool_calls = _cast(List[Dict[str, Any]], msg.get("tool_calls") or [])
                else:
                    choice = (data.get("choices") or [{}])[0] or {}
                    msg = choice.get("message", {})
                    tool_calls = msg.get("tool_calls") or []
                    content = msg.get("content", "")

                # Update tool support cache based on actual usage signal
                try:
                    if tool_calls:
                        self._tool_support_cache[_tools_key()] = True
                    elif cached_support is None and is_cf and allow_tools:
                        # If CF with tools allowed but no tool_calls ever returned across responses,
                        # don't change cache here; only set False on explicit errors above.
                        pass
                except Exception:
                    pass
                if not tool_calls:
                    final_text = content if isinstance(content, str) else str(content)
                    # If model produced no text but we have a prepared context summary (e.g., weather), use it
                    if (not isinstance(final_text, str) or not final_text.strip()) and isinstance(context_summary, str) and context_summary.strip():
                        final_text = context_summary
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
                _orig_final = final_text
                final_text = strip_thinking_blocks(final_text, logger=self.logger, instance=self.instance_id)
                # Avoid returning an empty string; if stripping removed everything, fall back to original
                if isinstance(final_text, str) and not final_text.strip() and isinstance(_orig_final, str) and _orig_final.strip():
                    final_text = _orig_final

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

