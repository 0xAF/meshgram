from __future__ import annotations

import httpx
import asyncio
from typing import Any, Dict, Optional, List

class OllamaClient:
    """Minimal async client for a local Ollama instance with per-conversation histories.

    Each conversation is keyed (e.g. by mesh shortName or Telegram user id) so that
    different users don't share context. A special key 'global' can still be used
    by callers if they want a shared context.
    """

    def __init__(self, base_url: str = "http://127.0.0.1:11434", model: str = "llama3", timeout: float = 120.0) -> None:
        self.base_url = base_url.rstrip('/')
        self.model = model
        self.timeout = timeout
        self._histories: dict[str, list[dict[str, str]]] = {}
        self._client: httpx.AsyncClient | None = None
        self._lock = asyncio.Lock()

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        return self._client

    def set_model(self, model: str) -> None:
        self.model = model

    def reset(self, conversation_id: str = "global") -> None:
        self._histories.pop(conversation_id, None)

    def reset_all(self) -> None:
        self._histories.clear()

    def get_history(self, conversation_id: str = "global") -> list[dict[str, str]]:
        return list(self._histories.get(conversation_id, []))

    async def chat(
        self,
        prompt: str,
        *,
        system: Optional[str] = None,
        keep_history: bool = True,
        conversation_id: str = "global",
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

            payload: Dict[str, Any] = {
                "model": self.model,
                "messages": messages,
                "stream": False,
            }
            try:
                resp = await client.post(f"{self.base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
                assistant = data.get('message', {}).get('content', '')
                if not isinstance(assistant, str):
                    assistant = str(assistant)
                if keep_history:
                    hist = self._histories.setdefault(conversation_id, [])
                    hist.append({"role": "user", "content": prompt})
                    hist.append({"role": "assistant", "content": assistant})
                return assistant
            except httpx.HTTPError as e:
                return f"[ollama_error] {e}"
            except Exception as e:  # pragma: no cover - defensive
                return f"[ollama_exception] {e}"

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None
