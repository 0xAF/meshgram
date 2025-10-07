from __future__ import annotations

# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportAny=false

import asyncio
import signal
from importlib import import_module
import os
from typing import TypedDict, Literal, Protocol, NotRequired, cast, Any, Dict
import sqlite3
from datetime import datetime, timezone, timedelta
from telegram import Update
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown
from meshtastic_interface import MeshtasticInterface
from telegram_interface import TelegramInterface
from config_manager import ConfigManager
from logging_utils import get_logger, StructuredLogger
from node_manager import NodeManager
import re
from logging_utils import new_id
from bbs_store import BbsStore

# --- Type Definitions ---

class CommandHandler(Protocol):
    async def __call__(self, args: list[str], user_id: int, update: Update) -> None: ...

class MeshtasticPacket(TypedDict):
    fromId: str
    toId: str
    decoded: dict[str, object]
    id: str

class TelegramMessage(TypedDict):
    type: Literal['command', 'telegram', 'location', 'reaction']
    text: NotRequired[str]
    sender: NotRequired[str]
    message_id: NotRequired[int]
    thread_id: NotRequired[int]
    user_id: NotRequired[int]
    command: NotRequired[str]
    args: NotRequired[list[str]]
    update: NotRequired[Update]
    location: NotRequired[dict[str, float]]
    emoji: NotRequired[str]
    original_message_id: NotRequired[int]

class PendingAck(TypedDict):
    telegram_message_id: int
    telegram_thread_id: int
    timestamp: datetime
    bridge_id: NotRequired[int]

class Reports(TypedDict):
    telemetry: bool
    location: bool
    nodes: bool

class MeshCommands(TypedDict):
    ping: bool
    help: bool
    ai: bool

# --- Main Processor ---

class MessageProcessor:
    """Bridge logic between Meshtastic and Telegram.

    Runs background tasks to:
        * Consume Meshtastic packets and dispatch to app handlers
        * Consume Telegram messages (commands, text, location, reactions)
        * Track pending ACKs and expire them
    Also exposes a set of /cmd_ handlers used by the Telegram bot.
    """

    def __init__(self, meshtastic: MeshtasticInterface, telegram: TelegramInterface, config: ConfigManager) -> None:
        self.config: ConfigManager = config
        from typing import cast as _cast
        self.logger: StructuredLogger = _cast(StructuredLogger, get_logger(__name__))
        self.instance_id = new_id()
        self.meshtastic: MeshtasticInterface = meshtastic
        self.telegram: TelegramInterface = telegram
        self.node_manager: NodeManager = meshtastic.node_manager
        self.start_time: datetime = datetime.now(timezone.utc)
        self.is_closing: bool = False
        self.processing_tasks: list[asyncio.Task[object]] = []  # broader suppression of "Any"
        self.message_id_map: dict[int, str] = {}
        self.reverse_message_id_map: dict[str, int] = {}
        self.pending_acks: dict[int, PendingAck] = {}
        self.ack_timeout: int = 60  # seconds
        self.reports: Reports = {
            'telemetry': config.get('reports.telemetry', True),
            'location': config.get('reports.location', True),
            'nodes': config.get('reports.nodes', True),
        }
        self.mesh_commands: MeshCommands = {
            'ping': config.get('meshtastic.commands.ping', False),
            'help': config.get('meshtastic.commands.help', False),
            'ai': config.get('meshtastic.commands.ai', False),
        }
        self.forwarding_enabled: bool = config.get('telegram.enable_message_forwarding', False)

        # AI feature flags (aim / ait) + config
        self.ai_enabled_mesh: bool = config.get('meshtastic.ai_enabled', config.get('meshtastic.ai_mesh_enabled', False))
        self.ai_enabled_telegram: bool = config.get('telegram.ai_enabled', config.get('telegram.ai_telegram_enabled', False))
        # Provider-specific AI config only; global ai.base_url/ai.model removed
        self.ai_system_prompt: str = config.get('ai.system_prompt', 'You are a helpful assistant for a Meshtastic ↔ Telegram bridge.')
        self._ai_client: Any | None = None

        # Message persistence DB (direct sqlite3)
        self._msgdb: sqlite3.Connection | None = None
        try:
            # Ensure data directory exists and open DB under ./data
            os.makedirs("data", exist_ok=True)
            self._msgdb = sqlite3.connect(os.path.join("data", "messages.db"))
            # Light tuning for reliability/perf; safe defaults
            try:
                self._msgdb.execute("PRAGMA journal_mode=WAL")
                self._msgdb.execute("PRAGMA synchronous=NORMAL")
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"Failed to open data/messages.db: {e}", exc_info=True)
            self._msgdb = None

        # BBS store (private messages)
        try:
            self.bbs = BbsStore()
        except Exception as e:
            self.logger.error(f"Failed to init BbsStore: {e}")
            self.bbs = None  # type: ignore[assignment]

    # No pre-created tables; created lazily on first insert


    # --- Main Message Loops ---

    async def process_messages(self) -> None:
        """Launch core processing loops and wait until completion/cancellation."""
        self.logger.info("processor_start", instance=self.instance_id)
        self.processing_tasks = [
            asyncio.create_task(self.process_meshtastic_messages()),
            asyncio.create_task(self.process_telegram_messages()),
            asyncio.create_task(self.process_pending_acks()),
        ]
        try:
            await asyncio.gather(*self.processing_tasks)
        except asyncio.CancelledError:
            pass  # Cancellation expected during shutdown
        finally:
            await self.close()

    async def process_meshtastic_messages(self) -> None:
        """Continuously read Meshtastic queue and route packets to handlers."""
        while not self.is_closing:
            try:
                # Dynamic packet from queue (library provides dict); keep loose typing
                message = await self.meshtastic.message_queue.get()  # type: ignore[assignment]
                # Defaults for enrichment fields in logs to avoid UnboundLocalError
                from_short_name = None
                from_long_name = None
                to_short_name = None
                to_long_name = None
                # Suppress noisy health-check ringtone responses
                _is_ringtone = (
                    message.get('decoded', {}).get('portnum') == 'ADMIN_APP' and
                    'getRingtoneResponse' in message.get('decoded', {}).get('admin', {})
                )
                if not _is_ringtone:
                    # Include cached shortName and longName if available
                    from_id = message.get('fromId')
                    to_id = message.get('toId')
                    try:
                        if isinstance(from_id, str):
                            node = self.node_manager.nodes.get(from_id)
                            if node:
                                sn = node.get('shortName')
                                ln = node.get('longName')
                                if isinstance(sn, str) and sn.strip().lower() != "unknown":
                                    from_short_name = sn.strip()
                                if isinstance(ln, str) and ln.strip().lower() != "unknown":
                                    from_long_name = ln.strip()
                        if isinstance(to_id, str):
                            node = self.node_manager.nodes.get(to_id)
                            if node:
                                sn = node.get('shortName')
                                ln = node.get('longName')
                                if isinstance(sn, str) and sn.strip().lower() != "unknown":
                                    to_short_name = sn.strip()
                                if isinstance(ln, str) and ln.strip().lower() != "unknown":
                                    to_long_name = ln.strip()
                    except Exception:
                        pass

                    # If sender/recipient names are unknown, request node info (once per node), skipping our own node
                    try:
                        local_id = getattr(self.meshtastic, 'my_node_id', '')
                        if isinstance(from_id, str) and from_id and from_id != local_id and from_id.startswith('!'):
                            if not from_short_name or from_short_name.lower() == 'unknown':
                                _req_key = f"_nodeinfo_req_{from_id}"
                                if not getattr(self, _req_key, False):
                                    setattr(self, _req_key, True)
                                    self.logger.info("requesting_nodeinfo", node_id=from_id, request_key=_req_key)
                                    _ = asyncio.create_task(self.meshtastic.request_nodeinfo(from_id))
                        if isinstance(to_id, str) and to_id and to_id != local_id and to_id.startswith('!'):
                            if not to_short_name or to_short_name.lower() == 'unknown':
                                _req_key = f"_nodeinfo_req_{to_id}"
                                if not getattr(self, _req_key, False):
                                    setattr(self, _req_key, True)
                                    self.logger.info("requesting_nodeinfo", node_id=to_id, request_key=_req_key)
                                    _ = asyncio.create_task(self.meshtastic.request_nodeinfo(to_id))
                    except Exception:
                        pass
                    
                    self.logger.info(
                        "mt_packet_rx", portnum=message.get('decoded', {}).get('portnum'), raw_type=message.get('type'),
                        from_id=from_id, from_sn=from_short_name, from_ln=from_long_name,
                        to_id=to_id, to_sn=to_short_name, to_ln=to_long_name,
                        message_id=message.get('id')
                    )
                # dynamic packet dict access
                self.logger.debug(
                    "mt_message_rx",
                    instance=self.instance_id,
                    portnum=message.get('decoded', {}).get('portnum'),
                    from_id=message.get('fromId'),
                    from_sn=from_short_name,
                    from_ln=from_long_name,
                )  # type: ignore[arg-type]
                match message.get('type'):
                    case 'ack':
                        _ = await self.handle_ack(message)
                    case _:
                        _ = await self.handle_meshtastic_message(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error processing Meshtastic message: {e=}", exc_info=True)
            await asyncio.sleep(0.1)

    async def process_telegram_messages(self) -> None:
        """Continuously read Telegram queue and route to specific Telegram handlers."""
        while not self.is_closing:
            try:
                message = await self.telegram.message_queue.get()  # type: ignore[assignment]
                self.logger.info("tg_message_rx", instance=self.instance_id, type=message.get('type'), user_id=message.get('user_id'))  # type: ignore[arg-type]
                await self.handle_telegram_message(cast(TelegramMessage, message))  # type: ignore[arg-type]
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error processing Telegram message: {e=}", exc_info=True)
            await asyncio.sleep(0.1)

    # --- BBS helpers ---

    def _is_private_dm_to_bot(self, packet: dict[str, object]) -> bool:
        """Return True when the incoming mesh text is a DM to the bot node."""
        try:
            to_id = packet.get('toId')
            my_id = getattr(self.meshtastic, 'my_node_id', None)
            return isinstance(to_id, str) and isinstance(my_id, str) and to_id == my_id
        except Exception:
            return False

    async def _bbs_reply(self, sender: str, text: str) -> None:
        try:
            # Use chunked sending so longer replies are handled gracefully
            await self.meshtastic.send_message(text, sender, channel=0)
        except Exception as e:
            self.logger.error(f"bbs_reply_error: {e}")

    # --- BBS formatting helpers ---
    def _bbs_preview(self, text: str, max_chars: int = 80, max_bytes: int = 160) -> str:
        """Return a short preview of text, limited by chars and bytes, with ellipsis if trimmed."""
        if not text:
            return ""
        s = text.strip().replace('\n', ' ')
        trimmed = s[:max_chars] if len(s) > max_chars else s
        b = trimmed.encode('utf-8')
        if len(b) > max_bytes:
            b = b[:max_bytes]
            # avoid splitting in the middle of UTF-8 code point
            while (b and (b[-1] & 0xC0) == 0x80):
                b = b[:-1]
            trimmed = b.decode('utf-8', errors='ignore')
        if len(trimmed) < len(s):
            return trimmed + "…"
        return trimmed

    def _bbs_fmt_from(self, row) -> str:
        sn = row.sender_short or (row.sender_node_id or "")
        ln = row.sender_long or ""
        nid = row.sender_node_id or ""
        out = f"From {sn}"
        if ln:
            out += f" ({ln})"
        if nid:
            out += f" [{nid}]"
        return out

    def _bbs_fmt_to(self, row) -> str:
        sn = row.recipient_short
        ln = row.recipient_long or ""
        nid = row.recipient_node_id or ""
    # If first-seen (no concrete recipient yet), show shortname and list candidate node IDs instead of long name
        if not nid:
            try:
                if row.candidates_json:
                    import json
                    cands = json.loads(row.candidates_json)
                    if isinstance(cands, list) and cands:
                        # Preferred shortname from first candidate if missing
                        if not sn:
                            c0 = cands[0]
                            cs = c0.get('short') if isinstance(c0, dict) else None
                            if isinstance(cs, str) and cs.strip():
                                sn = cs.strip()
                        # Build compact list of candidate IDs
                        ids = []
                        for c in cands:
                            if isinstance(c, dict):
                                cid = c.get('node_id')
                                if isinstance(cid, str) and cid.strip():
                                    ids.append(cid.strip())
                        if ids:
                            ln = ", ".join(ids)
            except Exception:
                pass
        if not sn:
            sn = "first-seen"
        out = f"To {sn}"
        if ln:
            out += f" ({ln})"
        if nid:
            out += f" [{nid}]"
        else:
            out += " [first-seen]"
        return out

    def _bbs_help_general(self) -> str:
        # Keep under ~200 bytes
        try:
            bbs_on = bool(self.config.get('bbs.enabled', True))
        except Exception:
            bbs_on = True
        if bbs_on:
            return (
                "BBS commands (for BOT commands send /help):\n"
                "!hm – Message help\n"
            )
        return f"BBS is disabled. For BOT commands send /help."

    def _bbs_help_pm(self) -> str:
        # Compact PM help with brief descriptions (<=200 bytes)
        return (
            "BBS Message commands:\n"
            "!ms shortName msg – send msg\n"
            "!mi – list inbox\n"
            "!mr N – read\n"
            "!mdi N – delete inbox\n"
            "!mo [-a] – list outbox\n"
            "!mro N – read outbox\n"
            "!mdo N – delete outbox\n"
            "!moa – delete outbox read"
        )

    def _bbs_compose_list(self, header: str, lines: list[str], limit: int = 200) -> str:
        """Compose a newline-joined list that fits within the byte limit.

        Adds a trailing "... +N more" indicator if not all lines fit.
        """
        try:
            out_parts: list[str] = [header]
            used = len(header.encode('utf-8'))
            remaining_lines = len(lines)
            for i, line in enumerate(lines):
                # Predict bytes if we add this line
                candidate = "\n" + line
                cand_b = len(candidate.encode('utf-8'))
                if used + cand_b <= limit:
                    out_parts.append(candidate)
                    used += cand_b
                    remaining_lines -= 1
                    continue
                # Can't fit this line; add suffix indicator if possible
                suffix = f"\n... +{remaining_lines} more"
                if used + len(suffix.encode('utf-8')) <= limit:
                    out_parts.append(suffix)
                    used += len(suffix.encode('utf-8'))
                # If even suffix doesn't fit, ensure header alone fits (fallback)
                break
            result = "".join(out_parts)
            # Final hard trim in case of edge-case overrun
            b = result.encode('utf-8')
            if len(b) > limit:
                # Trim bytes conservatively at codepoint boundaries
                trimmed = b[:limit]
                while (trimmed and (trimmed[-1] & 0xC0) == 0x80):
                    trimmed = trimmed[:-1]
                result = trimmed.decode('utf-8', errors='ignore')
            return result
        except Exception:
            # Fallback to header only if anything goes wrong
            hb = header.encode('utf-8')
            if len(hb) <= limit:
                return header
            return hb[:limit].decode('utf-8', errors='ignore')

    # --- Extend text handler for BBS '!' commands in DMs ---

    async def process_pending_acks(self) -> None:
        """Periodically remove expired pending ACK entries."""
        while True:
            now = datetime.now(timezone.utc)
            for message_id, data in list(self.pending_acks.items()):
                if (now - data['timestamp']).total_seconds() > self.ack_timeout:
                    bridge_id = data.get('bridge_id')
                    # Structured timeout event
                    self.logger.warning("ack_timeout", instance=self.instance_id, message_id=message_id, bridge_id=bridge_id)
                    del self.pending_acks[message_id]
            await asyncio.sleep(10)

    async def remove_pending_ack(self, message_id: int, delay: int | None = None) -> None:
        """Remove a pending ACK entry after a delay (defaults to ack_timeout).

        This is a safety net: we already have a periodic sweeper, but this
        targeted task lets us clean up even if the sweeper interval changes or
        the object is shutting down soon.
        """
        try:
            await asyncio.sleep(delay if delay is not None else self.ack_timeout)
            if message_id in self.pending_acks:
                data = self.pending_acks.pop(message_id)
                self.logger.debug("ack_pruned", instance=self.instance_id, message_id=message_id, bridge_id=data.get('bridge_id'))
        except asyncio.CancelledError:  # pragma: no cover - shutdown path
            pass
        except Exception as e:  # pragma: no cover - defensive
            self.logger.debug(f"remove_pending_ack error for id={message_id}: {e}")

    async def close(self) -> None:
        """Idempotently stop processing, cancel tasks, and emit structured stop events."""
        if getattr(self, "_already_closed", False):
            self.logger.info("MessageProcessor is already closed; skipping.")
            return
        if self.is_closing:
            # Another caller is already shutting down; wait for tasks to finish.
            self.logger.info("MessageProcessor close already in progress; awaiting existing shutdown.")
            # Give tasks a brief chance to finish gracefully.
            await asyncio.sleep(0)
            return
        self.is_closing = True
        self.logger.info("processor_stop_begin", instance=self.instance_id)

        # Cancel any still-running tasks spawned by process_messages (defensive).
        for t in self.processing_tasks:
            try:
                if not t.done():
                    t.cancel()
            except Exception:
                pass
        if self.processing_tasks:
            await asyncio.gather(*self.processing_tasks, return_exceptions=True)
        self.processing_tasks.clear()
        # Close message DB
        try:
            if self._msgdb is not None:
                self._msgdb.close()
        except Exception:
            pass
        self.is_closing = False
        self._already_closed = True  # type: ignore[attr-defined]
        self.logger.info("processor_stop_complete", instance=self.instance_id)

    # --- BBS command router ---

    async def _handle_bbs_command(self, raw: str, sender: str) -> None:
        # Global enable switch
        try:
            if not bool(self.config.get('bbs.enabled', True)):
                return
        except Exception:
            pass
        parts = raw.split()
        cmd = parts[0].lower()
        args = parts[1:]
        match cmd:
            case '!h':
                await self._bbs_reply(sender, self._bbs_help_general())
            case '!hm':
                await self._bbs_reply(sender, self._bbs_help_pm())
            case '!mi':
                await self._bbs_cmd_mi(sender)
            case '!mr' | '!mri':
                if not args:
                    await self._bbs_reply(sender, "Usage: !mr N")
                    return
                try:
                    idx = int(args[0])
                except Exception:
                    await self._bbs_reply(sender, "Usage: !mr N")
                    return
                await self._bbs_cmd_mr(sender, idx)
            case '!mdi' | '!md':
                if not args:
                    await self._bbs_reply(sender, "Usage: !mdi N")
                    return
                try:
                    idx = int(args[0])
                except Exception:
                    await self._bbs_reply(sender, "Usage: !mdi N")
                    return
                await self._bbs_cmd_mdi(sender, idx)
            case '!mo':
                include_arch = any(a == '-a' for a in args)
                await self._bbs_cmd_mo(sender, include_arch)
            case '!mro':
                if not args:
                    await self._bbs_reply(sender, "Usage: !mro N")
                    return
                try:
                    idx = int(args[0])
                except Exception:
                    await self._bbs_reply(sender, "Usage: !mro N")
                    return
                await self._bbs_cmd_mro(sender, idx)
            case '!mdo':
                if not args:
                    await self._bbs_reply(sender, "Usage: !mdo N")
                    return
                try:
                    idx = int(args[0])
                except Exception:
                    await self._bbs_reply(sender, "Usage: !mdo N")
                    return
                await self._bbs_cmd_mdo(sender, idx)
            case '!moa':
                await self._bbs_cmd_moa(sender)
            case '!ms':
                await self._bbs_cmd_ms(sender, args)
            case _:
                await self._bbs_reply(sender, "Unknown BBS command. Send !h or !hm")

    async def _bbs_cmd_mi(self, recipient_node_id: str) -> None:
        if not self.bbs:
            return
        # Before listing, finalize any first-seen messages for this recipient and notify
        try:
            await self._bbs_auto_send_first_seen(recipient_node_id)
        except Exception:
            pass
        # Mark queued items as delivered now that recipient asked for inbox
        try:
            _ = self.bbs.mark_delivered_for_recipient(recipient_node_id)
        except Exception:
            pass
        rows = self.bbs.list_inbox(recipient_node_id, include_archived=False)
        if not rows:
            await self._bbs_reply(recipient_node_id, "Inbox is empty.")
            return
        # Build session and formatted list (no content)
        pm_ids = [r.id for r in rows]
        ttl = int(self.config.get('bbs.session_index_ttl_seconds', 180))
        session_id = self.bbs.create_session(recipient_node_id, 'inbox', ttl, pm_ids)
        header = "Inbox (latest first):"
        lines: list[str] = []
        for idx, r in enumerate(rows, start=1):
            # Inbox shows only 'unread' or 'read' (ignore delivered_at here)
            status = 'read' if (r.read_at or r.status == 'read') else 'unread'
            date = r.created_at.split(' ')[0] if r.created_at else ''
            from_short = r.sender_short or (r.sender_node_id or '')
            from_long = r.sender_long or ''
            lines.append(f"[{idx}] {date} — From {from_short} ({from_long}, {r.sender_node_id}) — status={status}")
        payload = self._bbs_compose_list(header, lines)
        payload += f"\nsend !mr N to read, !mdi N to delete"
        await self._bbs_reply(recipient_node_id, payload)

    async def _bbs_cmd_mr(self, recipient_node_id: str, idx: int) -> None:
        if not self.bbs:
            return
        pm_id = self.bbs.resolve_session_index(recipient_node_id, 'inbox', idx)
        if not pm_id:
            await self._bbs_reply(recipient_node_id, "No active list. Send !mi first.")
            return
        row = self.bbs.get_pm(pm_id)
        if not row:
            await self._bbs_reply(recipient_node_id, "Message not found.")
            return
        # Mark read regardless of ACK
        self.bbs.mark_read(pm_id)
        # Send header + content; prefer single packet if it fits, else send two packets
        try:
            from_short = row.sender_short or (row.sender_node_id or "")
            from_long = row.sender_long or ""
            from_id = row.sender_node_id or ""
            header = f"From {from_short}"
            if from_long:
                header += f" ({from_long})"
            if from_id:
                header += f" [{from_id}]"
            full = f"{header}\n{row.text}" if row.text else header
            if len(full.encode('utf-8')) <= 200:
                await self.meshtastic.send_short_message(full, recipient_node_id, channel=0)
            else:
                await self.meshtastic.send_short_message(header, recipient_node_id, channel=0)
                await self.meshtastic.send_short_message(row.text, recipient_node_id, channel=0)
        except Exception as e:
            await self._bbs_reply(recipient_node_id, f"Failed to send content: {e}")

    async def _bbs_cmd_mdi(self, recipient_node_id: str, idx: int) -> None:
        if not self.bbs:
            return
        pm_id = self.bbs.resolve_session_index(recipient_node_id, 'inbox', idx)
        if not pm_id:
            await self._bbs_reply(recipient_node_id, "No active list. Send !mi first.")
            return
        row = self.bbs.get_pm(pm_id)
        if not row:
            await self._bbs_reply(recipient_node_id, "Message not found.")
            return
        info = f"{self._bbs_fmt_from(row)} — '{self._bbs_preview(row.text)}'"
        self.bbs.archive(pm_id)
        await self._bbs_reply(recipient_node_id, f"Deleted inbox: {info}")

    async def _bbs_cmd_mo(self, sender_node_id: str, include_archived: bool) -> None:
        if not self.bbs:
            return
        rows = self.bbs.list_outbox(sender_node_id, include_archived=include_archived)
        if not rows:
            await self._bbs_reply(sender_node_id, "Outbox is empty.")
            return
        pm_ids = [r.id for r in rows]
        ttl = int(self.config.get('bbs.session_index_ttl_seconds', 180))
        _ = self.bbs.create_session(sender_node_id, 'outbox', ttl, pm_ids)
        header = "Outbox (latest first):"
        lines: list[str] = []
        for idx, r in enumerate(rows, start=1):
            date = r.created_at.split(' ')[0] if r.created_at else ''
            status_label = 'read' if r.read_at else ('sent' if getattr(r, 'delivered_at', None) else (r.status or 'queued'))
            lines.append(f"[{idx}] {date} — {self._bbs_fmt_to(r)} — status={status_label}")
        payload = self._bbs_compose_list(header, lines)
        await self._bbs_reply(sender_node_id, payload)

    async def _bbs_cmd_mro(self, sender_node_id: str, idx: int) -> None:
        if not self.bbs:
            return
        pm_id = self.bbs.resolve_session_index(sender_node_id, 'outbox', idx)
        if not pm_id:
            await self._bbs_reply(sender_node_id, "No active list. Send !mo first.")
            return
        row = self.bbs.get_pm(pm_id)
        if not row:
            await self._bbs_reply(sender_node_id, "Message not found.")
            return
        await self._bbs_reply(sender_node_id, row.text)

    async def _bbs_cmd_mdo(self, sender_node_id: str, idx: int) -> None:
        if not self.bbs:
            return
        pm_id = self.bbs.resolve_session_index(sender_node_id, 'outbox', idx)
        if not pm_id:
            await self._bbs_reply(sender_node_id, "No active list. Send !mo first.")
            return
        row = self.bbs.get_pm(pm_id)
        if not row:
            await self._bbs_reply(sender_node_id, "Message not found.")
            return
        # Only allow deleting own outbox entries
        if row.sender_node_id != sender_node_id:
            await self._bbs_reply(sender_node_id, "Not allowed.")
            return
        info = f"{self._bbs_fmt_to(row)} — '{self._bbs_preview(row.text)}' (was {row.status})"
        self.bbs.archive(pm_id)
        await self._bbs_reply(sender_node_id, f"Deleted outbox: {info}")

    async def _bbs_cmd_moa(self, sender_node_id: str) -> None:
        if not self.bbs:
            return
        n = self.bbs.archive_all_read_for_sender(sender_node_id)
        await self._bbs_reply(sender_node_id, f"Deleted {n} read message(s)")

    async def _bbs_cmd_ms(self, sender_node_id: str, args: list[str]) -> None:
        if not self.bbs:
            return
        # Two forms:
        # 1) !ms <target> <message>
        # 2) !ms <index>    (after we presented candidates for this sender)
        if not args:
            await self._bbs_reply(sender_node_id, "Usage: !ms <shortname|!nodeid> <message> | !ms N")
            return
        # Check if this is a numeric selection first
        if len(args) == 1 and args[0].isdigit():
            idx = int(args[0])
            await self._bbs_cmd_ms_select(sender_node_id, idx)
            return
        # Otherwise treat as new send request
        target = args[0]
        text = " ".join(args[1:]).strip()
        if not text:
            await self._bbs_reply(sender_node_id, "Message is empty")
            return
        # Enforce 200-byte limit for stored payloads (readback uses single packet)
        if len(text.encode('utf-8')) > 200:
            await self._bbs_reply(sender_node_id, "Message too long (200B max)")
            return
        # Quota enforcement
        try:
            max_per = int(self.config.get('bbs.outbox.max_per_sender', 10))
        except Exception:
            max_per = 10
        queued = self.bbs.count_queued_for_sender(sender_node_id)
        if queued >= max_per:
            await self._bbs_reply(sender_node_id, f"Outbox full ({queued}/{max_per})")
            return
        # Resolve recipient: accept literal !nodeid or try to match by shortName (case-insensitive exact)
        recipient_node_id: str | None = None
        recipient_short: str | None = None
        recipient_long: str | None = None
        candidates: list[dict[str, str]] = []
        if target.startswith('!') and len(target) > 1:
            # Treat as node id
            recipient_node_id = target
            n = self.node_manager.get_node(recipient_node_id)
            if n:
                recipient_short = n.get('shortName')  # type: ignore[index]
                recipient_long = n.get('longName')   # type: ignore[index]
        else:
            # Exact shortName match among known nodes (case-insensitive)
            tnorm = target.strip().lower()
            for nid, n in self.node_manager.get_all_nodes().items():
                try:
                    sn = n.get('shortName')  # type: ignore[index]
                    ln = n.get('longName')   # type: ignore[index]
                    if isinstance(sn, str) and sn.strip() and sn.strip().lower() == tnorm:
                        candidates.append({'node_id': nid, 'short': sn.strip(), 'long': (ln.strip() if isinstance(ln, str) else '')})
                except Exception:
                    continue
            if len(candidates) == 1:
                c = candidates[0]
                recipient_node_id = c['node_id']
                recipient_short = c['short']
                recipient_long = c['long']
        # Insert queued PM
        try:
            import json
            pm_id = self.bbs.insert_pm(
                sender_node_id=sender_node_id,
                sender_short=(self.node_manager.get_node(sender_node_id) or {}).get('shortName') if self.node_manager.get_node(sender_node_id) else None,  # type: ignore[index]
                sender_long=(self.node_manager.get_node(sender_node_id) or {}).get('longName') if self.node_manager.get_node(sender_node_id) else None,  # type: ignore[index]
                sender_source='mesh',
                recipient_node_id=recipient_node_id,
                recipient_short=recipient_short,
                recipient_long=recipient_long,
                candidates_json=(json.dumps(candidates) if candidates and len(candidates) != 1 else None),
                text=text,
            )
        except Exception as e:
            await self._bbs_reply(sender_node_id, f"Store failed: {e}")
            return
        # Acknowledge and, if ambiguous, present candidate list with selection prompt
        if recipient_node_id:
            label = recipient_short or recipient_node_id
            await self._bbs_reply(sender_node_id, f"Queued to {label} [{recipient_node_id}] — '{self._bbs_preview(text)}'")
            # Immediately notify the recipient to fetch inbox (bypass cooldown)
            try:
                unread = self.bbs.unread_count_for_recipient(recipient_node_id)
                if unread > 0:
                    _ = self.bbs.mark_all_notified(recipient_node_id)
                    await self.meshtastic.send_short_message(f"You have {unread} PM(s). Send !mi", recipient_node_id, channel=0)
            except Exception:
                pass
            return
        if candidates:
            # Present selection list and create a session tied to this outbox PM id for this sender
            header = "Select recipient: 0=first-seen"
            lines = []
            idx_map_pmids: list[int] = []
            for i, c in enumerate(candidates, start=1):
                nid = c.get('node_id', '')
                sn = c.get('short', '')
                ln = c.get('long', '')
                lines.append(f"[{i}] {sn} ({ln}, {nid})")
                idx_map_pmids.append(pm_id)  # same pm, selection will finalize recipient
            # We use an 'outbox' kind session, but it references this single pm_id for all indices
            ttl = int(self.config.get('bbs.session_index_ttl_seconds', 180))
            _ = self.bbs.create_session(sender_node_id, 'outbox', ttl, idx_map_pmids)
            payload = self._bbs_compose_list(header, lines)
            await self._bbs_reply(sender_node_id, payload + "\nReply: !ms N")
            return
        # Fallback: unknown target — store as first-seen but require explicit confirmation (0) to enable delivery
        await self._bbs_reply(sender_node_id, f"Select recipient: 0=first-seen\nReply: !ms 0")

    async def _bbs_cmd_ms_select(self, sender_node_id: str, idx: int) -> None:
        """Handle '!ms N' after an ambiguous target list, including 0 for first-seen."""
        if not self.bbs:
            return
        if idx < 0:
            await self._bbs_reply(sender_node_id, "Invalid index")
            return
        if idx == 0:
            # User chose first-seen; enable auto-delivery for the most recent queued ambiguous PM for this sender
            # Find latest queued PM without recipient for this sender and enable first-seen
            try:
                rows = self.bbs.list_outbox(sender_node_id, include_archived=False)
                target_pm = next((r for r in rows if not r.recipient_node_id and r.status == 'queued'), None)
                if target_pm:
                    self.bbs.set_first_seen_enabled(target_pm.id, True)
                    await self._bbs_reply(sender_node_id, "ok: first-seen enabled")
                else:
                    await self._bbs_reply(sender_node_id, "No pending message to mark first-seen.")
            except Exception as e:
                await self._bbs_reply(sender_node_id, f"Failed: {e}")
            return
        # Resolve to the most recent 'outbox' session index
        pm_id = self.bbs.resolve_session_index(sender_node_id, 'outbox', idx)
        if not pm_id:
            await self._bbs_reply(sender_node_id, "No active selection. Send !ms <tgt> <msg> again.")
            return
        # We need to re-resolve candidates from the original text to fetch the chosen index
        row = self.bbs.get_pm(pm_id)
        if not row or not row.candidates_json:
            await self._bbs_reply(sender_node_id, "No candidates for selection.")
            return
        try:
            import json
            cands = json.loads(row.candidates_json)
        except Exception:
            await self._bbs_reply(sender_node_id, "Invalid candidates data.")
            return
        if not isinstance(cands, list) or idx < 1 or idx > len(cands):
            await self._bbs_reply(sender_node_id, "Invalid index")
            return
        chosen = cands[idx - 1]
        nid = chosen.get('node_id')
        sn = chosen.get('short')
        ln = chosen.get('long')
        if not isinstance(nid, str) or not nid:
            await self._bbs_reply(sender_node_id, "Invalid selection")
            return
        # Finalize recipient on that PM
        try:
            self.bbs.update_recipient(pm_id, node_id=nid, short=sn, long=ln)
        except Exception as e:
            await self._bbs_reply(sender_node_id, f"Finalize failed: {e}")
            return
        label = sn or nid
        await self._bbs_reply(sender_node_id, f"Recipient set: {label} [{nid}]")
        # Immediately notify the chosen recipient to fetch inbox (bypass cooldown)
        try:
            unread = self.bbs.unread_count_for_recipient(nid)
            if unread > 0:
                _ = self.bbs.mark_all_notified(nid)
                await self.meshtastic.send_short_message(f"You have {unread} PM(s). Send !mi", nid, channel=0)
        except Exception:
            pass

    # --- Notification hook: on inbound packets, detect appearances and nudge ---
    async def _bbs_maybe_notify_on_appearance(self, from_id: str) -> None:
        if not self.bbs:
            return
        try:
            if not bool(self.config.get('bbs.enabled', True)):
                return
        except Exception:
            pass
        # First-seen finalize for this appearing node
        finalized = 0
        try:
            finalized = await self._bbs_auto_send_first_seen(from_id)
        except Exception:
            finalized = 0

        # Only notify the appearing node about its unread count, with cooldown
        unread = self.bbs.unread_count_for_recipient(from_id)
        if unread <= 0:
            return
        # If we just finalized first-seen items, bypass cooldown once
        bypass_cooldown = finalized > 0
        ok_to_notify = True
        if not bypass_cooldown:
            # Cooldown check
            try:
                cooldown_h = float(self.config.get('bbs.notify.cooldown_hours', 6))
            except Exception:
                cooldown_h = 6.0
            last = self.bbs.latest_notified_at(from_id)
            if last:
                try:
                    from datetime import datetime as _dt
                    last_dt = _dt.strptime(last, "%Y-%m-%d %H:%M:%S%z")
                    from datetime import timezone as _tz, timedelta as _td
                    if (_dt.now(_tz.utc) - last_dt) < _td(hours=cooldown_h):
                        ok_to_notify = False
                except Exception:
                    pass
        if not ok_to_notify:
            return
        # Mark queued as notified and send a nudge
        changed = self.bbs.mark_all_notified(from_id)
        if changed <= 0:
            return
        try:
            msg = f"You have {unread} PM(s). Send !mi"
            await self.meshtastic.send_short_message(msg, from_id, channel=0)
        except Exception:
            pass

    async def _bbs_auto_send_first_seen(self, node_id: str) -> int:
        """Finalize and deliver any queued first-seen PMs for this node.

        Returns number of messages delivered.
        """
        if not self.bbs:
            return 0
        try:
            rows = self.bbs.find_queued_first_seen_for_node(node_id)
        except Exception:
            rows = []
        delivered = 0
        for r in rows:
            try:
                self.bbs.update_recipient(r.id, node_id=node_id, short=None, long=None)
                delivered += 1
                # Optional: inform sender that delivery occurred
                try:
                    snd = r.sender_node_id
                    if isinstance(snd, str) and snd:
                        await self.meshtastic.send_short_message(
                            f"{node_id} seen: PM queued. They will be nudged to !mi. '{self._bbs_preview(r.text)}'",
                            snd,
                            channel=0,
                        )
                except Exception:
                    pass
            except Exception:
                # leave queued; retry on next appearance or !mi
                pass
        return delivered

    # --- AI Helpers ---
    async def _get_ai_client(self):
        if self._ai_client is None:
            try:
                provider = str(self.config.get('ai.provider', 'ollama')).strip().lower()
                env_script = self.config.get('telemetry.environment_script', None)
                strip_default = bool(self.config.get('ai.strip_thinking', True))
                if provider == 'openai':
                    mod = import_module('openai_client')
                    Client = getattr(mod, 'OpenAIClient')
                    base_url = self.config.get('ai.openai.base_url', 'https://api.openai.com/v1')
                    model = self.config.get('ai.openai.model', 'gpt-4o-mini')
                    api_key = self.config.get('ai.openai.api_key', "")
                    self._ai_client = Client(
                        base_url=base_url,
                        model=model,
                        api_key=api_key,
                        environment_script=env_script,
                        enable_thinking_default=bool(self.config.get('ai.enable_thinking', False)),
                        strip_thinking_default=strip_default,
                    )
                else:
                    mod = import_module('ollama_client')
                    Client = getattr(mod, 'OllamaClient')
                    base_url = self.config.get('ai.ollama.base_url', 'http://127.0.0.1:11434')
                    model = self.config.get('ai.ollama.model', 'llama3')
                    self._ai_client = Client(
                        base_url=base_url,
                        model=model,
                        environment_script=env_script,
                        enable_thinking_default=bool(self.config.get('ai.enable_thinking', False)),
                        strip_thinking_default=strip_default,
                    )
            except Exception as e:
                self.logger.error(f"Failed to init AI client: {e}")
                return None
        return self._ai_client

    async def _ai_chat(self, prompt: str, conversation_id: str | None = None) -> str:
        client = await self._get_ai_client()
        if not client:
            return "[AI unavailable]"
        enable_tools = self.config.get('ai.enable_tools', False)
        enable_thinking = bool(self.config.get('ai.enable_thinking', False))
        strip = bool(self.config.get('ai.strip_thinking', False))
        chat_kwargs = {
            "prompt": prompt,
            "system": self.ai_system_prompt,
            "keep_history": True,
            "conversation_id": conversation_id or 'global',
            "enable_tools": enable_tools,
            "strip_thinking": strip,
        }
        if enable_thinking:
            chat_kwargs["enable_thinking"] = True
        return await client.chat(**chat_kwargs)

    def _split_mesh(self, text: str) -> list[str]:
        """Split text into chunks of at most 200 UTF-8 bytes.

        - Counts bytes (not characters) so multi-byte code points don't overflow.
        - Prefers splitting on newline or space when available in the current window.
        - Never splits inside a UTF-8 code point (iterates by Python characters).
        """
        limit = 200
        # Fast path based on UTF-8 bytes
        if len(text.encode('utf-8')) <= limit:
            return [text]

        chunks: list[str] = []
        start = 0
        byte_count = 0
        last_break_index = -1

        for i, ch in enumerate(text):
            ch_bytes = len(ch.encode('utf-8'))
            if ch in {'\n', ' ', '.', ',', '!', '?', ';', ':', '\t'}:
                last_break_index = i

            if byte_count + ch_bytes > limit:
                if last_break_index >= start:
                    # Split at the last break position within window
                    chunks.append(text[start:last_break_index + 1].rstrip())
                    start = last_break_index + 1
                else:
                    # No natural break; split at current char boundary
                    chunks.append(text[start:i])
                    start = i
                byte_count = 0
                last_break_index = -1

            byte_count += ch_bytes

        if start < len(text):
            chunks.append(text[start:])
        return chunks

    # --- Meshtastic Message Handlers ---
    async def handle_meshtastic_message(self, packet: dict[str, object]) -> None:
        if packet.get('type') == 'ack':
            await self.handle_ack(packet)
            return

        # Human readable dispatch log (supplements structured events)
        is_ringtone = (
            packet.get('decoded', {}).get('portnum') == 'ADMIN_APP' and
            'getRingtoneResponse' in packet.get('decoded', {}).get('admin', {})
        )
        if not is_ringtone:
            self.logger.debug(
                f"[Meshtastic] Dispatching packet from={packet.get('fromId')} to={packet.get('toId')} "
                f"type={packet.get('decoded', {}).get('portnum')} id={packet.get('id')}"
            )

        portnum = packet.get('decoded', {}).get('portnum', '')  # type: ignore[index, attr-defined]
        handler_name = f"handle_{portnum.lower()}" if isinstance(portnum, str) else f"handle_{portnum}"
        handler = getattr(self, handler_name, None)

        # BBS appearance-based notification: treat any non-ringtone inbound packet as an appearance signal
        try:
            fid = packet.get('fromId')
            local_id = getattr(self.meshtastic, 'my_node_id', None)
            if isinstance(fid, str) and fid and fid != local_id and not is_ringtone:
                _ = asyncio.create_task(self._bbs_maybe_notify_on_appearance(fid))
        except Exception:
            pass

        if handler:
            # if not (portnum == 'ADMIN_APP' and 'getRingtoneResponse' in packet.get('decoded', {}).get('admin', {})):
                # self.logger.info(f"Handling Meshtastic message type {portnum} from {formatted_name}")
            await handler(packet)
        elif not portnum:
            # Ignoring private message (no event needed)
            pass
        else:
            self.logger.warning(
                f"Unhandled Meshtastic message type: {portnum} from: {packet.get('fromId')}, packet:\n{packet}"
            )

    async def handle_ack(self, packet: dict[str, object]) -> None:  # type: ignore[override]
        """Process ACK updates: add reaction in Telegram and clear tracking map."""
        message_id = packet.get('request_id')
        if message_id is None:
            # self.logger.warning(f"Received ACK without message ID\n{packet=}\n")
            self.logger.warning("ack_missing_id", instance=self.instance_id)
            return

        try:
            # Defensive cast: library may supply str/int; treat others as invalid
            if isinstance(message_id, (str, int)):
                message_id_int = int(message_id)
            else:
                raise ValueError("unsupported message_id type")
        except Exception:
            self.logger.warning(f"ACK message id not convertible to int: {message_id}")
            return
        pending_message = self.pending_acks.pop(message_id_int, None)
        if pending_message:
            telegram_message_id = pending_message.get('telegram_message_id')
            if telegram_message_id:
                await self.telegram.add_reaction(telegram_message_id, '👌')
                # self.logger.info(f"ACK processed for message ID: {message_id}, Telegram message ID: {telegram_message_id}")
                self.logger.info("ack_processed", instance=self.instance_id, message_id=message_id_int, telegram_message_id=telegram_message_id, bridge_id=pending_message.get('bridge_id'))
            else:
                self.logger.info("ack_processed", instance=self.instance_id, message_id=message_id_int, telegram_message_id=None, bridge_id=pending_message.get('bridge_id'))
        else:
            self.logger.info(f"ack_processed", message_id=message_id)

    async def handle_text_message_app(self, packet: dict[str, object]) -> None:  # type: ignore[override]
        """Format and forward a Meshtastic text message to Telegram (enriched logging)."""
        try:
            _fid = packet.get('fromId')
            _tid = packet.get('toId')
            _fsn = _fln = _tsn = _tln = None
            try:
                if isinstance(_fid, str):
                    _n = self.node_manager.nodes.get(_fid)
                    if _n:
                        _fsn = _n.get('shortName')
                        _fln = _n.get('longName')
                if isinstance(_tid, str):
                    _n = self.node_manager.nodes.get(_tid)
                    if _n:
                        _tsn = _n.get('shortName')
                        _tln = _n.get('longName')
            except Exception:
                pass
            self.logger.info(
                "mt_handle_text",
                from_id=_fid, from_short=_fsn, from_long=_fln,
                to_id=_tid, to_short=_tsn, to_long=_tln,
                channel=packet.get('channel'),
                text=packet.get('decoded', {}).get('payload'),
            )
        except Exception:
            self.logger.info( "mt_handle_text", from_id=packet.get('fromId'), to_id=packet.get('toId'), channel=packet.get('channel'), text=packet.get('decoded', {}).get('payload'))
        bridge_id = new_id()
        sender = str(packet.get('fromId', 'unknown'))
        recipient = str(packet.get('toId', 'unknown'))
        decoded = cast(dict, packet.get('decoded', {}))
        payload = decoded.get('payload', b'')
        text: str = payload.decode('utf-8') if isinstance(payload, (bytes, bytearray)) else str(payload)
        request = text
        tg_trigger_responses: list[str] = []
        # Normalize channel number to int for type safety
        raw_channel = packet.get('channel', 0)
        try:
            channel_num: int = int(raw_channel)  # type: ignore[arg-type]
        except Exception:
            channel_num = 0
        ignored_channels = self.config.get('meshtastic.ignored_channels', [])  # type: ignore[assignment]
        # ignore messages and commands on ignored channels
        if channel_num in ignored_channels:
            return

        # Node metadata
        node = self.node_manager.nodes.get(sender)
        node_recipient = self.node_manager.nodes.get(recipient)  # type: ignore[index]
        from_short = sender
        from_long = None
        to_short = recipient
        to_long = None
        if node:
            sn = node.get('shortName', '')  # type: ignore[index]
            ln = node.get('longName', '')  # type: ignore[index]
            if isinstance(sn, str) and sn.strip() and sn.lower() != 'unknown':
                from_short = sn.strip()
            if isinstance(ln, str) and ln.strip() and ln.lower() != 'unknown':
                from_long = ln.strip()
        if node_recipient:
            rsn = node_recipient.get('shortName', '')  # type: ignore[index]
            rln = node_recipient.get('longName', '')  # type: ignore[index]
            if isinstance(rsn, str) and rsn.strip() and rsn.lower() != 'unknown':
                to_short = rsn.strip()
            if isinstance(rln, str) and rln.strip() and rln.lower() != 'unknown':
                to_long = rln.strip()

            self.logger.info("bridge_start", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg", from_id=sender, to_id=recipient, from_short=from_short, from_long=from_long, to_short=to_short, to_long=to_long)

        # Metrics (used for command handlers and rendering)
        hops_start = packet.get('hopStart', 0)  # type: ignore[index]
        hops_limit = packet.get('hopLimit', 0)  # type: ignore[index]
        try:
            hops_away = hops_start - hops_limit  # type: ignore[operator]
        except Exception:
            hops_away = 0
        snr = packet.get('rxSnr', 'n/a')  # type: ignore[index]
        rssi = packet.get('rxRssi', 'n/a')  # type: ignore[index]
        mqtt = bool(packet.get('viaMqtt', 0))  # type: ignore[index]
        # Signal quality and info formatting
        signal_emoji = "❓"
        signal_label = "Unknown"
        try:
            rssi_val = float(str(rssi)) if rssi != 'n/a' else None  # type: ignore[arg-type]
            snr_val = float(str(snr)) if snr != 'n/a' else None  # type: ignore[arg-type]
            if rssi_val is not None and snr_val is not None:
                if rssi_val > -80 and snr_val > 8:
                    signal_emoji = "😃"; signal_label = "Excellent"
                elif rssi_val > -90 and snr_val > 2:
                    signal_emoji = "🙂"; signal_label = "Good"
                elif rssi_val > -100 and snr_val > -5:
                    signal_emoji = "😐"; signal_label = "Fair"
                else:
                    signal_emoji = "😣"; signal_label = "Bad"
            elif rssi_val is not None:
                if rssi_val > -80:
                    signal_emoji = "😃"; signal_label = "Excellent"
                elif rssi_val > -90:
                    signal_emoji = "🙂"; signal_label = "Good"
                elif rssi_val > -100:
                    signal_emoji = "😐"; signal_label = "Fair"
                else:
                    signal_emoji = "😣"; signal_label = "Bad"
            elif snr_val is not None:
                if snr_val > 8:
                    signal_emoji = "😃"; signal_label = "Excellent"
                elif snr_val > 2:
                    signal_emoji = "🙂"; signal_label = "Good"
                elif snr_val > -5:
                    signal_emoji = "😐"; signal_label = "Fair"
                else:
                    signal_emoji = "😣"; signal_label = "Bad"
        except Exception:
            pass

        receive_only_channels = self.config.get('meshtastic.receive_only_channels', [])  # type: ignore[assignment]
    # Apply config-driven triggers: transform text and send any trigger replies
        if channel_num not in receive_only_channels and isinstance(text, str):
            try:
                text, trigger_replies = self._run_meshtastic_triggers(
                    text,
                    ctx={
                        'sender': sender,
                        'recipient': recipient,
                        'channel_num': channel_num,
                        'from_short': from_short,
                        'to_short': to_short,
                        'hops_start': hops_start,
                        'hops_limit': hops_limit,
                        'hops_away': hops_away,
                        'rssi': rssi,
                        'snr': snr,
                        'mqtt': mqtt,
                        'signal_emoji': signal_emoji,
                        'signal_label': signal_label,
                    }
                )
                # Send any queued replies due to triggers (e.g., op=reply/test)
                if trigger_replies:
                    # Keep for Telegram rendering
                    try:
                        tg_trigger_responses = list(trigger_replies)
                    except Exception:
                        tg_trigger_responses = trigger_replies  # type: ignore[assignment]
                    send_to = sender
                    out_channel = 0
                    if recipient == "^all":
                        send_to = "^all"
                        out_channel = channel_num
                    # if self.config.get('meshtastic.reply_directly', False):
                        # send_to = sender
                        # out_channel = 0
                    for idx, reply_text in enumerate(trigger_replies, start=1):
                        try:
                            meshtastic_message_id = await self.meshtastic.send_message(reply_text, send_to, channel=out_channel)
                            self.pending_acks[meshtastic_message_id] = {
                                'telegram_message_id': 0,
                                'telegram_thread_id': 0,
                                'timestamp': datetime.now(timezone.utc)
                            }
                            self.logger.info(
                                "trigger_reply_sent",
                                instance=self.instance_id,
                                bridge_id=bridge_id,
                                meshtastic_message_id=meshtastic_message_id,
                                idx=idx,
                                sender=sender,
                                recipient=recipient,
                                channel=out_channel,
                            )
                        except Exception as e:
                            self.logger.error(f"Failed to send trigger reply: {e}", exc_info=True)
            except Exception:
                pass
        
        # BBS: handle DM-only '!' commands before slash commands
        if self.bbs and isinstance(text, str) and text.startswith('!') and self._is_private_dm_to_bot(packet):
            try:
                await self._handle_bbs_command(text.strip(), sender)
            except Exception as e:
                self.logger.error(f"bbs_cmd_error: {e}", exc_info=True)
            return

        is_command: str | None = None
        if channel_num not in receive_only_channels:
            if isinstance(text, str) and text.startswith('/'):
                parts = text.split()
                cmd = parts[0][1:].partition('@')[0].lower()
                args = parts[1:]
                handler_name = f"handle_mesh_cmd_{cmd}"
                handler = getattr(self, handler_name, None)
                if handler:
                    try:
                        is_command = cmd
                        text = await handler(
                            sender=sender,
                            recipient=recipient,
                            channel_num=channel_num,
                            hops_start=hops_start,
                            hops_limit=hops_limit,
                            hops_away=hops_away,
                            mqtt=mqtt,
                            rssi=rssi,
                            snr=snr,
                            bridge_id=bridge_id,
                            args=args,
                            from_short=from_short,
                            to_short=to_short,
                            signal_emoji=signal_emoji,
                            signal_label=signal_label,
                            reply_directly=self.config.get('meshtastic.reply_directly', False),
                        )
                    except Exception as e:
                        self.logger.error(f"Mesh command '/{cmd}' failed: {e}", exc_info=True)
                # If no handler, fall through and forward the original text

        # DM fallback: send help when a DM to the bot is not a command
        if self._is_private_dm_to_bot(packet) and isinstance(text, str) and not text.startswith('!') and not text.startswith('/'):
            try:
                await self.meshtastic.send_short_message(self._bbs_help_general(), sender, channel=0)
            except Exception:
                pass
            return

        if is_command:
            self.logger.info("mesh_command", instance=self.instance_id, bridge_id=bridge_id, command=is_command, from_id=sender, to_id=recipient, from_short=from_short, to_short=to_short, hops_away=hops_away, hop_limit=hops_limit, hop_start=hops_start, rssi=rssi, snr=snr, mqtt=mqtt)
            self.logger.info(f"CMD[{is_command}] from {from_short}:\nREQ: {request}\nRPL: {text}")

        # Persist message to sqlite3 (per-channel table or DIRECT_MESSAGES)
        try:
            if self._msgdb is not None:
                if recipient.startswith('!'):
                    table = 'DIRECT_MESSAGES'
                else:
                    channels = self.config.get('channels', [])  # type: ignore[assignment]
                    table_name: str | None = None
                    if channels:
                        try:
                            table_name = str(channels[channel_num])
                        except Exception:
                            table_name = None
                    table = table_name if table_name else f"CHANNEL_{channel_num}"
                    table = self._sanitize_identifier(table)
                # Ensure table + index exist
                self._ensure_table_and_index(table)
                ts = datetime.now(timezone.utc).isoformat()
                name_val = from_short if isinstance(from_short, str) and from_short else sender
                long_name_val = from_long if isinstance(from_long, str) else ""
                self._msgdb.execute(
                    f"INSERT INTO {table} (timestamp, from_id, name, long_name, message) VALUES (?, ?, ?, ?, ?)",
                    (ts, sender, name_val, long_name_val, 
                        ( text if not is_command else f"[CMD:{request}]: {text}" )  # type: ignore[operator]
                    ),
                )
                self._msgdb.commit()
        except Exception as e:
            self.logger.error(f"db_write_error: {e}", exc_info=True)

        # Build outbound Telegram message (unchanged format except using from_short)
        channel_label = f"[<u>CH{channel_num}</u>]"
        channels = self.config.get('channels', [])  # type: ignore[assignment]
        if channels and not recipient.startswith('!'):
            try:
                channel_name = channels[channel_num]
                channel_label = f"[<u>{channel_name}</u>]"
            except Exception:
                pass

        if is_command:
            message = (
                f"💻 <b>{channel_label} CMD: {is_command} `{sender}` - `{from_short}` - `{from_long}`</b>\n"
                f"<u>[REQ]</u>: {request}\n"
                f"<u>[RPL]</u>: {text}\n"
            )
        elif tg_trigger_responses:
            joined = "\n".join(tg_trigger_responses)
            try:
                self.logger.info(f"TRIGGER from {from_short}:\nREQ: {request}\nRPL: {joined}")
            except Exception:
                pass
            message = (
                f"💡 <b>{channel_label} TRIGGER `{sender}` - `{from_short}` - `{from_long}`</b>\n"
                f"<u>[REQ]</u>: {request}\n"
                f"<u>[RPL]</u>: {joined}\n"
            )
        else:
            message = f"💬 <b>{channel_label} `{sender}` - `{from_long}`</b>\n<u>`{from_short}`</u>: {text}\n\n"

        message += f"↔️ HAway: {hops_away}, HLimit: {hops_limit}"
        if snr and snr != 'n/a':
            message += f", SNR: {snr}dB"
        if rssi and rssi != 'n/a':
            message += f", RSSI: {rssi}dBm"
        if signal_label != 'Unknown':
            message += f", Signal: {signal_emoji} {signal_label}"
        if mqtt:
            message += " (MQTT)"

        # Emit meta + render events
        self.logger.info("bridge_meta", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg", from_short=from_short, from_long=from_long, to_short=to_short, to_long=to_long, hops_away=hops_away, hop_limit=hops_limit, hop_start=hops_start, rssi=rssi, snr=snr, mqtt=mqtt)
        log_text = text.replace('\n', '\\n')
        if len(log_text) > 160:
            log_text = log_text[:160] + '…'
        self.logger.debug("bridge_render", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg", message_text=log_text)
        if channel_num in receive_only_channels:
            message = f"[🚫🤐]  {message}";
        _ = await self.telegram.send_message(message, disable_notification=False, topic=f"channel{channel_num}" if not recipient.startswith('!') else "default")
        self.logger.info("bridge_sent", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg")
        # self.logger.info("bridge_complete", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg")

    # --- Persistence helpers ---
    def _sanitize_identifier(self, name: str) -> str:
        """Return a safe SQLite identifier consisting of A-Z, 0-9, and underscores.

        If the name starts with a digit, prefix with T_. Collapse runs of
        invalid characters into a single underscore.
        """
        safe = re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_")
        if not safe:
            safe = 'T'
        if safe[0].isdigit():
            safe = 'T_' + safe
        return safe.upper()

    def _ensure_table_and_index(self, table: str) -> None:
        if self._msgdb is None:
            return
        # Create schema lazily
        self._msgdb.execute(
            f"CREATE TABLE IF NOT EXISTS {table} (timestamp TEXT NOT NULL, from_id TEXT NOT NULL, name TEXT NOT NULL, long_name TEXT NOT NULL, message TEXT NOT NULL)"
        )
        self._msgdb.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table}_from_id ON {table}(from_id)"
        )

    # --- Telegram Message Handlers ---

    def _run_meshtastic_triggers(self, text: str, ctx: dict[str, Any]) -> tuple[str, list[str]]:
        """Apply meshtastic trigger rules and also collect reply actions.

        - replace: re.sub(key, val, text, count=1)
        - prepend: if regex matches anywhere, prefix val + space unless already present
    - reply: if regex matches, format rule.val as a template with ctx and queue to send
        Returns transformed_text, [reply_text, ...].
        """
        try:
            rules = self.config.get('meshtastic.triggers', [])  # type: ignore[assignment]
        except Exception:
            rules = []
        if not isinstance(rules, list) or not rules:
            return text, []
        import re as _re
        out = text
        replies: list[str] = []
        # Build formatting context for templates
        fmt = dict(ctx)
        # Aliases for convenience in templates
        fmt['short_name'] = ctx.get('from_short')
        # Derived fields
        fmt['signal'] = ctx.get('signal_label')
        fmt['signal_full'] = f"{ctx.get('signal_emoji', '')} {ctx.get('signal_label', '')}".strip()
        fmt['mqtt_flag'] = " (MQTT)" if ctx.get('mqtt') else ""
        # Optional channel name
        try:
            channels = self.config.get('channels', [])  # type: ignore[assignment]
            chn = None
            if isinstance(channels, list) and isinstance(ctx.get('channel_num'), int):
                idx = int(ctx['channel_num'])
                chn = channels[idx] if 0 <= idx < len(channels) else None
            fmt['channel_name'] = chn
        except Exception:
            fmt['channel_name'] = None

        for rule in rules:
            try:
                key = rule.get('key') if isinstance(rule, dict) else None
                op = str(rule.get('op', '')).strip().lower() if isinstance(rule, dict) else ''
                val = rule.get('val') if isinstance(rule, dict) else None
                if not isinstance(key, str) or not key:
                    continue
                pattern = _re.compile(key, flags=_re.IGNORECASE)
                if op == 'replace':
                    if pattern.search(out) and isinstance(val, str):
                        out = pattern.sub(val, out, count=1)
                elif op == 'prepend':
                    if pattern.search(out) and isinstance(val, str):
                        prefix = val if out.lower().startswith(val.lower()) else f"{val} "
                        out = prefix + out
                elif op == 'reply':
                    # Queue a reply if matches. Use val as template or fallback to default if missing.
                    if pattern.search(out):
                        template = val if isinstance(val, str) and val.strip() else (
                            "{short_name}: Ack. Signal {signal_emoji} {signal}. "
                            "HopsAway={hops_away}, HStart={hops_start}, HLimit={hops_limit}, "
                            "RSSI={rssi}, SNR={snr}{mqtt_flag}."
                        )
                        try:
                            replies.append(str(template).format(**fmt))
                        except Exception:
                            # On bad template, append raw template
                            replies.append(str(template))
                else:
                    # Unsupported op -> skip
                    continue
            except Exception:
                # Ignore malformed rules or regex errors
                continue
        return out, replies

    def _apply_meshtastic_triggers(self, text: str) -> str:
        """Apply meshtastic.triggers rules from config to the input text.

                Config shape (YAML):
                    meshtastic:
                        triggers:
                            - key: "^bot[:,\\s]+"
                                op: "replace"
                                val: "/ai"
                            - key: "(flight|plane)"
                                op: "prepend"
                                val: "/travel"

        Behavior:
          - For op=replace: re.sub(key, val, text, count=1)
          - For op=prepend: if regex matches anywhere, prefix val + space unless text already starts with val
        Rules are applied in list order; the text is transformed cumulatively.
        """
        # Legacy helper kept for compatibility in case it's used elsewhere
        transformed, _ = self._run_meshtastic_triggers(text, ctx={})
        return transformed

    def _apply_telegram_triggers(self, text: str) -> str:
        """Apply telegram.triggers rules from config to the input text.

        Same structure as meshtastic.triggers, but under telegram.triggers.
        """
        try:
            rules = self.config.get('telegram.triggers', [])  # type: ignore[assignment]
        except Exception:
            rules = []
        if not isinstance(rules, list) or not rules:
            return text
        import re as _re
        out = text
        for rule in rules:
            try:
                key = rule.get('key') if isinstance(rule, dict) else None
                op = str(rule.get('op', '')).strip().lower() if isinstance(rule, dict) else ''
                val = rule.get('val') if isinstance(rule, dict) else None
                if not (isinstance(key, str) and key and isinstance(val, str)):
                    continue
                pattern = _re.compile(key, flags=_re.IGNORECASE)
                if op == 'replace':
                    if pattern.search(out):
                        out = pattern.sub(val, out, count=1)
                elif op == 'prepend':
                    if pattern.search(out):
                        prefix = val if out.lower().startswith(val.lower()) else f"{val} "
                        out = prefix + out
                else:
                    continue
            except Exception:
                continue
        return out

    async def handle_telegram_message(self, message: TelegramMessage) -> None:
        """Dispatch a normalized Telegram message to its concrete handler."""
        # Inline Telegram triggers: apply regex rules from telegram.triggers and handle /ai locally
        if message.get('type') == 'telegram':
            raw_text = str(message.get('text') or '')
            text_l = raw_text.lstrip()
            # Transform text using configured triggers (replace/prepend)
            try:
                text_l = self._apply_telegram_triggers(text_l)
            except Exception:
                pass
            # Handle /ai inline (same behavior as before)
            if text_l.startswith('/ai'):
                prompt = text_l[len('/ai'):].lstrip()
                thread_id = message.get('thread_id')
                user_id = message.get('user_id')
                if not self.ai_enabled_telegram:
                    await self.telegram.send_message("AI feature (ait) is disabled", topic=str(thread_id) if thread_id is not None else "default")
                    return
                if not prompt:
                    await self.telegram.send_message("Usage: /ai <prompt>", topic=str(thread_id) if thread_id is not None else "default")
                    return
                conv_id = str(user_id) if user_id is not None else 'global'
                reply = await self._ai_chat(prompt, conversation_id=conv_id)
                reply_to = message.get('message_id') if isinstance(message.get('message_id'), int) else None
                await self.telegram.send_message(
                    reply[:3500],
                    topic=str(thread_id) if thread_id is not None else "default",
                    reply_to_message_id=reply_to
                )
                return
        handlers = {
            'command': self.handle_telegram_command,
            'telegram': self.handle_telegram_text,
            'location': self.handle_telegram_location,
            'reaction': self.handle_telegram_reaction
        }
        handler = handlers.get(message['type'])
        if handler:
            await handler(message)
        else:
            self.logger.warning(f"Received unknown message type: {message['type']=}")

    async def handle_telegram_text(self, message: dict[str, object]) -> None:  # type: ignore[override]
        """Forward Telegram text to Meshtastic if forwarding is enabled."""
        if not self.forwarding_enabled:
            self.logger.info("Message forwarding to Meshtastic is disabled, skipping Telegram text message.")
            return
        bridge_id = new_id()
        self.logger.info("bridge_start", instance=self.instance_id, bridge_id=bridge_id, direction="tg_to_mesh")
        sender = str(message['sender'])[:10]
        recipient = self.config.get('meshtastic.default_node_id')
        text = str(message['text'])
        telegram_message_id = cast(int, message['message_id'])
        telegram_thread_id = cast(int, message['thread_id'])
        meshtastic_message = f"[TG:{sender}] {text}"
        channel: int | None = None

        use_topics = self.config.get('telegram.use_topics', False)
        if use_topics:
            topics = self.config.get('topics', {})
            for key, value in topics.items():
                if key.startswith('channel') and value == telegram_thread_id:
                    try:
                        channel = int(key.split('channel')[-1])
                    except Exception:
                        channel = None
                    break

        try:
            meshtastic_message_id = await self.meshtastic.send_message(meshtastic_message, recipient, channel=channel)
            self.pending_acks[meshtastic_message_id] = {
                'telegram_message_id': telegram_message_id,
                'telegram_thread_id': telegram_thread_id,
                'timestamp': datetime.now(timezone.utc),
                'bridge_id': bridge_id,
            }
            _ = asyncio.create_task(self.remove_pending_ack(meshtastic_message_id))
            self.logger.info("bridge_sent", instance=self.instance_id, bridge_id=bridge_id, direction="tg_to_mesh", meshtastic_message_id=meshtastic_message_id)
        except Exception as e:
            self.logger.error(f"Failed to send message to Meshtastic: {e}", exc_info=True)
            await self.telegram.send_message("Failed to send message to Meshtastic. Please try again.", topic=str(telegram_thread_id))
            self.logger.error("bridge_error", instance=self.instance_id, bridge_id=bridge_id, direction="tg_to_mesh", error=str(e))

    async def handle_telegram_command(self, message: TelegramMessage) -> None:  # type: ignore[override]
        """Process a Telegram command message.

        The raw Update object (if present) is used for direct replies. We keep
        formatting simple and escape Markdown V2 to avoid parse errors.
        """
        update = message.get('update')
        command = (message.get('command') or '').lower()
        args: list[str] = message.get('args', []) or []  # type: ignore[assignment]
        user_id = message.get('user_id')
        # Basic context values
        uptime_delta = datetime.now(timezone.utc) - self.start_time

        async def _reply(text: str) -> None:
            if update and update.message:
                try:
                    await update.message.reply_text(  # type: ignore[attr-defined]
                        escape_markdown(text, version=2),
                        parse_mode=ParseMode.MARKDOWN_V2
                    )
                except Exception as e:  # pragma: no cover - network
                    self.logger.debug(f"Failed replying to command /{command}: {e}")

        self.logger.info("tg_cmd_rx", instance=self.instance_id, command=command, user_id=user_id)

        if command == 'status':
            node_count = len(self.node_manager.get_all_nodes())
            features = []
            for k, v in self.reports.items():
                features.append(f"{k}={'on' if v else 'off'}")
            features.append(f"forwarding={'on' if self.forwarding_enabled else 'off'}")
            features.append(f"aim={'on' if self.ai_enabled_mesh else 'off'}")
            features.append(f"ait={'on' if self.ai_enabled_telegram else 'off'}")
            features_str = ", ".join(features)
            await _reply(
                f"Status:\nUptime: {uptime_delta}\nNodes: {node_count}\nReports: {features_str}"
            )
            return
        if command == 'features':
            features_lines = [f"• {k}: {'enabled' if v else 'disabled'}" for k, v in self.reports.items()]
            features_lines.append(f"• forwarding: {'enabled' if self.forwarding_enabled else 'disabled'}")
            features_lines.append(f"• aim (mesh ai): {'enabled' if self.ai_enabled_mesh else 'disabled'}")
            features_lines.append(f"• ait (telegram ai): {'enabled' if self.ai_enabled_telegram else 'disabled'}")
            await _reply("Features:\n" + "\n".join(features_lines))
            return
        if command in ('enable', 'disable') and args:
            target = args[0].lower()
            new_val = (command == 'enable')
            if target in self.reports:
                self.reports[target] = new_val  # type: ignore[index]
                self.logger.info("tg_cmd_feature_toggle", instance=self.instance_id, feature=target, value=new_val)
                await _reply(f"Feature {target} set to {'enabled' if new_val else 'disabled'}")
            elif target in ('forwarding', 'message_forwarding', 'forward'):
                self.forwarding_enabled = new_val
                self.logger.info("tg_cmd_forwarding_toggle", instance=self.instance_id, value=new_val)
                await _reply(f"Forwarding set to {'enabled' if new_val else 'disabled'}")
            elif target in ('aim', 'ai_mesh', 'ait', 'ai_telegram'):
                if target in ('aim', 'ai_mesh'):
                    self.ai_enabled_mesh = new_val
                    await _reply(f"Mesh AI (aim) set to {'enabled' if new_val else 'disabled'}")
                else:
                    self.ai_enabled_telegram = new_val
                    await _reply(f"Telegram AI (ait) set to {'enabled' if new_val else 'disabled'}")
            else:
                await _reply(f"Unknown feature: {target}")
            return
        if command == 'ai':
            if not self.ai_enabled_telegram:
                await _reply("AI feature (ait) is disabled")
                return
            if not args:
                await _reply("Usage: /ai <prompt>")
                return
            prompt = " ".join(args)
            conv_id = str(user_id) if user_id is not None else 'global'
            response = await self._ai_chat(prompt, conversation_id=conv_id)
            await _reply(response[:3500])
            return
        if command == 'aireset':
            client = await self._get_ai_client()
            if client:
                conv_id = str(user_id) if user_id is not None else 'global'
                client.reset(conv_id)
                await _reply("AI context reset for this conversation.")
            else:
                await _reply("AI not initialized")
            return
        if command == 'node':
            if not args:
                await _reply("Usage: /node <node_id>")
                return
            node_id = args[0]
            info_text = self.node_manager.format_node_info(node_id)
            await _reply(info_text)
            return
        if command == 'listnodes':
            nodes_dict = self.node_manager.get_all_nodes()
            if not nodes_dict:
                await _reply("No nodes known yet.")
            else:
                # Collect and sort by shortName (case-insensitive)
                items: list[tuple[str, str, str]] = []
                for node_id, info in nodes_dict.items():
                    if not isinstance(info, dict):
                        info = {}
                    sn = info.get('shortName') or 'unknown'
                    ln = info.get('longName') or 'unknown'
                    items.append((str(node_id), str(sn), str(ln)))
                items.sort(key=lambda t: t[1].lower())
                # Build enumerated lines
                lines: list[str] = []
                for idx, (node_id, sn, ln) in enumerate(items, start=1):
                    lines.append(
                        f"{idx}. `{node_id}` - "
                        f"{sn.decode('utf-8') if isinstance(sn, bytes) else sn} - "
                        f"{ln.decode('utf-8') if isinstance(ln, bytes) else ln}")
                content = "Known nodes:\n" + "\n".join(lines)
                await self.telegram.send_message(content, topic="default")
            return
        if command == 'bell':
            # Placeholder bell implementation: send a small marker to default node
            target = self.config.get('meshtastic.default_node_id')
            try:
                _ = await self.meshtastic.send_message("(bell)", target)
                await _reply("Bell sent.")
            except Exception as e:  # pragma: no cover - network
                await _reply(f"Failed to send bell: {e}")
            return
        # Fallback for unknown commands
        await _reply(f"Unknown command: {command}")

    async def handle_telegram_location(self, message: TelegramMessage) -> None:  # type: ignore[override]
        """Forward a Telegram location if forwarding is enabled."""
        if not self.forwarding_enabled:
            return
        loc = message.get('location') or {}
        lat = loc.get('latitude')
        lon = loc.get('longitude')
        if lat is None or lon is None:
            return
        recipient = self.config.get('meshtastic.default_node_id')
        body = f"[TG:LOC] {lat},{lon}"
        try:
            _ = await self.meshtastic.send_message(body, recipient)
            self.logger.info("bridge_sent", instance=self.instance_id, direction="tg_to_mesh", kind="location")
        except Exception as e:  # pragma: no cover - network
            self.logger.error(f"Failed to forward location: {e}")

    async def handle_telegram_reaction(self, message: TelegramMessage) -> None:  # type: ignore[override]
        """Currently just log reactions; could map to mesh actions later."""
        emoji = message.get('emoji')
        self.logger.debug("tg_reaction", instance=self.instance_id, emoji=emoji)

    # --- (Re)Added Meshtastic App Handlers ---

    # --- Mesh text-app slash command handlers ---
    async def handle_mesh_cmd_ping(
        self,
        *,
        sender: str,
        recipient: str,
        channel_num: int,
        hops_start: int,
        hops_limit: int,
        hops_away: int,
        mqtt: bool,
        rssi: Any,
        snr: Any,
        bridge_id: int,
        args: list[str],
        from_short: str,
        to_short: str,
        signal_emoji: str,
        signal_label: str, 
        reply_directly: bool = False,
    ) -> str:
        """Handle '/ping' issued from the mesh text app and return the reply text.

        Returns the text that should be displayed in Telegram for this command.
        """
        if not self.mesh_commands.get('ping', False):
            return f"{from_short} → ping not enabled"

        ping_text = f"{from_short} → HopsAway={hops_away}, HStart={hops_start}, HLimit={hops_limit}"
        if rssi != 'n/a':
            ping_text += f", RSSI={rssi}"
        if snr != 'n/a':
            ping_text += f", SNR={snr}"
        ping_text += f", Signal={signal_emoji} {signal_label}"
        ping_text += f", (MQTT)" if mqtt else ""

        self.logger.info(
            "ping_command_rx",
            instance=self.instance_id,
            bridge_id=bridge_id,
            sender=sender,
            recipient=recipient,
            from_short=from_short,
            to_short=to_short,
            channel=channel_num,
        )
        sent_to = sender
        if recipient == "^all": # channel message
            send_to = "^all"
        else:
            channel_num = 0  # direct message
        if reply_directly:
            send_to = sender
            channel_num = 0  # direct message

        try:
            meshtastic_message_id = await self.meshtastic.send_message(ping_text, send_to, channel=channel_num)
            self.pending_acks[meshtastic_message_id] = {
                'telegram_message_id': 0,
                'telegram_thread_id': 0,
                'timestamp': datetime.now(timezone.utc)
            }
            self.logger.info(
                "ping_command_reply_sent",
                instance=self.instance_id,
                bridge_id=bridge_id,
                meshtastic_message_id=meshtastic_message_id,
                from_short=from_short,
                to_short=to_short,
            )
        except Exception as e:
            self.logger.error(f"Failed to send ping reply: {e}", exc_info=True)
        return ping_text

    async def handle_mesh_cmd_help(
        self,
        *,
        sender: str,
        recipient: str,
        channel_num: int,
        hops_start: int,
        hops_limit: int,
        hops_away: int,
        mqtt: bool,
        rssi: Any,
        snr: Any,
        bridge_id: int,
        args: list[str],
        from_short: str,
        to_short: str,
        signal_emoji: str,
        signal_label: str,
        reply_directly: bool = False,
    ) -> str:
        """Handle '/help' issued from the mesh text app and return the reply text."""
        if not self.mesh_commands.get('help', False):
            return f"{from_short} → help not enabled"

        cmds_ping = "• /ping" + (" (disabled)" if not self.mesh_commands.get('ping', False) else "") + " - link stats"
        ai_enabled = self.mesh_commands.get('ai', False) and self.ai_enabled_mesh
        cmds_ai = "• /ai" + (" (disabled)" if not ai_enabled else "") + " - chat with AI model"
        cmds_aireset = "• /aireset" + (" (disabled)" if not ai_enabled else "") + " - reset AI context"
        cmds_travel = "• /travel" + (" (disabled)" if not self.config.get('meshtastic.commands.travel', True) else "") + " - travel safety reminder"

        admins = self.config.get('meshtastic.admin_nodes', [])
        sender1 = sender
        if isinstance(sender1, str) and sender1.startswith('!'):
            sender1 = sender1[1:]
        is_admin = sender1 in admins

        cmds_admin = ""
        if is_admin:
            cmds_admin = "• /admin - admin commands"
            
        lines = [
            "Bot commands (for BBS commands send !h):\n",
            f"{cmds_ping}\n",
            f"{cmds_travel}\n",
            f"{cmds_ai}\n",
            f"{cmds_aireset}\n",
            f"{cmds_admin}",
        ]
        help_text = "\n".join(lines)
        # help_command_rx: include long names if available
        from_ln = None
        to_ln = None
        try:
            n = self.node_manager.nodes.get(sender)
            if n:
                from_ln = n.get('longName')
            n = self.node_manager.nodes.get(recipient)
            if n:
                to_ln = n.get('longName')
        except Exception:
            pass
        self.logger.info("help_command_rx", instance=self.instance_id, bridge_id=bridge_id, sender=sender, recipient=recipient, from_short=from_short, from_long=from_ln, to_short=to_short, to_long=to_ln, channel=channel_num)

        send_to = sender
        if recipient == "^all":
            send_to = "^all"
        else:
            channel_num = 0  # direct message
        if reply_directly:
            send_to = sender
            channel_num = 0  # direct message
        try:
            meshtastic_message_id = await self.meshtastic.send_message(help_text, send_to, channel=channel_num)
            self.pending_acks[meshtastic_message_id] = {
                'telegram_message_id': 0,
                'telegram_thread_id': 0,
                'timestamp': datetime.now(timezone.utc)
            }
            self.logger.info(
                "help_command_reply_sent",
                instance=self.instance_id,
                bridge_id=bridge_id,
                meshtastic_message_id=meshtastic_message_id,
                from_short=from_short,
                to_short=to_short,
            )
        except Exception as e:
            self.logger.error(f"Failed to send help reply: {e}", exc_info=True)
        return help_text

    async def handle_mesh_cmd_ai(self, *, sender: str, recipient: str, channel_num: int, hops_start: int, hops_limit: int, hops_away: int, mqtt: bool, rssi: Any, snr: Any, bridge_id: int, args: list[str], from_short: str, to_short: str, signal_emoji: str, signal_label: str, reply_directly: bool = False) -> str:
        if not self.mesh_commands.get('ai', False):
            return f"{from_short} → ai command not enabled"
        if not self.ai_enabled_mesh:
            return f"{from_short} → mesh AI disabled (aim)"
        if not args:
            return f"{from_short} → usage: /ai <prompt>"
        prompt = " ".join(args)
        # Conversation scoping:
        # - Direct message to bot: per-node context (prefer shortName, fallback to node id)
        # - Channel (^all): per-channel global context keyed by channel number
        conv_id: str | None = None
        if recipient == "^all":
            conv_id = f"mesh:channel:{channel_num}"
        else:
            node = self.node_manager.nodes.get(sender) if hasattr(self.node_manager, 'nodes') else None
            try:
                if node:
                    sn = node.get('shortName')
                    if isinstance(sn, str) and sn.strip():
                        conv_id = sn.strip()
            except Exception:
                pass
            if not conv_id:
                conv_id = sender
        reply_full = await self._ai_chat(prompt, conv_id)
        send_to = sender
        if recipient == "^all":
            send_to = "^all"
        else:
            channel_num = 0
        # if reply_directly:
        #     send_to = sender
        #     channel_num = 0
        # Important: do NOT pre-split here. Let MeshtasticInterface handle chunking
        # so it can append "MSG i of N" suffixes consistently across the whole message.
        try:
            # Include requester's shortname so it prefixes each chunk (AI responses only)
            requester_sn = from_short if isinstance(from_short, str) else None
            _ = await self.meshtastic.send_message(reply_full, send_to, channel=channel_num, sender_shortname=requester_sn)
        except Exception:
            pass
        # Provide the full reply for logging/confirmation
        return f"{from_short} → {reply_full}"

    async def handle_mesh_cmd_aireset(self, *, sender: str, recipient: str, channel_num: int, hops_start: int, hops_limit: int, hops_away: int, mqtt: bool, rssi: Any, snr: Any, bridge_id: int, args: list[str], from_short: str, to_short: str, signal_emoji: str, signal_label: str, reply_directly: bool = False) -> str:
        """Reset AI conversation history for this mesh node."""
        if not self.mesh_commands.get('ai', False):
            return f"{from_short} → ai command not enabled"
        if not self.ai_enabled_mesh:
            return f"{from_short} → mesh AI disabled (aim)"
        client = await self._get_ai_client()
        if not client:
            return f"{from_short} → AI unavailable"
        # Conversation scoping for reset mirrors /ai:
        # - Direct message: per-node
        # - Channel (^all): per-channel
        conv_id: str | None = None
        if recipient == "^all":
            conv_id = f"mesh:channel:{channel_num}"
        else:
            node = self.node_manager.nodes.get(sender) if hasattr(self.node_manager, 'nodes') else None
            try:
                if node:
                    sn = node.get('shortName')
                    if isinstance(sn, str) and sn.strip():
                        conv_id = sn.strip()
            except Exception:
                pass
            if not conv_id:
                conv_id = sender
        client.reset(conv_id)
        reply_text = f"{from_short} → AI context reset"
        send_to = sender
        if recipient == "^all":
            send_to = "^all"
        else:
            channel_num = 0
        if reply_directly:
            send_to = sender
            channel_num = 0
        try:
            meshtastic_message_id = await self.meshtastic.send_message(reply_text, send_to, channel=channel_num)
            self.pending_acks[meshtastic_message_id] = {
                'telegram_message_id': 0,
                'telegram_thread_id': 0,
                'timestamp': datetime.now(timezone.utc)
            }
        except Exception:
            pass
        return reply_text

    async def handle_mesh_cmd_travel(self, *, sender: str, recipient: str, channel_num: int, hops_start: int, hops_limit: int, hops_away: int, mqtt: bool, rssi: Any, snr: Any, bridge_id: int, args: list[str], from_short: str, to_short: str, signal_emoji: str, signal_label: str, reply_directly: bool = False) -> str:
        """Reply to '/travel' with a fixed safety message."""
        if not self.config.get('meshtastic.commands.travel', True):
            return f"{from_short} → travel not enabled"
        tpl = self.config.get('meshtastic.travel_template', "This is Varna, Bulgaria. Be safe.")
        try:
            reply_text = str(tpl).format(
                from_short=from_short,
                to_short=to_short,
                hops_away=hops_away,
                hops_limit=hops_limit,
                hops_start=hops_start,
                signal=signal_label,
                signal_emoji=signal_emoji,
            )
        except Exception:
            reply_text = str(tpl)
        # travel_command_rx: include long names if available
        from_ln = None
        to_ln = None
        try:
            n = self.node_manager.nodes.get(sender)
            if n:
                from_ln = n.get('longName')
            n = self.node_manager.nodes.get(recipient)
            if n:
                to_ln = n.get('longName')
        except Exception:
            pass
        self.logger.info("travel_command_rx", instance=self.instance_id, bridge_id=bridge_id, sender=sender, recipient=recipient, from_short=from_short, from_long=from_ln, to_short=to_short, to_long=to_ln, channel=channel_num)
        send_to = sender
        if recipient == "^all":
            send_to = "^all"
        else:
            channel_num = 0  # direct message
        # if reply_directly:
        #     send_to = sender
        #     channel_num = 0
        try:
            meshtastic_message_id = await self.meshtastic.send_message(reply_text, send_to, channel=channel_num)
            self.pending_acks[meshtastic_message_id] = {
                'telegram_message_id': 0,
                'telegram_thread_id': 0,
                'timestamp': datetime.now(timezone.utc)
            }
            self.logger.info("travel_command_reply_sent", instance=self.instance_id, bridge_id=bridge_id, meshtastic_message_id=meshtastic_message_id, from_short=from_short, to_short=to_short)
        except Exception as e:
            self.logger.error(f"Failed to send travel reply: {e}", exc_info=True)
        return reply_text

    async def handle_mesh_cmd_admin(self, *, sender: str, recipient: str, channel_num: int, hops_start: int, hops_limit: int, hops_away: int, mqtt: bool, rssi: Any, snr: Any, bridge_id: int, args: list[str], from_short: str, to_short: str, signal_emoji: str, signal_label: str, reply_directly: bool = False) -> str:
        admin_text = ""
        admins = self.config.get('meshtastic.admin_nodes', [])
        sender1 = sender
        if isinstance(sender1, str) and sender1.startswith('!'):
            sender1 = sender1[1:]
        is_admin = sender1 in admins
        if not is_admin:
            admin_text = f"{from_short} → admin command denied"
        elif not args:
            admin_text = "\n\n".join([
                f"{from_short} → usage: /admin \\<command> [args]",
                "Available commands:",
                "• reboot - reboot the node",
            ])

        else:
            subcmd = args[0].lower()
            if subcmd == 'reboot':
                try:
                    await self.meshtastic.reboot_node()
                    admin_text = f"{from_short} → node reboot in 5 seconds requested"
                except Exception as e:
                    self.logger.error(f"Failed to reboot node {sender}: {e}", exc_info=True)
                    admin_text = f"{from_short} → failed to reboot node: {e}"

        send_to = sender
        if recipient == "^all":
            send_to = "^all"
        else:
            channel_num = 0  # direct message
        if reply_directly:
            send_to = sender
            channel_num = 0  # direct message
        try:
            meshtastic_message_id = await self.meshtastic.send_message(admin_text, send_to, channel=channel_num)
            self.pending_acks[meshtastic_message_id] = {
                'telegram_message_id': 0,
                'telegram_thread_id': 0,
                'timestamp': datetime.now(timezone.utc)
            }
            self.logger.info(
                "admin_command_reply_sent",
                instance=self.instance_id,
                bridge_id=bridge_id,
                meshtastic_message_id=meshtastic_message_id,
                from_short=from_short,
                to_short=to_short,
            )
        except Exception as e:
            self.logger.error(f"Failed to send help reply: {e}", exc_info=True)

        return admin_text

    async def handle_traceroute_app(self, _packet: Dict[str, Any]) -> None:
        self.logger.info("[Meshtastic] Traceroute app packet received (ignored)")
        return

    async def handle_store_forward_app(self, _packet: Dict[str, Any]) -> None:
        self.logger.info("[Meshtastic] StoreForward app packet received (ignored)")
        return

    async def handle_range_test_app(self, _packet: Dict[str, Any]) -> None:
        self.logger.info("[Meshtastic] RangeTest app packet received (ignored)")
        return

    async def handle_nodeinfo_app(self, packet: Dict[str, Any]) -> None:
        raw_id = packet.get('fromId')
        if not self._valid_node_id(raw_id):
            self.logger.warning("mt_node_id_invalid", instance=self.instance_id, app="nodeinfo", raw_id=raw_id)
            self.logger.warning(f"[Meshtastic] Invalid nodeinfo id ignored id={raw_id}")
            return
        node_id: str = str(raw_id)
        node_info: dict[str, Any] = packet.get('decoded', {})  # type: ignore[assignment]
        # If "user" key exists in node_info, use it; otherwise use node_info itself
        # info_to_update = node_info.get('user') if 'user' in node_info else node_info
        # self.node_manager.update_node(node_id, info_to_update)
        self.node_manager.update_node(node_id, node_info)
        if self.reports.get('nodes', True):
            info_text: str = self.node_manager.format_node_info(node_id)
            await self.telegram.send_or_edit_message('nodeinfo', node_id, info_text)
        try:
            _user = node_info.get('user', {}) if isinstance(node_info, dict) else {}
        except Exception:
            _user = {}
        self.logger.info(
            "mt_nodeinfo_handle",
            from_id=raw_id,
            short_name=_user.get('shortName'),
            long_name=_user.get('longName'),
        )

    async def handle_position_app(self, packet: Dict[str, Any]) -> None:
        raw_id = packet.get('fromId')
        if not self._valid_node_id(raw_id):
            self.logger.warning("mt_node_id_invalid", instance=self.instance_id, app="position", raw_id=raw_id)
            self.logger.warning(f"[Meshtastic] Invalid position id ignored id={raw_id}")
            return
        node_id = str(raw_id)
        position = packet.get('decoded', {}).get('position', {})  # type: ignore[assignment]
        self.node_manager.update_node_position(node_id, position)
        if self.reports.get('location', True):
            position_info = self.node_manager.get_node_position(node_id)
            await self.telegram.send_or_edit_message('location', node_id, position_info)
        latitude = position.get('latitudeI', 0) / 1e7
        longitude = position.get('longitudeI', 0) / 1e7
        if latitude != 0 and longitude != 0 and self.reports.get('location', True):
            if self.telegram.bot and self.telegram.chat_id is not None:
                try:
                    await self.telegram.bot.send_location(  # type: ignore[call-arg]
                        chat_id=self.telegram.chat_id,  # type: ignore[arg-type]
                        latitude=latitude,
                        longitude=longitude,
                    )
                except Exception as e:
                    self.logger.debug(f"Failed to send raw location map: {e}")
        try:
            _n = self.node_manager.nodes.get(str(raw_id))
            _sn = _n.get('shortName') if _n else None
            _ln = _n.get('longName') if _n else None
        except Exception:
            _sn = _ln = None
        self.logger.info("mt_position_handle", from_id=raw_id, short_name=_sn, long_name=_ln, latitude=latitude, longitude=longitude)

    async def handle_telemetry_app(self, packet: Dict[str, Any]) -> None:
        raw_id = packet.get('fromId')
        if not self._valid_node_id(raw_id):
            self.logger.warning("mt_node_id_invalid", instance=self.instance_id, app="telemetry", raw_id=raw_id)
            self.logger.warning(f"[Meshtastic] Invalid telemetry id ignored id={raw_id}")
            return
        node_id = str(raw_id)
        telemetry = packet.get('decoded', {}).get('telemetry', {})  # type: ignore[assignment]
        device_metrics = telemetry.get('deviceMetrics', {})
        self.node_manager.update_node_telemetry(node_id, device_metrics)
        if self.reports.get('telemetry', True):
            telemetry_info = self.node_manager.get_node_telemetry(node_id)
            await self.telegram.send_or_edit_message('telemetry', node_id, telemetry_info)
        try:
            _n = self.node_manager.nodes.get(str(raw_id))
            _sn = _n.get('shortName') if _n else None
            _ln = _n.get('longName') if _n else None
        except Exception:
            _sn = _ln = None
        self.logger.info("mt_telemetry_handle", from_id=raw_id, short_name=_sn, long_name=_ln, device_metrics=device_metrics)

    async def handle_admin_app(self, packet: dict[str, Any]) -> None:
        admin_message = packet.get('decoded', {}).get('admin', {})
        if 'getRingtoneResponse' not in admin_message:
            self.logger.info(
                f"[Meshtastic] Handling admin message from={packet.get('fromId')} "
                f"keys={list(admin_message.keys())}"
            )
        if 'getRouteReply' in admin_message:
            await self._handle_route_reply(admin_message, packet.get('toId', 'unknown'))
        elif 'deviceMetrics' in admin_message:
            raw_id = packet.get('fromId')
            if not self._valid_node_id(raw_id):
                self.logger.warning("mt_node_id_invalid", instance=self.instance_id, app="admin_deviceMetrics", raw_id=raw_id)
                self.logger.warning(f"[Meshtastic] Invalid admin deviceMetrics id ignored id={raw_id}")
            else:
                await self._handle_device_metrics(str(raw_id), admin_message['deviceMetrics'])
        elif 'position' in admin_message:
            raw_id = packet.get('fromId')
            if not self._valid_node_id(raw_id):
                self.logger.warning("mt_node_id_invalid", instance=self.instance_id, app="admin_position", raw_id=raw_id)
                self.logger.warning(f"[Meshtastic] Invalid admin position id ignored id={raw_id}")
            else:
                await self._handle_position(str(raw_id), admin_message['position'])
        elif 'getDeviceMetadataResponse' in admin_message:
            self.logger.info(f"Received device metadata response: {admin_message['getDeviceMetadataResponse']}")
        elif 'getRingtoneResponse' in admin_message:
            self.logger.debug(f"(This is used for HEALTH CHECK) Received ringtone response: {admin_message['getRingtoneResponse']}")
        else:
            self.logger.warning(f"Received unexpected admin message:\n{admin_message}")

    # --- Helper methods for admin & updates ---

    async def _handle_route_reply(self, admin_message: dict[str, Any], dest_id: str) -> None:
        route = admin_message['getRouteReply'].get('route', [])
        if route:
            route_str = " → ".join(f"!{node:08x}" for node in route)
            traceroute_result = f"🔍 Traceroute to {dest_id}:\n{route_str}"
        else:
            traceroute_result = f"🔍 Traceroute to {dest_id}: No route found"
        await self.telegram.send_message(escape_markdown(traceroute_result, version=2))

    async def _handle_device_metrics(self, node_id: str, device_metrics: dict[str, Any]) -> None:
        self.node_manager.update_node_telemetry(node_id, device_metrics)
        telemetry_info = self.node_manager.get_node_telemetry(node_id)
        if self.reports.get('telemetry', True):
            await self.telegram.send_or_edit_message('telemetry', node_id, telemetry_info)

    async def _handle_position(self, node_id: str, position: dict[str, Any]) -> None:
        self.node_manager.update_node_position(node_id, position)
        position_info = self.node_manager.get_node_position(node_id)
        if self.reports.get('location', True):
            await self.telegram.send_or_edit_message('location', node_id, position_info)

    # Validation helper
    def _valid_node_id(self, node_id: Any) -> bool:  # type: ignore[override]
        if not isinstance(node_id, str):
            return False
        if not node_id or node_id.lower() == 'unknown':
            return False
        if node_id == '^all':  # broadcast
            return True
        if node_id.startswith('!') and len(node_id) == 9:  # ! + 8 hex chars
            return True
        return True  # fallback permissive