from __future__ import annotations

import asyncio
import queue
from typing import Dict, Any, TypedDict, cast, Callable, Awaitable, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from collections import deque
from meshtastic import tcp_interface, serial_interface
from meshtastic.serial_interface import SerialInterface
from meshtastic.tcp_interface import TCPInterface
from pubsub import pub
from config_manager import ConfigManager
from logging_utils import get_logger
from logging_utils import new_id, StructuredLogger
from node_manager import NodeManager
import socket
import os
import errno
from meshtastic.protobuf import telemetry_pb2, portnums_pb2
from meshtastic import BROADCAST_ADDR

MESSAGE_MAX_LEN = 230  # Meshtastic message size hard limit (bytes / characters)
HEALTH_CHECK_INTERVAL_SECONDS = 5
DEFAULT_MAX_SEND_CHUNKS = 5  # Default max number of chunks to send per message
DEFAULT_TRUNCATION_NOTICE_TEMPLATE = "\n...and {omitted} more chunk(s) not sent: message too large"


class DeviceMetrics(TypedDict):
    """Subset of device metrics we rely on for status reporting."""
    batteryLevel: int
    voltage: float
    channelUtilization: float
    airUtilTx: float

class NodeInfo(TypedDict):
    """Structure returned by meshtastic getMyNodeInfo()."""
    user: Dict[str, Any]
    deviceMetrics: DeviceMetrics

"""PendingMessage removed: retry logic is handled by the outgoing queue re-enqueue."""

@dataclass
class OutgoingJob:
    """Represents a queued outbound message split into chunks to send sequentially."""
    chunks: list[str]
    recipient: str
    channel: int | None
    result_fut: Optional[asyncio.Future[int]]
    attempts: int = 0

class MeshtasticInterface:
    config: ConfigManager
    logger: StructuredLogger
    interface: SerialInterface | TCPInterface | None
    message_queue: asyncio.Queue[dict[str, object]]
    thread_safe_queue: queue.Queue[dict[str, object]]
    outgoing_queue: asyncio.Queue[OutgoingJob]
    loop: asyncio.AbstractEventLoop
    last_telemetry: dict[str, object]
    max_retries: int
    retry_interval: int
    node_manager: NodeManager
    is_setup: bool
    is_closing: bool
    my_node_id: str
    my_short_name: str
    send_delay_seconds: float
    max_send_chunks: int
    truncation_notice_template: str
    on_reconnect_storm: Optional[Callable[[], Awaitable[None]]]
    _reconnect_attempts: deque[datetime]
    _reconnect_storm_triggered: bool
    _send_in_progress: asyncio.Event
    # Health-check failure tracking
    health_failures: int
    health_max_failures: int

    def __init__(self, config: ConfigManager, on_reconnect_storm: Optional[Callable[[], Awaitable[None]]] = None) -> None:
        """Initialize interface state but do not connect yet."""
        self.config = config
        # get_logger() returns a StructuredLogger at runtime via configure_logging
        self.logger = cast(StructuredLogger, get_logger(__name__))
        self.instance_id = new_id()
        self.interface = None
        self.message_queue = asyncio.Queue()
        self.thread_safe_queue = queue.Queue()
        self.outgoing_queue = asyncio.Queue()
        self.loop = asyncio.get_running_loop()
        self.last_telemetry = {}
        self.max_retries = 3
        self.retry_interval = 60
        self.node_manager = NodeManager(config)
        self.is_setup = False
        self.is_closing = False
        self.my_node_id = ""
        self.my_short_name = ""
        # Reconnect storm handling
        self.on_reconnect_storm = on_reconnect_storm
        self._reconnect_attempts = deque()
        self._reconnect_storm_triggered = False
        self._send_in_progress = asyncio.Event()
        # Health-check policy: require N consecutive failures before action
        try:
            hmf = self.config.get('meshtastic.health_max_failures', 3)
            hmf = int(hmf) if not isinstance(hmf, int) else hmf
            if hmf < 1:
                hmf = 1
            self.health_max_failures = hmf
        except Exception:
            self.health_max_failures = 3
        self.health_failures = 0
        # Exit strategy on serial disconnect/health failures (configurable)
        # New key: meshtastic.on_disconnect: 'exit' | 'reconnect' (default: 'exit')
        # Back-compat: meshtastic.exit_on_disconnect: bool
        try:
            on_disc = str(self.config.get('meshtastic.on_disconnect', '') or '').strip().lower()
        except Exception:
            on_disc = ''
        if on_disc not in ('exit', 'reconnect'):
            try:
                legacy = self.config.get('meshtastic.exit_on_disconnect', None)
                if isinstance(legacy, bool):
                    on_disc = 'exit' if legacy else 'reconnect'
            except Exception:
                on_disc = ''
        if on_disc not in ('exit', 'reconnect'):
            on_disc = 'exit'
        self.exit_on_disconnect: bool = (on_disc == 'exit')
        # Configurable send delay (ms -> seconds)
        try:
            delay_ms = self.config.get('meshtastic.send_delay_ms', 200)
            if not isinstance(delay_ms, (int, float)):
                delay_ms = 200
            self.send_delay_seconds = float(delay_ms) / 1000.0
        except Exception:
            self.send_delay_seconds = 0.2
        # Configurable max chunks
        try:
            msc = self.config.get('meshtastic.max_send_chunks', DEFAULT_MAX_SEND_CHUNKS)
            msc = int(msc) if not isinstance(msc, int) else msc
            if msc < 1:
                msc = 1
            self.max_send_chunks = msc
        except Exception:
            self.max_send_chunks = DEFAULT_MAX_SEND_CHUNKS
        # Configurable truncation notice template with {omitted}
        try:
            tmpl = self.config.get('meshtastic.truncation_notice_template', DEFAULT_TRUNCATION_NOTICE_TEMPLATE)
            if not isinstance(tmpl, str) or '{omitted}' not in tmpl:
                tmpl = DEFAULT_TRUNCATION_NOTICE_TEMPLATE
            self.truncation_notice_template = tmpl
        except Exception:
            self.truncation_notice_template = DEFAULT_TRUNCATION_NOTICE_TEMPLATE

    async def setup(self) -> None:
        """Create the low-level meshtastic interface and subscribe for packets."""
        # Begin setup (structured event replaces verbose human log)
        self.logger.info("meshtastic_setup_begin", instance=self.instance_id)
        try:
            self.interface = await self._create_interface()
            self.logger.debug(f"Meshtastic interface created:\n{self.interface.myInfo}")
            _ = pub.subscribe(self.on_meshtastic_message, "meshtastic.receive")
            await self._fetch_node_info()
            self.is_setup = True
            self.logger.info("meshtastic_setup_complete", instance=self.instance_id, node_id=self.my_node_id)
        except Exception as e:
            self.logger.error(f"Failed to set up Meshtastic interface: {e=}", exc_info=True)
            self.logger.error("meshtastic_setup_error", instance=self.instance_id, error=str(e))
            raise

    async def _create_interface(self) -> SerialInterface | TCPInterface:
        """Instantiate either a Serial or TCP meshtastic interface based on config."""
        connection_type = cast(str, self.config.get('meshtastic.connection_type', 'serial'))
        device = cast(str, self.config.get('meshtastic.device', ''))
        if not device:
            raise ValueError("Meshtastic device is not configured in the YAML file.")
        
        match connection_type:
            case 'serial':
                return await asyncio.to_thread(serial_interface.SerialInterface, device)
            case 'tcp':
                host, port = device.split(':')
                return await asyncio.to_thread(tcp_interface.TCPInterface, hostname=host, portNumber=int(port))
            case _:
                raise ValueError(f"Unsupported connection type: {connection_type}")

    async def _fetch_node_info(self) -> None:
        """Populate local node id for logging and status reporting."""
        try:
            if not self.interface:
                return
            raw_info = await asyncio.to_thread(self.interface.getMyNodeInfo)
            if not isinstance(raw_info, dict):
                self.logger.error("mt_node_info_type_error", instance=self.instance_id)
                return
            # Keep node manager in sync using the correct id from node_info.user.id
            if isinstance(raw_info, dict):
                node_id = raw_info.get('user', {}).get('id')
                self.logger.info(f"Updating my node info: {node_id} = {raw_info}")
                self.node_manager.remove_node(node_id)
                self.node_manager.update_node( node_id, cast(Dict[str, Any], raw_info.get('user', {})))
            user_part = raw_info.get('user')
            node_id = user_part.get('id') if isinstance(user_part, dict) else None
            self.my_node_id = node_id if isinstance(node_id, str) else ""
            # Derive short name (max 4 bytes) for prefixing outbound chunks
            try:
                sn = user_part.get('shortName') if isinstance(user_part, dict) else None
                if isinstance(sn, str) and sn.strip():
                    self.my_short_name = self._normalize_shortname(sn.strip())
                else:
                    fallback = (self.my_node_id or "").replace('!', '')
                    self.my_short_name = self._normalize_shortname(fallback[:4] if fallback else "BOT")
            except Exception:
                fallback = (self.my_node_id or "").replace('!', '')
                self.my_short_name = self._normalize_shortname(fallback[:4] if fallback else "BOT")
            if self.my_node_id:
                self.logger.info("mt_node_info_received", instance=self.instance_id, node_id=self.my_node_id)
                # self.logger.info(f"Node info: {raw_info}")
            else:
                self.logger.error("mt_node_info_missing_id", instance=self.instance_id)
        except Exception as e:
            self.logger.error(f"Failed to get node info: {e=}", exc_info=True)
            self.logger.error("mt_node_info_error", instance=self.instance_id, error=str(e))

    def on_meshtastic_message(self, packet: dict[str, object], interface: object | None = None, **_extra: object) -> None:
        """PubSub callback when a Meshtastic packet arrives (thread context).

        The pubsub publisher sends keyword args `packet` and `interface`; we make
        `interface` optional and accept **_extra to stay resilient to library changes.
        """
        self.logger.debug(
            f"Message details - {packet.get('fromId')=}, {packet.get('toId')=}, {packet.get('decoded', {}).get('portnum')=}"
        )
        try:
            from_id = str(packet.get('fromId', '') or '')
            to_id = str(packet.get('toId', '') or '')
            try:
                f_node = self.node_manager.get_node(from_id)
                from_sn = f_node.get('shortName') if f_node else None
                from_ln = f_node.get('longName') if f_node else None
            except Exception:
                from_sn = from_ln = None
            try:
                t_node = self.node_manager.get_node(to_id)
                to_sn = t_node.get('shortName') if t_node else None
                to_ln = t_node.get('longName') if t_node else None
            except Exception:
                to_sn = to_ln = None
            portnum = packet.get('decoded', {}).get('portnum')
            self.logger.debug(
                "packet_rx",
                instance=self.instance_id,
                from_id=from_id, from_sn=from_sn, from_ln=from_ln,
                to_id=to_id, to_sn=to_sn, to_ln=to_ln,
                portnum=str(portnum),
                is_ack=packet.get('decoded', {}).get('portnum') == 'ROUTING_APP'
            )
        except Exception:
            pass
        if packet.get('decoded', {}).get('portnum') == 'ROUTING_APP':
            self.handle_ack(packet)
        else:
            self.thread_safe_queue.put(packet)

    def handle_ack(self, packet: dict[str, object]) -> None:
        """Convert ACK packets into simplified dicts and enqueue them in the async queue."""
        try:
            from_id = str(packet.get('fromId', '') or '')
            to_id = str(packet.get('toId', '') or '')
            try:
                f_node = self.node_manager.get_node(from_id)
                from_sn = f_node.get('shortName') if f_node else None
                from_ln = f_node.get('longName') if f_node else None
            except Exception:
                from_sn = from_ln = None
            try:
                t_node = self.node_manager.get_node(to_id)
                to_sn = t_node.get('shortName') if t_node else None
                to_ln = t_node.get('longName') if t_node else None
            except Exception:
                to_sn = to_ln = None
            self.logger.debug(
                "mt_ack_rx",
                instance=self.instance_id,
                from_id=from_id, from_sn=from_sn, from_ln=from_ln,
                to_id=to_id, to_sn=to_sn, to_ln=to_ln,
                message_id=str(packet.get('id', '')),
                request_id=str(packet.get('decoded', {}).get('requestId', ''))
            )
        except Exception:
            pass
        self.loop.call_soon_threadsafe(
            self.message_queue.put_nowait,
            {
                'type': 'ack',
                'from': packet.get('fromId'),
                'to': packet.get('toId'),
                'message_id': packet.get('id'),
                'request_id': packet.get('decoded', {}).get('requestId'),
            }
        )

    def _require_interface(self) -> SerialInterface | TCPInterface:
        """Return the underlying interface or raise a RuntimeError (callers log already)."""
        if not self.interface:
            raise RuntimeError("Interface not ready")
        return self.interface

    async def send_reaction(self, emoji: str, message_id: str) -> None:
        """Send a reaction to a previously sent message if supported by firmware."""
        try:
            iface = self._require_interface()
            await asyncio.to_thread(iface.sendReaction, emoji, messageId=message_id)  # type: ignore[attr-defined]
            self.logger.info(f"Reaction {emoji} sent for message {message_id}")
            self.logger.info("mt_reaction_sent", instance=self.instance_id, emoji=emoji, message_id=message_id)
        except Exception as e:
            self.logger.error(f"Error sending reaction to Meshtastic: {e=}", exc_info=True)
            self.logger.error("mt_reaction_error", instance=self.instance_id, emoji=emoji, message_id=message_id, error=str(e))

    async def send_message(self, text: str, recipient: str, channel: int | None = None, *, sender_shortname: str | None = None) -> int:
        """Split the message into chunks and enqueue them for paced sending.

        A background worker sends each chunk sequentially with a configurable
        delay (meshtastic.send_delay_ms, default 200ms). Returns the first
        chunk's message id, or -1 if the first attempt fails.
        """
        if not text or not recipient:
            raise ValueError("Text and recipient must not be empty")
        # Log based on bytes for accuracy
        try:
            try:
                _n = self.node_manager.get_node(recipient)
                _rsn = _n.get('shortName') if _n else None
                _rln = _n.get('longName') if _n else None
            except Exception:
                _rsn = _rln = None
            self.logger.info(
                "mt_send_attempt",
                instance=self.instance_id,
                recipient=recipient,
                recipient_sn=_rsn,
                recipient_ln=_rln,
                channel=channel,
                size=len(text.encode('utf-8')),
                text=text,
            )
        except Exception:
            pass

        # Split into chunks; include an optional requester shortname prefix (max 4 bytes)
        prefix = self._make_short_prefix(sender_shortname) if sender_shortname else ""
        chunks, full_total, omitted = self._build_chunks_with_prefix_suffix(text, MESSAGE_MAX_LEN, prefix=prefix, max_chunks=self.max_send_chunks)
        # Log chunking plan to help diagnose suffix behavior
        try:
            total = len(chunks)
            if total > 1:
                preview = [(i + 1, len(c.encode('utf-8'))) for i, c in enumerate(chunks)]
                self.logger.info("mt_chunk_plan", instance=self.instance_id, recipient=recipient, channel=channel, total_chunks=total, full_total=full_total, omitted=omitted, sizes=preview)
            else:
                self.logger.info("mt_chunk_plan", instance=self.instance_id, recipient=recipient, channel=channel, total_chunks=total, full_total=full_total, omitted=omitted, sizes=[len(chunks[0].encode('utf-8')) if chunks else 0])
        except Exception:
            pass
        # Enqueue job and await first chunk result
        fut: asyncio.Future[int] = self.loop.create_future()
        await self.outgoing_queue.put(OutgoingJob(chunks=chunks, recipient=recipient, channel=channel, result_fut=fut, attempts=0))
        return await fut

    async def send_short_message(self, text: str, recipient: str, channel: int | None = 0) -> int:
        """Send a single short text packet (no chunking), enforcing 200-byte UTF-8 limit.

        Returns message id or -1 on failure. Intended for brief system notices and BBS payloads.
        """
        if not text or not recipient:
            raise ValueError("Text and recipient must not be empty")
        encoded = text.encode('utf-8')
        if len(encoded) > 200:
            raise ValueError("Short message exceeds 200-byte limit")
        try:
            iface = self._require_interface()
            if channel is None:
                result = await asyncio.to_thread(iface.sendText, text, destinationId=recipient)  # type: ignore[attr-defined]
            else:
                result = await asyncio.to_thread(iface.sendText, text, destinationId=recipient, channelIndex=int(channel))  # type: ignore[attr-defined]
            msg_id = getattr(result, 'id', None)
            try:
                _n = self.node_manager.get_node(recipient)
                _rsn = _n.get('shortName') if _n else None
                _rln = _n.get('longName') if _n else None
            except Exception:
                _rsn = _rln = None
            self.logger.info("mt_send_short", instance=self.instance_id, recipient=recipient, recipient_sn=_rsn, recipient_ln=_rln, channel=channel, message_id=msg_id)
            return int(msg_id) if isinstance(msg_id, int) else -1
        except Exception as e:
            try:
                self.logger.error("mt_send_short_error", instance=self.instance_id, recipient=recipient, channel=channel, error=str(e))
            except Exception:
                pass
            return -1

    async def process_outgoing_messages(self) -> None:
        """Background worker: send queued chunked messages with configured delay."""
        while True:
            job = await self.outgoing_queue.get()
            try:
                # Mark sending as active for health-check gating
                self._send_in_progress.set()
                recipient = job.recipient
                channel = job.channel
                explicit_channel = channel is not None
                if channel is None:
                    # Prefer top-level default_channel_id, fallback to legacy meshtastic.default_channel_id
                    channel = self.config.get('default_channel_id', self.config.get('meshtastic.default_channel_id', 0))
                    if not isinstance(channel, int):
                        channel = 0
                iface = self._require_interface()

                first_id: int | None = None
                total_chunks = len(job.chunks)
                for idx, chunk in enumerate(job.chunks, start=1):
                    try:
                        if explicit_channel:
                            result = await asyncio.to_thread(iface.sendText, chunk, destinationId=recipient, channelIndex=int(channel))  # type: ignore[attr-defined]
                        else:
                            result = await asyncio.to_thread(iface.sendText, chunk, destinationId=recipient)  # type: ignore[attr-defined]
                        if first_id is None:
                            first_id = getattr(result, 'id', None)
                        try:
                            try:
                                _n = self.node_manager.get_node(recipient)
                                _rsn = _n.get('shortName') if _n else None
                                _rln = _n.get('longName') if _n else None
                            except Exception:
                                _rsn = _rln = None
                            self.logger.info("mt_send_success", instance=self.instance_id, recipient=recipient, recipient_sn=_rsn, recipient_ln=_rln, channel=channel, message_id=getattr(result, 'id', None))
                            # Check whether the suffix is present for diagnostics
                            # Suffix detection tolerant to truncated totals
                            has_suffix = "\nMSG " in chunk
                            self.logger.info(
                                "mt_send_chunk",
                                instance=self.instance_id,
                                chunk_index=idx,
                                total_chunks=total_chunks,
                                bytes=len(chunk.encode('utf-8')),
                                has_suffix=has_suffix,
                                text=chunk,
                            )
                        except Exception:
                            pass
                    except Exception as e:
                        # Resolve first future on failure of first chunk
                        try:
                            self.logger.error(f"Error sending chunk to Meshtastic: {e=}", exc_info=True)
                            try:
                                _n = self.node_manager.get_node(recipient)
                                _rsn = _n.get('shortName') if _n else None
                                _rln = _n.get('longName') if _n else None
                            except Exception:
                                _rsn = _rln = None
                            self.logger.error("mt_send_failure", instance=self.instance_id, recipient=recipient, recipient_sn=_rsn, recipient_ln=_rln, channel=channel, error=str(e))
                        except Exception:
                            pass
                        if idx == 1 and job.result_fut is not None and not job.result_fut.done():
                            job.result_fut.set_result(-1)
                        # For non-size errors, schedule a full-message retry via pending_messages
                        if "Data payload too big" in str(e):
                            try:
                                self.logger.error("mt_send_too_big", instance=self.instance_id, recipient=recipient, channel=channel)
                            except Exception:
                                pass
                            break
                        else:
                            # Re-enqueue the same job with attempts+1 after retry_interval if under max_retries
                            if job.attempts < self.max_retries:
                                async def _requeue() -> None:
                                    try:
                                        await asyncio.sleep(self.retry_interval)
                                        await self.outgoing_queue.put(OutgoingJob(
                                            chunks=job.chunks,
                                            recipient=recipient,
                                            channel=job.channel,
                                            result_fut=None,
                                            attempts=job.attempts + 1,
                                        ))
                                        try:
                                            self.logger.debug("mt_retry_scheduled", instance=self.instance_id, recipient=recipient, attempts=job.attempts + 1)
                                        except Exception:
                                            pass
                                    except Exception:
                                        pass
                                self.loop.create_task(_requeue())
                            else:
                                try:
                                    self.logger.error("mt_retry_giveup", instance=self.instance_id, recipient=recipient)
                                except Exception:
                                    pass
                            break

                    # Delay between chunks, except after the last one
                    if idx < total_chunks:
                        await asyncio.sleep(self.send_delay_seconds)

                # Resolve the future once we’ve attempted the first chunk
                if job.result_fut is not None and not job.result_fut.done():
                    job.result_fut.set_result(first_id if isinstance(first_id, int) else -1)
            finally:
                self.outgoing_queue.task_done()
                # If queue drained, clear sending flag; otherwise next loop keeps it set
                if self.outgoing_queue.empty():
                    self._send_in_progress.clear()

    def _split_utf8_bytes(self, s: str, limit: int) -> list[str]:
        """Split string into chunks not exceeding `limit` UTF-8 bytes.

        Preference: split on the last newline before the limit when possible,
        otherwise the last space. Always preserve UTF-8 code point boundaries.
        """
        if len(s.encode('utf-8')) <= limit:
            return [s]
        chunks: list[str] = []
        start = 0
        byte_count = 0
        last_nl_index = -1
        last_sp_index = -1
        for i, ch in enumerate(s):
            b = len(ch.encode('utf-8'))
            if ch == '\n':
                last_nl_index = i
                last_sp_index = i
            elif ch == ' ':
                last_sp_index = i
            if byte_count + b > limit:
                if last_nl_index >= start:
                    cut = last_nl_index + 1
                elif last_sp_index >= start:
                    cut = last_sp_index + 1
                else:
                    cut = i
                chunks.append(s[start:cut])
                start = cut
                byte_count = 0
                last_nl_index = -1
                last_sp_index = -1
            byte_count += b
        if start < len(s):
            chunks.append(s[start:])
        return chunks

    def _split_utf8_bytes_reserve_suffix(self, s: str, hard_limit: int, *, prefix: str = "") -> list[str]:
        """Split `s` into UTF-8 chunks so that `chunk + suffix` fits `hard_limit`.

        The suffix format is " [MSG i of N]". Since N is unknown up front, we do a
        two-pass approach:
        1) First, split optimistically with a conservative reserved budget
           large enough for worst-case digits (assume up to 9999 chunks).
        2) After counting chunks, we re-split only if the calculated suffix
           for the actual N would overflow; to keep it simple and fast, we
           use the conservative reservation for all chunks.

        This keeps logic simple and avoids per-chunk reflow.
        """
        # Normalized sizes
        prefix_b = prefix.encode('utf-8') if prefix else b""
        prefix_len = len(prefix_b)
        # Worst-case suffix when N <= 9999: " [MSG 9999 of 9999]" -> 19 bytes (ASCII)
        reserved_suffix = 19
        limit = max(1, hard_limit - reserved_suffix - prefix_len)

        # If it fits in one without suffix, return single prefixed chunk
        if len((prefix + s).encode('utf-8')) <= hard_limit:
            return [prefix + s]

        # First pass: split with conservative reservation for prefix+suffix budget
        base_chunks = self._split_utf8_bytes(s, limit)
        # Multiple chunks: append actual suffix using true N and ensure prefix+chunk+suffix fits
        total = len(base_chunks)
        out: list[str] = []
        for idx, chunk in enumerate(base_chunks, start=1):
            suffix = f" [MSG {idx} of {total}]"
            # Trim if prefix + chunk + suffix would overflow
            while len((prefix + chunk + suffix).encode('utf-8')) > hard_limit and chunk:
                chunk = chunk[:-1]
            out.append(prefix + chunk + suffix)
        return out

    def _build_chunks_with_prefix_suffix(self, s: str, hard_limit: int, *, prefix: str = "", max_chunks: int = DEFAULT_MAX_SEND_CHUNKS) -> tuple[list[str], int, int]:
        """Produce chunks with prefix and MSG suffix, capping the number of sent chunks.

        Returns (chunks_to_send, full_total_chunks, omitted_count).
        If omitted_count > 0, the last chunk_to_send contains an extra explanatory line.
        """
        all_chunks = self._split_utf8_bytes_reserve_suffix(s, hard_limit, prefix=prefix)
        full_total = len(all_chunks)
        if full_total <= max_chunks:
            return all_chunks, full_total, 0

        # We need to send at most max_chunks. We'll send the first max_chunks-1 normal chunks,
        # and make the last chunk an explanatory notice.
        send_count = max(1, max_chunks - 1)
        chunks_to_send = all_chunks[:send_count]
        omitted = full_total - send_count
        # Build notice from template
        try:
            notice = self.truncation_notice_template.format(omitted=omitted)
        except Exception:
            notice = DEFAULT_TRUNCATION_NOTICE_TEMPLATE.format(omitted=omitted)
        # Build a final notice chunk (with same prefix), ensure it fits by trimming if needed
        final_chunk = (prefix + notice) if prefix else notice
        # Trim if needed to fit within hard_limit
        while len(final_chunk.encode('utf-8')) > hard_limit and len(notice) > 0:
            notice = notice[:-1]
            final_chunk = (prefix + notice) if prefix else notice
        chunks_to_send.append(final_chunk)
        return chunks_to_send, full_total, omitted

    def _normalize_shortname(self, s: str) -> str:
        """Return a string trimmed so its UTF-8 byte length is at most 4."""
        if not s:
            return ""
        out = []
        total = 0
        for ch in s:
            b = len(ch.encode('utf-8'))
            if total + b > 4:
                break
            out.append(ch)
            total += b
        return "".join(out)

    def _make_short_prefix(self, shortname: str | None = None) -> str:
        """Build the per-chunk prefix like "SN: ".

        If a shortname is provided (e.g., requester), normalize and use it; otherwise
        fall back to this node's my_short_name. Returns empty string when unavailable.
        """
        if shortname is not None and isinstance(shortname, str) and shortname.strip():
            sn = self._normalize_shortname(shortname.strip())
        else:
            sn = self.my_short_name.strip() if isinstance(self.my_short_name, str) else ""
        if not sn:
            return ""
        return f"{sn}: "

    async def send_bell(self, dest_id: str) -> int:
        """Send a bell (notification) to a specific destination node id."""
        if not dest_id:
            raise ValueError("Destination ID must not be empty")
        self.logger.info("mt_bell_attempt", instance=self.instance_id, dest_id=dest_id)
        try:
            iface = self._require_interface()
            result = await asyncio.to_thread(iface.sendText, "🔔", destinationId=dest_id)  # type: ignore[attr-defined]
            self.logger.info("mt_bell_success", instance=self.instance_id, dest_id=dest_id, message_id=getattr(result, 'id', None))
            return result.id  # Return the message ID for tracking
        except Exception as e:
            self.logger.error(f"Error sending bell to node {dest_id}: {e}", exc_info=True)
            self.logger.error("mt_bell_error", instance=self.instance_id, dest_id=dest_id, error=str(e))
            raise

    # process_pending_messages removed; retries handled by outgoing_queue re-enqueue

    async def process_thread_safe_queue(self) -> None:
        """Drain thread-safe queue (from callback thread) into async queue."""
        while True:
            try:
                packet = self.thread_safe_queue.get_nowait()
                await self.message_queue.put(packet)
                try:
                    self.logger.debug("enqueue_packet", instance=self.instance_id)
                except Exception:
                    pass
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def get_status(self) -> str:
        """Return a human-friendly node status summary."""
        if not self.interface:
            return "Meshtastic interface not connected"
        try:
            if not self.interface:
                return "Meshtastic interface not connected"
            node_info = await asyncio.to_thread(self.interface.getMyNodeInfo)
            battery_level = node_info.get('deviceMetrics', {}).get('batteryLevel', 'N/A')
            battery_str = "PWR" if battery_level == 101 else f"{battery_level}%"
            air_util_tx = node_info.get('deviceMetrics', {}).get('airUtilTx', 'N/A')
            air_util_tx_str = f"{air_util_tx:.2f}%" if isinstance(air_util_tx, (int, float)) else air_util_tx
            return (
                f"Node: {node_info.get('user', {}).get('longName', 'N/A')}\n"
                f"Battery: {battery_str}\n"
                f"Air Utilization TX: {air_util_tx_str}"
            )
        except Exception as e:
            self.logger.error(f"Error getting meshtastic status: {e}", exc_info=True)
            return f"Error getting meshtastic status: {e}"

    async def close(self) -> None:
        """Close interface and unsubscribe. Safe to call multiple times."""
        if self.is_closing:
            self.logger.info("Meshtastic interface is already closing, skipping.")
            return

        self.is_closing = True
        if not self.is_setup:
            self.logger.info("Meshtastic interface was not set up, skipping close.")
            return
        try:
            if self.interface:
                await asyncio.to_thread(self.interface.close)
            pub.unsubscribe(self.on_meshtastic_message, "meshtastic.receive")
        except Exception as e:
            self.logger.error(f"Error closing Meshtastic interface: {e}", exc_info=True)
            self.logger.error("meshtastic_close_error", instance=self.instance_id, error=str(e))
        finally:
            self.is_setup = False
            self.is_closing = False
            self.logger.info("Meshtastic interface closed.")
            self.logger.info("meshtastic_closed", instance=self.instance_id)

    async def reconnect(self) -> None:
        """Attempt to recreate the underlying interface."""
        self.logger.info("Attempting to reconnect to Meshtastic...")
        # Register this reconnect attempt and check for storm conditions
        await self._register_reconnect_attempt()
        try:
            if self.interface:
                await asyncio.to_thread(self.interface.close)
            self.interface = await self._create_interface()
            self.logger.info("Reconnected to Meshtastic successfully.")
        except Exception as e:
            self.logger.error(f"Failed to reconnect to Meshtastic: {e}", exc_info=True)
            # If the serial device is missing, exit immediately (no point retrying)
            try:
                device = cast(str, self.config.get('meshtastic.device', ''))
            except Exception:
                device = ''
            en = getattr(e, 'errno', None)
            msg = str(e)
            is_no_device = (
                isinstance(e, FileNotFoundError) or
                (isinstance(e, OSError) and en in (errno.ENODEV, errno.ENOENT)) or
                ('No such device' in msg) or
                ('No such file or directory' in msg)
            )
            # Also treat locked/temporarily unavailable serial port as fatal (someone else holds it)
            is_locked = (
                (isinstance(e, OSError) and en in (errno.EBUSY, errno.EAGAIN)) or
                ('Resource temporarily unavailable' in msg) or
                ('device or resource busy' in msg.lower()) or
                ('Could not exclusively lock port' in msg)
            )
            if is_no_device:
                try:
                    self.logger.error(
                        "mt_serial_device_missing",
                        instance=self.instance_id,
                        device=device,
                        errno=en,
                    )
                except Exception:
                    pass
                if self.on_reconnect_storm is not None:
                    try:
                        await self.on_reconnect_storm()
                        return
                    except Exception:
                        pass
                # Fallback: hard-exit
                os._exit(1)
            elif is_locked:
                try:
                    self.logger.error(
                        "mt_serial_device_locked",
                        instance=self.instance_id,
                        device=device,
                        errno=en,
                    )
                except Exception:
                    pass
                if self.on_reconnect_storm is not None:
                    try:
                        await self.on_reconnect_storm()
                        return
                    except Exception:
                        pass
                os._exit(1)

    async def _register_reconnect_attempt(self) -> None:
        """Record a reconnect attempt and trigger shutdown if storm threshold exceeded.

        If more than 5 attempts occur within a 60-second rolling window, we call
        the provided on_reconnect_storm callback (once) to shut the app down.
        """
        now = datetime.now()
        self._reconnect_attempts.append(now)
        # Prune entries older than 60 seconds
        one_minute_ago = now - timedelta(seconds=60)
        while self._reconnect_attempts and self._reconnect_attempts[0] < one_minute_ago:
            self._reconnect_attempts.popleft()

        count = len(self._reconnect_attempts)
        try:
            self.logger.info(
                "mt_reconnect_attempt",
                instance=self.instance_id,
                attempts_last_min=count,
            )
        except Exception:
            pass

        if count > 5 and not self._reconnect_storm_triggered:
            self._reconnect_storm_triggered = True
            try:
                self.logger.error(
                    "mt_reconnect_storm",
                    instance=self.instance_id,
                    attempts_last_min=count,
                )
            except Exception:
                pass
            # Trigger app shutdown if callback provided
            if self.on_reconnect_storm is not None:
                try:
                    # Call it in a task to avoid reentrancy issues and return immediately
                    async def _do_shutdown():
                        try:
                            self.logger.error(
                                "shutdown_requested_by_reconnect_storm",
                                instance=self.instance_id,
                                attempts_last_min=count,
                            )
                        except Exception:
                            pass
                        cb = self.on_reconnect_storm
                        if cb is not None:
                            await cb()

                    self.loop.create_task(_do_shutdown())
                    return
                except Exception:
                    # Best-effort; don't await directly to avoid recursive cancellation
                    pass

    def getNodeInfo(self):
        """Lightweight health probe: use ringtone call only.

        getMyNodeInfo() is not a reliable health indicator (can return stale
        data). The ringtone probe exercises the control path and fails when the
        link is unhealthy.
        """
        try:
            # If this call doesn't raise, consider the link healthy
            self.interface.localNode.get_ringtone()  # type: ignore[attr-defined]
            try:
                self.logger.debug("mt_health_probe_ringtone_ok", instance=self.instance_id)
            except Exception:
                pass
            return "OK"
        except (socket.error, BrokenPipeError, ConnectionResetError, Exception) as e:
            try:
                self.logger.error(f"mt_health_probe_ringtone_error: {e}")
            except Exception:
                pass
            raise e  # Propagate the error to handle reconnection

    async def periodic_health_check(self) -> None:
        """Continuously verify interface health and auto-reconnect if needed."""
        while True:
            # Skip health checks while we are actively sending or have queued messages
            if self._send_in_progress.is_set() or not self.outgoing_queue.empty():
                try:
                    self.logger.warning("mt_health_skip_busy", instance=self.instance_id, queue_size=self.outgoing_queue.qsize())
                except Exception:
                    pass
                await asyncio.sleep(1)
                continue
            # self.logger.debug("Performing periodic health check...")
            self.logger.debug("mt_health_check", instance=self.instance_id)
            if self.interface is None:
                self.logger.warning("Meshtastic interface is not initialized, attempting to reconnect...")
                self.logger.warning("mt_health_missing_interface", instance=self.instance_id)
                # Treat missing interface as a health failure
                self.health_failures += 1
                try:
                    self.logger.warning("mt_health_fail_inc", instance=self.instance_id, failures=self.health_failures, threshold=self.health_max_failures)
                except Exception:
                    pass
                if self.health_failures >= self.health_max_failures:
                    await self.reconnect()
                    # After taking action, reset the counter so we require fresh failures
                    self.health_failures = 0
                continue
            try:
                info = await asyncio.wait_for(asyncio.to_thread(self.getNodeInfo), timeout=5)
                if info == -1 or info is None:
                    self.logger.error("Health check failed: Invalid or no node info received.")
                    self.logger.error("mt_health_invalid", instance=self.instance_id)
                    self.health_failures += 1
                    try:
                        self.logger.warning("mt_health_fail_inc", instance=self.instance_id, failures=self.health_failures, threshold=self.health_max_failures)
                    except Exception:
                        pass
                    if self.health_failures >= self.health_max_failures:
                        if self.exit_on_disconnect:
                            try:
                                self.logger.error("mt_health_fatal_exit", instance=self.instance_id)
                            except Exception:
                                pass
                            if self.on_reconnect_storm is not None:
                                try:
                                    await self.on_reconnect_storm()
                                    return
                                except Exception:
                                    pass
                            os._exit(1)
                        await self.reconnect()
                        self.health_failures = 0
                    continue
                self.logger.debug(f"Health check = {info}")
                self.logger.info("mt_health_ok", instance=self.instance_id)
                # success: reset consecutive failure counter
                if self.health_failures:
                    self.health_failures = 0
            except Exception as e:
                if isinstance(e, TimeoutError):
                    self.logger.error("Health check failed: Timeout while retrieving node info.")
                    self.logger.error("mt_health_timeout", instance=self.instance_id)
                else:
                    self.logger.error(f"Health check failed: {e}", exc_info=True)
                    self.logger.error("mt_health_error", instance=self.instance_id, error=str(e))
                # count consecutive failures and only act after threshold
                self.health_failures += 1
                try:
                    self.logger.warning("mt_health_fail_inc", instance=self.instance_id, failures=self.health_failures, threshold=self.health_max_failures)
                except Exception:
                    pass
                if self.health_failures >= self.health_max_failures:
                    if self.exit_on_disconnect:
                        try:
                            self.logger.error("mt_health_fatal_exit", instance=self.instance_id)
                        except Exception:
                            pass
                        if self.on_reconnect_storm is not None:
                            try:
                                await self.on_reconnect_storm()
                                return
                            except Exception:
                                pass
                        os._exit(1)
                    await self.reconnect()
                    # reset failures after we take recovery action
                    self.health_failures = 0
            await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECONDS)  # Check periodically

    async def periodic_telemetry_report(self) -> None:
        """Run external script for environment metrics and forward to mesh periodically."""
        telemetry_config = self.config.get('telemetry', {})
        if not telemetry_config.get('environment_enabled', False):
            self.logger.info("mt_telemetry_disabled", instance=self.instance_id)
            return

        script_path = self.config.get('telemetry', {}).get('environment_script', 'echo')
        interval = self.config.get('telemetry', {}).get('environment_send_interval', 300)

        while True:
            self.logger.debug("Running environment telemetry script...")
            self.logger.debug("mt_telemetry_script_start", instance=self.instance_id, script=script_path)
            try:
                process = await asyncio.create_subprocess_shell(
                    script_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()

                if process.returncode != 0:
                    self.logger.error(f"Telemetry script error (code {process.returncode}): {stderr.decode().strip()}")
                    self.logger.error("mt_telemetry_script_error", instance=self.instance_id, code=process.returncode)
                else:
                    output = stdout.decode().strip()
                    self.logger.debug(f"Telemetry script output:\n{output}")
                    t = telemetry_pb2.Telemetry()
                    for line in output.splitlines():
                        if ':' in line:
                            key, value = line.split(':', 1)
                            key = key.strip()
                            value = value.strip()
                            if hasattr(t.environment_metrics, key):
                                try:
                                    cur_val = getattr(t.environment_metrics, key)
                                    if isinstance(cur_val, float):
                                        setattr(t.environment_metrics, key, float(value))
                                    elif isinstance(cur_val, int):
                                        setattr(t.environment_metrics, key, int(float(value)))
                                    else:
                                        self.logger.warning(f"Unsupported telemetry field type for {key}: {type(cur_val)}")
                                except ValueError as ve:
                                    self.logger.error(f"Invalid value for {key}: {value} ({ve})")
                                    self.logger.warning("mt_telemetry_parse_error", instance=self.instance_id, field=key)
                            else:
                                self.logger.warning(f"Unknown telemetry field: {key}")
                                self.logger.warning("mt_telemetry_unknown_field", instance=self.instance_id, field=key)
                    # Structured publish event (mt_telemetry_publish) emitted below
                    self.logger.debug(f"Telemetry Data:\n{t}")
                    if self.interface:
                        self.interface.sendData(t, BROADCAST_ADDR, portnums_pb2.PortNum.TELEMETRY_APP)  # type: ignore[attr-defined]
                        self.logger.info("mt_telemetry_publish", instance=self.instance_id)
            except Exception as e:
                self.logger.error(f"Error running telemetry script: {e}", exc_info=True)
                self.logger.error("mt_telemetry_script_exception", instance=self.instance_id, error=str(e))

            await asyncio.sleep(interval)

    async def reboot_node(self) -> None:
        """Restart the local node via the Meshtastic interface, if supported."""
        if not self.interface:
            self.logger.error("Cannot reboot node: interface not initialized.")
            return
        try:
            # local_node = getattr(self.interface, "localNode", None)
            # restart_fn = getattr(local_node, "reboot", None)
            # await asyncio.to_thread(self.interface.localNode.reboot(5))  # type: ignore[attr-defined]
            self.logger.info("mt_node_reboot", instance=self.instance_id, node_id=self.my_node_id)
            self.interface.localNode.reboot(5)  # type: ignore[attr-defined]
            self.logger.info("Local node reboot command issued. Reboot will happen in 5 seconds.")
        except Exception as e:
            self.logger.error(f"Error rebooting local node: {e}", exc_info=True)
            self.logger.error("mt_node_reboot_error", instance=self.instance_id, error=str(e))

    async def request_nodeinfo(self, node_id: str) -> None:
        """Best-effort request to retrieve metadata for a remote node.

        Tries library APIs if available; falls back to a harmless admin action
        that may elicit routing/admin replies. Fully defensive: no exception leakage.
        """
        # Do not proceed during shutdown
        if self.is_closing:
            return
        try:
            if not self.interface or not node_id:
                return
            # Preferred: explicitly request NODEINFO from the target node using NODEINFO_APP.
            # Some firmware responds to an empty payload. Use destinationId string (e.g. "!abcd1234").
            def _send_nodeinfo(iface, dest: str, want_ack: bool):
                try:
                    self.logger.info(f"Sending NODEINFO request to {dest} (wantAck={want_ack}) code 1")
                    return iface.sendData(  # type: ignore[attr-defined]
                        b"",
                        dest,
                        portNum=portnums_pb2.PortNum.NODEINFO_APP,  # type: ignore[arg-type]
                        wantAck=want_ack,  # type: ignore[arg-type]
                    )
                except TypeError:
                    # Older signatures: no keywords
                    if want_ack:
                        self.logger.info(f"Sending NODEINFO request to {dest} (wantAck={want_ack}) code 2")
                        return iface.sendData(b"", dest, portnums_pb2.PortNum.NODEINFO_APP, True)  # type: ignore[attr-defined]
                    self.logger.info(f"Sending NODEINFO request to {dest} (wantAck={want_ack}) code 3")
                    return iface.sendData(b"", dest, portnums_pb2.PortNum.NODEINFO_APP)  # type: ignore[attr-defined]

            # Direct to node (wantAck)
            self.logger.info(f"Sending NODEINFO request to {node_id} (wantAck=True) code 4")
            try:
                await asyncio.to_thread(_send_nodeinfo, self.interface, node_id, True)
            except BaseException as be:  # Catch SystemExit from library code
                self.logger.warning(f"NODEINFO request thread failed for {node_id}: {be}")
            # Direct to node (no-ack)
            try:
                await asyncio.sleep(0.25)
                await asyncio.to_thread(_send_nodeinfo, self.interface, node_id, False)
            except BaseException as be:
                self.logger.debug(f"NODEINFO (no-ack) send failed for {node_id}: {be}")
            # Broadcast prompt
            try:
                def _send_broadcast(iface):
                    try:
                        self.logger.info("Sending NODEINFO broadcast request code 5")
                        return iface.sendData(b"", BROADCAST_ADDR, portNum=portnums_pb2.PortNum.NODEINFO_APP)  # type: ignore[attr-defined]
                    except TypeError:
                        self.logger.info("Sending NODEINFO broadcast request code 6")
                        return iface.sendData(b"", BROADCAST_ADDR, portnums_pb2.PortNum.NODEINFO_APP)  # type: ignore[attr-defined]
                await asyncio.sleep(0.5)
                await asyncio.to_thread(_send_broadcast, self.interface)
                self.logger.debug("NODEINFO broadcast prompt sent")
            except BaseException as be:
                self.logger.debug(f"NODEINFO broadcast send failed: {be}")
            # Fallback: attempt a traceroute which can prompt admin traffic
            try:
                ln = getattr(self.interface, 'localNode', None)
                traceroute = getattr(ln, 'traceroute', None)
                if callable(traceroute):
                    try:
                        await asyncio.to_thread(traceroute, node_id)
                    except BaseException as be:
                        self.logger.warning(f"traceroute failed for {node_id}: {be}")
                        return
                    self.logger.info(f"Traceroute issued to prompt info for {node_id}")
            except Exception:
                pass
        except BaseException as e:  # pragma: no cover - defensive
            self.logger.warning(f"request_nodeinfo failed for {node_id}: {e}")
