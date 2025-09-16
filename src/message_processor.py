from __future__ import annotations

# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportAny=false

import asyncio
import signal
from typing import TypedDict, Literal, Protocol, NotRequired, cast, Any, Dict
import sqlite3
from datetime import datetime, timezone, timedelta
from telegram import Update, LinkPreviewOptions
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown
from meshtastic_interface import MeshtasticInterface
from telegram_interface import TelegramInterface
from config_manager import ConfigManager
from logging_utils import get_logger, StructuredLogger
from node_manager import NodeManager
import re
from logging_utils import new_id

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
    help: bool
    ping: bool

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
        }
        self.forwarding_enabled: bool = config.get('telegram.enable_message_forwarding', False)

        # Message persistence DB (direct sqlite3)
        self._msgdb: sqlite3.Connection | None = None
        try:
            self._msgdb = sqlite3.connect("messages.db")
            # Light tuning for reliability/perf; safe defaults
            try:
                self._msgdb.execute("PRAGMA journal_mode=WAL")
                self._msgdb.execute("PRAGMA synchronous=NORMAL")
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"Failed to open messages.db: {e}", exc_info=True)
            self._msgdb = None

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
                # Suppress noisy health-check ringtone responses
                _is_ringtone = (
                    message.get('decoded', {}).get('portnum') == 'ADMIN_APP' and
                    'getRingtoneResponse' in message.get('decoded', {}).get('admin', {})
                )
                if not _is_ringtone:
                    # Include cached shortName and longName if available
                    from_id = message.get('fromId')
                    to_id = message.get('toId')
                    from_short_name = None
                    from_long_name = None
                    to_short_name = None
                    to_long_name = None
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
                    self.logger.info(
                        "mt_packet_rx", portnum=message.get('decoded', {}).get('portnum'), raw_type=message.get('type'),
                        from_id=from_id, from_sn=from_short_name, from_ln=from_long_name,
                        to_id=to_id, to_sn=to_short_name, to_ln=to_long_name,
                        message_id=message.get('id')
                    )
                # dynamic packet dict access
                self.logger.debug("mt_message_rx", instance=self.instance_id, portnum=message.get('decoded', {}).get('portnum'), from_id=message.get('fromId'))  # type: ignore[arg-type]
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

    # --- Meshtastic Message Handlers ---

    async def handle_meshtastic_message(self, packet: dict[str, object]) -> None:  # type: ignore[override]
        """Handle a non-ACK Meshtastic packet by resolving an app-specific handler."""
        if packet.get('type') == 'ack':  # type: ignore[attr-defined]
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
            message_id_int = int(message_id)  # type: ignore[arg-type]
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
        self.logger.info( "mt_handle_text", from_id=packet.get('fromId'), to_id=packet.get('toId'), channel=packet.get('channel'))
        bridge_id = new_id()
        sender = str(packet.get('fromId', 'unknown'))
        recipient = str(packet.get('toId', 'unknown'))
        decoded = cast(dict, packet.get('decoded', {}))
        payload = decoded.get('payload', b'')
        text: str = payload.decode('utf-8') if isinstance(payload, (bytes, bytearray)) else str(payload)
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
                    signal_emoji = "😃"
                    signal_label = "Excellent"
                elif rssi_val > -90 and snr_val > 2:
                    signal_emoji = "🙂"
                    signal_label = "Good"
                elif rssi_val > -100 and snr_val > -5:
                    signal_emoji = "😐"
                    signal_label = "Fair"
                else:
                    signal_emoji = "😣"
                    signal_label = "Bad"
            elif rssi_val is not None:
                if rssi_val > -80:
                    signal_emoji = "😃"
                    signal_label = "Excellent"
                elif rssi_val > -90:
                    signal_emoji = "🙂"
                    signal_label = "Good"
                elif rssi_val > -100:
                    signal_emoji = "😐"
                    signal_label = "Fair"
                else:
                    signal_emoji = "😣"
                    signal_label = "Bad"
            elif snr_val is not None:
                if snr_val > 8:
                    signal_emoji = "😃"
                    signal_label = "Excellent"
                elif snr_val > 2:
                    signal_emoji = "🙂"
                    signal_label = "Good"
                elif snr_val > -5:
                    signal_emoji = "😐"
                    signal_label = "Fair"
                else:
                    signal_emoji = "😣"
                    signal_label = "Bad"
        except Exception:
            pass

        is_command = None
        # Slash-command dispatch: parse "/command" and delegate to handler if present
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
                    (ts, sender, name_val, long_name_val, text),
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
            message = f"💻 <b>{channel_label} CMD: {is_command} `{sender}` - `{from_short}` - `{from_long}`</b>\n<u>[REPLY]</u>: {text}\n"
        else:
            message = f"💬 <b>{channel_label} `{sender}` - `{from_long}`\n<u>`{from_short}`</u>: </b>{text}\n\n"

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
            safe = "T"
        if safe[0].isdigit():
            safe = f"T_{safe}"
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

    async def handle_telegram_message(self, message: TelegramMessage) -> None:
        """Dispatch a normalized Telegram message to its concrete handler."""
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
            features_str = ", ".join(features)
            await _reply(
                f"Status:\nUptime: {uptime_delta}\nNodes: {node_count}\nReports: {features_str}"
            )
            return
        if command == 'features':
            features_lines = [f"• {k}: {'enabled' if v else 'disabled'}" for k, v in self.reports.items()]
            features_lines.append(f"• forwarding: {'enabled' if self.forwarding_enabled else 'disabled'}")
            await _reply("Features:\n" + "\n".join(features_lines))
            return
        if command in ('enable', 'disable') and args:
            target = args[0].lower()
            if target in self.reports:
                new_val = (command == 'enable')
                self.reports[target] = new_val  # type: ignore[index]
                self.logger.info("tg_cmd_feature_toggle", instance=self.instance_id, feature=target, value=new_val)
                await _reply(f"Feature {target} set to {'enabled' if new_val else 'disabled'}")
            elif target in ('forwarding', 'message_forwarding', 'forward'):
                new_val = (command == 'enable')
                self.forwarding_enabled = new_val
                self.logger.info("tg_cmd_forwarding_toggle", instance=self.instance_id, value=new_val)
                await _reply(f"Forwarding set to {'enabled' if new_val else 'disabled'}")
            else:
                await _reply(f"Unknown feature: {target}")
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
                lines: list[str] = []
                for idx, (node_id, info) in enumerate(nodes_dict.items(), start=1):
                    if not isinstance(info, dict):
                        info = {}
                    sn = info.get('shortName') or 'unknown'
                    ln = info.get('longName') or 'unknown'
                    lines.append(f"{idx}. `{node_id}` - {sn} - {ln}")
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

        enabled_cmds = [name for name, on in self.mesh_commands.items() if on]
        # Always show ping in help (if disabled, mark it accordingly)
        cmds_line = "/ping" + (" (disabled)" if not self.mesh_commands.get('ping', False) else "")
        help_text = (
            "Mesh commands:\n"
            f"• {cmds_line}\n"
            "Use '/ping' to get link stats (hops, RSSI, SNR)."
        )
        self.logger.info(
            "help_command_rx",
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
        self.logger.info("mt_nodeinfo_handle", from_id=raw_id, short_name=node_info.get('user', {}).get('shortName'), long_name=node_info.get('user', {}).get('longName'))

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
        self.logger.info("mt_position_handle", from_id=raw_id, latitude=latitude, longitude=longitude)

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
        self.logger.info("mt_telemetry_handle", from_id=raw_id, device_metrics=device_metrics)

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