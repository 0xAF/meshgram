from __future__ import annotations

# pyright: reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false, reportUnknownParameterType=false, reportAny=false

import asyncio
from typing import TypedDict, Literal, Protocol, NotRequired, cast, Any, Dict
from datetime import datetime, timezone, timedelta
from telegram import Update, LinkPreviewOptions
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown
from meshtastic_interface import MeshtasticInterface
from telegram_interface import TelegramInterface
from config_manager import ConfigManager, get_logger
from node_manager import NodeManager
import re
from logging_utils import log_event, new_id

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
        import logging
        self.logger: logging.Logger = get_logger(__name__)
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
        }
        self.forwarding_enabled: bool = config.get('telegram.enable_message_forwarding', False)

    # --- Main Message Loops ---

    async def process_messages(self) -> None:
        """Launch core processing loops and wait until completion/cancellation."""
        log_event(self.logger, 20, "processor_start", instance=self.instance_id)
        self.processing_tasks = [
            asyncio.create_task(self.process_meshtastic_messages()),
            asyncio.create_task(self.process_telegram_messages()),
            asyncio.create_task(self.process_pending_acks())
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
                    self.logger.info(
                        f"[Meshtastic] RX packet type={message.get('decoded', {}).get('portnum')} "
                        f"from={message.get('fromId')} raw_type={message.get('type')} id={message.get('id')}"
                    )
                # dynamic packet dict access
                log_event(self.logger, 10, "mt_message_rx", instance=self.instance_id, portnum=message.get('decoded', {}).get('portnum'), from_id=message.get('fromId'))  # type: ignore[arg-type]
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
                log_event(self.logger, 20, "tg_message_rx", instance=self.instance_id, type=message.get('type'), user_id=message.get('user_id'))  # type: ignore[arg-type]
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
                    log_event(self.logger, 30, "ack_timeout", instance=self.instance_id, message_id=message_id, bridge_id=bridge_id)
                    del self.pending_acks[message_id]
            await asyncio.sleep(10)

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
        log_event(self.logger, 20, "processor_stop_begin", instance=self.instance_id)

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
        self.is_closing = False
        self._already_closed = True  # type: ignore[attr-defined]
        log_event(self.logger, 20, "processor_stop_complete", instance=self.instance_id)

    # --- Meshtastic Message Handlers ---

    async def handle_meshtastic_message(self, packet: dict[str, object]) -> None:  # type: ignore[override]
        """Handle a non-ACK Meshtastic packet by resolving an app-specific handler."""
        # Human readable dispatch log (supplements structured events)
        is_ringtone = (
            packet.get('decoded', {}).get('portnum') == 'ADMIN_APP' and
            'getRingtoneResponse' in packet.get('decoded', {}).get('admin', {})
        )
        if not is_ringtone:
            self.logger.info(
                f"[Meshtastic] Dispatching packet from={packet.get('fromId')} to={packet.get('toId')} "
                f"type={packet.get('decoded', {}).get('portnum')} id={packet.get('id')}"
            )
        if packet.get('type') == 'ack':  # type: ignore[attr-defined]
            await self.handle_ack(packet)
            return

        portnum = packet.get('decoded', {}).get('portnum', '')  # type: ignore[index, attr-defined]
        handler_name = f"handle_{portnum.lower()}" if isinstance(portnum, str) else f"handle_{portnum}"
        handler = getattr(self, handler_name, None)

        sender = packet.get('fromId', 'unknown')  # type: ignore[attr-defined]
        formatted_name = f"`{sender}`"
        node = self.node_manager.nodes.get(sender)  # type: ignore[index]
        short_name = sender
        if node:
            short_name = node.get('shortName', '')  # type: ignore[index, attr-defined]
            long_name = node.get('longName', '')  # type: ignore[index, attr-defined]
            if short_name and isinstance(short_name, str) and short_name.strip() and short_name.lower() != "unknown":
                short_name = short_name.strip()
            else:
                short_name = sender
            formatted_name += f" - `{short_name}`"
            if long_name and isinstance(long_name, str) and long_name.strip() and long_name.lower() != "unknown":
                formatted_name += f" - `{long_name}`"

        if handler:
            # if not (portnum == 'ADMIN_APP' and 'getRingtoneResponse' in packet.get('decoded', {}).get('admin', {})):
                # self.logger.info(f"Handling Meshtastic message type {portnum} from {formatted_name}")
            await handler(packet)
        elif not portnum:
            # Ignoring private message (no event needed)
            pass
        else:
            self.logger.warning(
                f"Unhandled Meshtastic message type: {portnum} from: {packet.get('fromId')} - {formatted_name}, packet:\n{packet}"
            )

    async def handle_ack(self, packet: dict[str, object]) -> None:  # type: ignore[override]
        """Process ACK updates: add reaction in Telegram and clear tracking map."""
        message_id = packet.get('request_id')
        if message_id is None:
            self.logger.warning(f"Received ACK without message ID\n{packet=}\n")
            log_event(self.logger, 30, "ack_missing_id", instance=self.instance_id)
            return
        self.logger.info(f"[Meshtastic] ACK received for message_id={message_id}")

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
                self.logger.info(f"ACK processed for message ID: {message_id}, Telegram message ID: {telegram_message_id}")
                log_event(self.logger, 20, "ack_processed", instance=self.instance_id, message_id=message_id_int, telegram_message_id=telegram_message_id, bridge_id=pending_message.get('bridge_id'))

    async def handle_text_message_app(self, packet: dict[str, object]) -> None:  # type: ignore[override]
        """Format and forward a Meshtastic text message to Telegram (enriched logging)."""
        self.logger.info(
            f"[Meshtastic] Handling text message from={packet.get('fromId')} to={packet.get('toId')} "
            f"channel={packet.get('channel')}"
        )
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
        if channel_num in ignored_channels and not text.startswith('/ping'):
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

        log_event(self.logger, 20, "bridge_start", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg", from_id=sender, to_id=recipient, from_short=from_short, from_long=from_long, to_short=to_short, to_long=to_long)

        # Metrics
        hops_start = packet.get('hopStart', 0)  # type: ignore[index]
        hops_limit = packet.get('hopLimit', 0)  # type: ignore[index]
        try:
            hops_away = hops_start - hops_limit  # type: ignore[operator]
        except Exception:
            hops_away = 0
        snr = packet.get('rxSnr', 'n/a')  # type: ignore[index]
        rssi = packet.get('rxRssi', 'n/a')  # type: ignore[index]
        mqtt = bool(packet.get('viaMqtt', 0))  # type: ignore[index]

        # Ping shortcut
        if self.mesh_commands.get('ping', False) and text.startswith('/ping'):
            log_event(self.logger, 20, "ping_command_rx", instance=self.instance_id, bridge_id=bridge_id, sender=sender, recipient=recipient, from_short=from_short, to_short=to_short, channel=channel_num)
            ping_text = f"{from_short} → HopsAway={hops_away}, HStart={hops_start}, HLimit={hops_limit}"
            if rssi != 'n/a':
                ping_text += f", RSSI={rssi}"
            if snr != 'n/a':
                ping_text += f", SNR={snr}"
            try:
                meshtastic_message_id = await self.meshtastic.send_message(ping_text, sender, channel=channel_num)
                self.pending_acks[meshtastic_message_id] = {
                    'telegram_message_id': 0,
                    'telegram_thread_id': 0,
                    'timestamp': datetime.now(timezone.utc)
                }
                log_event(self.logger, 20, "ping_command_reply_sent", instance=self.instance_id, bridge_id=bridge_id, meshtastic_message_id=meshtastic_message_id, from_short=from_short, to_short=to_short)
            except Exception as e:
                self.logger.error(f"Failed to send ping reply: {e}", exc_info=True)
            # Continue to publish ping result to Telegram
            text = ping_text

        # Build outbound Telegram message (unchanged format except using from_short)
        channel_label = f"[<u>CH{channel_num}</u>]"
        channels = self.config.get('channels', [])  # type: ignore[assignment]
        if channels and not recipient.startswith('!'):
            try:
                channel_name = channels[channel_num]
                channel_label = f"[<u>{channel_name}</u>]"
            except Exception:
                pass
        message = (
            f"💬 <b>{channel_label} <u>{from_short}</u>: </b>{text}\n\n"
            f"📟 [{from_short}{(' - ' + from_long) if from_long else ''}] → [{to_short}{(' - ' + to_long) if to_long else ''}]"
        )

        # Emit meta + render events
        log_event(self.logger, 20, "bridge_meta", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg", from_short=from_short, from_long=from_long, to_short=to_short, to_long=to_long, hops_away=hops_away, hop_limit=hops_limit, hop_start=hops_start, rssi=rssi, snr=snr, mqtt=mqtt)
        log_text = text.replace('\n', '\\n')
        if len(log_text) > 160:
            log_text = log_text[:160] + '…'
        log_event(self.logger, 10, "bridge_render", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg", message_text=log_text)
        _ = await self.telegram.send_message(message, disable_notification=False, topic=f"channel{channel_num}" if not recipient.startswith('!') else "default")
        log_event(self.logger, 20, "bridge_sent", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg")
        log_event(self.logger, 20, "bridge_complete", instance=self.instance_id, bridge_id=bridge_id, direction="mesh_to_tg")

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
        log_event(self.logger, 20, "bridge_start", instance=self.instance_id, bridge_id=bridge_id, direction="tg_to_mesh")
        sender = str(message['sender'])[:10]
        recipient = self.config.get('meshtastic.default_node_id')
        text = str(message['text'])
        telegram_message_id = cast(int, message['message_id'])
        telegram_thread_id = cast(int, message['thread_id'])
        meshtastic_message = f"[TG:{sender}] {text}"
        channel = None

        use_topics = self.config.get('telegram.use_topics', False)
        if use_topics:
            topics = self.config.get('topics', {})
            for key, value in topics.items():
                if key.startswith('channel') and value == telegram_thread_id:
                    channel = key.split('channel')[-1]
                    break

        try:
            meshtastic_message_id = await self.meshtastic.send_message(meshtastic_message, recipient, channel=channel)
            self.pending_acks[meshtastic_message_id] = {
                'telegram_message_id': telegram_message_id,
                'telegram_thread_id': telegram_thread_id,
                'timestamp': datetime.now(timezone.utc),
                'bridge_id': bridge_id
            }
            _ = asyncio.create_task(self.remove_pending_ack(meshtastic_message_id))
            log_event(self.logger, 20, "bridge_sent", instance=self.instance_id, bridge_id=bridge_id, direction="tg_to_mesh", meshtastic_message_id=meshtastic_message_id)
        except Exception as e:
            self.logger.error(f"Failed to send message to Meshtastic: {e}", exc_info=True)
            await self.telegram.send_message("Failed to send message to Meshtastic. Please try again.", topic=str(telegram_thread_id))
            log_event(self.logger, 40, "bridge_error", instance=self.instance_id, bridge_id=bridge_id, direction="tg_to_mesh", error=str(e))

    # --- (Re)Added Meshtastic App Handlers ---

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
        self.logger.info(f"[Meshtastic] Handling nodeinfo from={raw_id}")
        if not self._valid_node_id(raw_id):
            log_event(self.logger, 30, "mt_node_id_invalid", instance=self.instance_id, app="nodeinfo", raw_id=raw_id)
            self.logger.warning(f"[Meshtastic] Invalid nodeinfo id ignored id={raw_id}")
            return
        node_id: str = str(raw_id)
        node_info: dict[str, Any] = packet.get('decoded', {})  # type: ignore[assignment]
        self.node_manager.update_node(node_id, {
            'shortName': node_info.get('user', {}).get('shortName', 'unknown'),
            'longName': node_info.get('user', {}).get('longName', 'unknown'),
            'hwModel': node_info.get('user', {}).get('hwModel', 'unknown')
        })
        if self.reports.get('nodes', True):
            info_text: str = self.node_manager.format_node_info(node_id)
            await self.telegram.send_or_edit_message('nodeinfo', node_id, info_text)

    async def handle_position_app(self, packet: Dict[str, Any]) -> None:
        raw_id = packet.get('fromId')
        self.logger.info(f"[Meshtastic] Handling position from={raw_id}")
        if not self._valid_node_id(raw_id):
            log_event(self.logger, 30, "mt_node_id_invalid", instance=self.instance_id, app="position", raw_id=raw_id)
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

    async def handle_telemetry_app(self, packet: Dict[str, Any]) -> None:
        raw_id = packet.get('fromId')
        self.logger.info(f"[Meshtastic] Handling telemetry from={raw_id}")
        if not self._valid_node_id(raw_id):
            log_event(self.logger, 30, "mt_node_id_invalid", instance=self.instance_id, app="telemetry", raw_id=raw_id)
            self.logger.warning(f"[Meshtastic] Invalid telemetry id ignored id={raw_id}")
            return
        node_id = str(raw_id)
        telemetry = packet.get('decoded', {}).get('telemetry', {})  # type: ignore[assignment]
        device_metrics = telemetry.get('deviceMetrics', {})
        self.node_manager.update_node_telemetry(node_id, device_metrics)
        if self.reports.get('telemetry', True):
            telemetry_info = self.node_manager.get_node_telemetry(node_id)
            await self.telegram.send_or_edit_message('telemetry', node_id, telemetry_info)

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
                log_event(self.logger, 30, "mt_node_id_invalid", instance=self.instance_id, app="admin_deviceMetrics", raw_id=raw_id)
                self.logger.warning(f"[Meshtastic] Invalid admin deviceMetrics id ignored id={raw_id}")
            else:
                await self._handle_device_metrics(str(raw_id), admin_message['deviceMetrics'])
        elif 'position' in admin_message:
            raw_id = packet.get('fromId')
            if not self._valid_node_id(raw_id):
                log_event(self.logger, 30, "mt_node_id_invalid", instance=self.instance_id, app="admin_position", raw_id=raw_id)
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