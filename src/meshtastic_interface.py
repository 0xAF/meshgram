from __future__ import annotations

import asyncio
import queue
import logging
from typing import Dict, Any, TypedDict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from meshtastic import tcp_interface, serial_interface
from meshtastic.serial_interface import SerialInterface
from meshtastic.tcp_interface import TCPInterface
from pubsub import pub
from config_manager import ConfigManager, get_logger
from node_manager import NodeManager
import socket
import io
import contextlib
from meshtastic.protobuf import telemetry_pb2, portnums_pb2
from meshtastic import BROADCAST_ADDR

class DeviceMetrics(TypedDict):
    batteryLevel: int
    voltage: float
    channelUtilization: float
    airUtilTx: float

class NodeInfo(TypedDict):
    user: Dict[str, Any]
    deviceMetrics: DeviceMetrics

@dataclass
class PendingMessage:
    text: str
    recipient: str
    attempts: int = 0
    last_attempt: datetime | None = field(default=None)

class MeshtasticInterface:
    config: ConfigManager
    logger: logging.Logger
    interface: SerialInterface | TCPInterface | None
    message_queue: asyncio.Queue[dict[str, object]]
    thread_safe_queue: queue.Queue[dict[str, object]]
    loop: asyncio.AbstractEventLoop
    pending_messages: list[PendingMessage]
    last_telemetry: dict[str, object]
    max_retries: int
    retry_interval: int
    node_manager: NodeManager
    is_setup: bool
    is_closing: bool
    my_node_id: str

    def __init__(self, config: ConfigManager) -> None:
        self.config = config
        self.logger = get_logger(__name__)
        self.interface = None
        self.message_queue = asyncio.Queue()
        self.thread_safe_queue = queue.Queue()
        self.loop = asyncio.get_running_loop()
        self.pending_messages = []
        self.last_telemetry = {}
        self.max_retries = 3
        self.retry_interval = 60
        self.node_manager = NodeManager(config)
        self.is_setup = False
        self.is_closing = False
        self.my_node_id = ""

    async def setup(self) -> None:
        self.logger.info("Setting up meshtastic interface...")
        try:
            self.interface = await self._create_interface()
            self.logger.debug(f"Meshtastic interface created:\n{self.interface.myInfo}")
            pub.subscribe(self.on_meshtastic_message, "meshtastic.receive")
            await self._fetch_node_info()
            self.is_setup = True
            self.logger.info("Meshtastic interface setup complete.")
        except Exception as e:
            self.logger.error(f"Failed to set up Meshtastic interface: {e=}", exc_info=True)
            raise

    async def _create_interface(self) -> SerialInterface | TCPInterface:
        connection_type = cast(str, self.config.get('meshtastic.connection_type', 'serial'))
        device = cast(str, self.config.get('meshtastic.device'))
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
        try:
            if not self.interface:
                return
            raw_info = await asyncio.to_thread(self.interface.getMyNodeInfo)
            if not isinstance(raw_info, dict):
                self.logger.error("getMyNodeInfo returned non-dict")
                return
            user_part = raw_info.get('user')
            node_id = user_part.get('id') if isinstance(user_part, dict) else None
            self.my_node_id = node_id if isinstance(node_id, str) else ""
            if self.my_node_id:
                self.logger.info(f"Received info on our node: {raw_info=}")
            else:
                self.logger.error(f"Received node info without a node ID: {raw_info=}")
        except Exception as e:
            self.logger.error(f"Failed to get node info: {e=}", exc_info=True)

    def on_meshtastic_message(self, packet: dict[str, object], _interface: object) -> None:
        self.logger.debug(f"Message details - {packet.get('fromId')=}, {packet.get('toId')=}, {packet.get('decoded', {}).get('portnum')=}")
        if packet.get('decoded', {}).get('portnum') == 'ROUTING_APP':
            self.handle_ack(packet)
        else:
            self.thread_safe_queue.put(packet)

    def handle_ack(self, packet: dict[str, object]) -> None:
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

    async def send_reaction(self, emoji: str, message_id: str) -> None:
        try:
            if not self.interface:
                raise RuntimeError("Interface not ready")
            await asyncio.to_thread(self.interface.sendReaction, emoji, messageId=message_id)  # type: ignore[attr-defined]
            self.logger.info(f"Reaction {emoji} sent for message {message_id}")
        except Exception as e:
            self.logger.error(f"Error sending reaction to Meshtastic: {e=}", exc_info=True)

    async def send_message(self, text: str, recipient: str, channel: int | None = None) -> int:
        if not text or not recipient:
            raise ValueError("Text and recipient must not be empty")
        if len(text) > 230:  # Meshtastic message size limit
            raise ValueError("Message too long")

        self.logger.info(f"Attempting to send message to Meshtastic: {text=}")
        try:
            if channel is None:
                channel = self.config.get('meshtastic.default_channel_id', 0)
            self.logger.debug(f"Sending message to Meshtastic {channel=} with {recipient=}")
            if not self.interface:
                raise RuntimeError("Interface not ready")
            result = await asyncio.to_thread(self.interface.sendText, text, destinationId=recipient, channelIndex=int(channel))  # type: ignore[attr-defined]
            self.logger.info(f"Message sent to Meshtastic {channel=}: {text=}")
            self.logger.debug(f"{result=}")
            return result.id  # Return the message ID for tracking
        except Exception as e:
            self.logger.error(f"Error sending message to Meshtastic: {e=}", exc_info=True)
            self.pending_messages.append(PendingMessage(text, recipient))
            return -1  # Indicate failure to send

    async def send_bell(self, dest_id: str) -> int:
        if not dest_id:
            raise ValueError("Destination ID must not be empty")

        try:
            if not self.interface:
                raise RuntimeError("Interface not ready")
            result = await asyncio.to_thread(self.interface.sendText, "🔔", destinationId=dest_id)  # type: ignore[attr-defined]
            self.logger.info(f"Bell (text message) sent to node {dest_id}")
            return result.id  # Return the message ID for tracking
        except Exception as e:
            self.logger.error(f"Error sending bell to node {dest_id}: {e}", exc_info=True)
            raise

    async def process_pending_messages(self) -> None:
        while True:
            current_time = datetime.now()
            for message in self.pending_messages[:]:
                if (message.last_attempt is None or (current_time - message.last_attempt) > timedelta(seconds=self.retry_interval)):
                    if message.attempts < self.max_retries:
                        try:
                            await self.send_message(message.text, message.recipient)
                            self.pending_messages.remove(message)
                        except Exception:
                            message.attempts += 1
                            message.last_attempt = current_time
                    else:
                        self.logger.warning(f"Max retries reached for message: {message.text}")
                        self.pending_messages.remove(message)
            await asyncio.sleep(self.retry_interval)

    async def process_thread_safe_queue(self) -> None:
        while True:
            try:
                packet = self.thread_safe_queue.get_nowait()
                await self.message_queue.put(packet)
            except queue.Empty:
                await asyncio.sleep(0.1)

    async def get_status(self) -> str:
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
        finally:
            self.is_setup = False
            self.is_closing = False
            self.logger.info("Meshtastic interface closed.")

    async def reconnect(self) -> None:
        self.logger.info("Attempting to reconnect to Meshtastic...")
        try:
            if self.interface:
                await asyncio.to_thread(self.interface.close)
            self.interface = await self._create_interface()
            self.logger.info("Reconnected to Meshtastic successfully.")
        except Exception as e:
            self.logger.error(f"Failed to reconnect to Meshtastic: {e}", exc_info=True)

    def getNodeInfo(self):
        try:
            output_capture = io.StringIO()
            with contextlib.redirect_stdout(output_capture), contextlib.redirect_stderr(output_capture):
                # self.interface.localNode.getMetadata()
                self.interface.localNode.get_ringtone()

            console_output = output_capture.getvalue()
            if "ringtone:" in console_output:
                return "OK"
            return -1
        except (socket.error, BrokenPipeError, ConnectionResetError, Exception) as e:
            self.logger.error(f"Error retrieving node info: {e}")
            raise e  # Propagate the error to handle reconnection

    async def periodic_health_check(self) -> None:
        while True:
            self.logger.debug("Performing periodic health check...")
            if self.interface is None:
                self.logger.warning("Meshtastic interface is not initialized, attempting to reconnect...")
                await self.reconnect()
                continue
            try:
                info = await asyncio.wait_for(asyncio.to_thread(self.getNodeInfo), timeout=5)
                if info == -1 or info is None:
                    self.logger.error("Health check failed: Invalid or no node info received. Attempting to reconnect...")
                    await self.reconnect()
                    continue
                self.logger.debug(f"Health check = {info}")
            except Exception as e:
                if isinstance(e, TimeoutError):
                    self.logger.error("Health check failed: Timeout while retrieving node info.")
                else:
                    self.logger.error(f"Health check failed: {e}", exc_info=True)
                await self.reconnect()
            await asyncio.sleep(5)  # Check every X seconds

    async def periodic_telemetry_report(self) -> None:
        telemetry_config = self.config.get('telemetry', {})
        if not telemetry_config.get('environment_enabled', False):
            self.logger.info("Environment telemetry reporting is disabled in the configuration.")
            return

        script_path = self.config.get('telemetry', {}).get('environment_script', 'echo')
        interval = self.config.get('telemetry', {}).get('environment_send_interval', 300)

        while True:
            self.logger.debug("Running environment telemetry script...")
            try:
                process = await asyncio.create_subprocess_shell(
                    script_path,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await process.communicate()

                if process.returncode != 0:
                    self.logger.error(f"Telemetry script error (code {process.returncode}): {stderr.decode().strip()}")
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
                                    field_type = type(getattr(t.environment_metrics, key))
                                    if field_type == float:
                                        setattr(t.environment_metrics, key, float(value))
                                    elif field_type == int:
                                        setattr(t.environment_metrics, key, int(float(value)))
                                    else:
                                        self.logger.warning(f"Unsupported telemetry field type for {key}: {field_type}")
                                except ValueError as ve:
                                    self.logger.error(f"Invalid value for {key}: {value} ({ve})")
                            else:
                                self.logger.warning(f"Unknown telemetry field: {key}")
                    self.logger.info(f"Sending telemetry data and sleeping for {interval} seconds...")
                    self.logger.debug(f"Telemetry Data:\n{t}")
                    if self.interface:
                        self.interface.sendData(t, BROADCAST_ADDR, portnums_pb2.PortNum.TELEMETRY_APP)  # type: ignore[attr-defined]
            except Exception as e:
                self.logger.error(f"Error running telemetry script: {e}", exc_info=True)

            await asyncio.sleep(interval)
