from __future__ import annotations

from typing import Dict, Any, Optional, List, Union, TypedDict, Tuple
from datetime import datetime, timedelta
from telegram.helpers import escape_markdown
import json
from config_manager import ConfigManager
from logging_utils import get_logger
from sqlitedict import SqliteDict
import os

class NodeData(TypedDict):
    shortName: str
    longName: str
    hwModel: str
    batteryLevel: Optional[int]
    voltage: Optional[float]
    channelUtilization: Optional[float]
    airUtilTx: Optional[float]
    temperature: Optional[float]
    relativeHumidity: Optional[float]
    barometricPressure: Optional[float]
    gasResistance: Optional[float]
    current: Optional[float]
    latitude: Optional[float]
    longitude: Optional[float]
    last_updated: str
    last_position_update: Optional[str]
    routing: Dict[str, Any]
    neighbors: Dict[str, Any]
    sensor: Dict[str, Any]

class NodeManager:
    def __init__(self, config: ConfigManager) -> None:
        self.config: ConfigManager = config
        self.logger = get_logger(__name__)
        # Ensure data directory exists and store cache under ./data
        os.makedirs('data', exist_ok=True)
        self.nodes: Dict[str, NodeData] = SqliteDict(
            'data/cache.db',
            tablename="nodes",
            autocommit=True,
            encode=json.dumps,
            decode=json.loads
        )
        self.history_limit: int = 100
        self.migrate_nodes_from_old_cache()
        # Expose a module-global pointer for logging enrichment utilities
        try:
            set_global_node_manager(self)
        except Exception:
            pass

    # --- Migration ---
    def migrate_nodes_from_old_cache(self):
        try:
            with open('nodes.json', 'r') as f:
                self.logger.info("Migrating nodes from nodes.json to data/cache.db...")
                migration_nodes = json.load(f)
                for node_id, data in migration_nodes.items():
                    if 'last_updated' not in data:
                        data['last_updated'] = datetime.now().isoformat()
                    if node_id not in self.nodes:
                        self.nodes[node_id] = data
            os.remove('nodes.json')
            self.logger.info("Migration completed.")
        except FileNotFoundError:
            self.logger.debug("No nodes.json file found, skipping migration.")
        except json.JSONDecodeError:
            self.logger.warning("Error decoding JSON from nodes.json, skipping migration.")

    # --- Formatting ---
    def format_node_name_no_map(self, node_id: Union[str, int], short_name: str) -> str:
        return f'{self.escape_value(node_id)} ({self.escape_value(short_name)})'

    def format_node_name(self, node_id: Union[str, int], short_name: str) -> str:
        numeric_id: int = (
            int(node_id[1:], 16) if isinstance(node_id, str) and node_id.startswith('!')
            else int(node_id) if isinstance(node_id, str)
            else node_id
        )
        node_info_link = self.config.get('meshtastic.node_info_link', 'https://meshmap.net/#{node_id}')
        node_info_link = node_info_link.replace("{node_id}", str(numeric_id))
        return f'[{self.escape_value(node_id)}]({node_info_link}) ({self.escape_value(short_name)})'

    def format_node_info(self, node_id: str) -> str:
        node = self.get_node(node_id)
        if not node:
            return f"ℹ️ No information available for node {self.escape_node_id(node_id)}"
        short_name = node.get('shortName', 'unknown')
        formatted_name = self.format_node_name(node_id, short_name)
        info = [f"🔷 Node {formatted_name}:"]
        emoji_map = {
            'name': '📛', 'longName': '📝', 'hwModel': '🖥️',
            'batteryLevel': '🔋', 'voltage': '⚡', 'channelUtilization': '📊',
            'airUtilTx': '📡', 'temperature': '🌡️', 'relativeHumidity': '💧',
            'barometricPressure': '🌪️', 'gasResistance': '💨', 'current': '⚡',
            'last_updated': '🕒', 'uptimeSeconds': '⏱️'
        }
        for key, value in node.items():
            if key == 'uptimeSeconds':
                value = self._format_uptime(value)
            if key == 'last_updated':
                value = self._format_date(str(value))
            elif key in ['channelUtilization', 'airUtilTx']:
                value = self._format_percentage(value)
            elif key == 'shortName':
                continue
            emoji = emoji_map.get(key, '🔹')
            info.append(f"{emoji} {self.escape_value(key.capitalize())}: {self.escape_value(str(value))}")
        return "\n".join(info)

    def format_node_routing(self, node_id: str) -> str:
        node = self.get_node(node_id)
        if not node or 'routing' not in node:
            return f"🔀 No routing information available for node {self.escape_node_id(node_id)}"
        short_name = node.get('shortName', 'unknown')
        formatted_name = self.format_node_name_no_map(node_id, short_name)
        routing_info = node['routing']
        return (f"🔀 Routing information for node {formatted_name}:\n" +
                "\n".join(f"  {self.escape_value(k)}: {self.escape_value(v)}" for k, v in routing_info.items()))

    def format_node_neighbors(self, node_id: str) -> str:
        node = self.get_node(node_id)
        if not node or 'neighbors' not in node:
            return f"👥 No neighbor information available for node {self.escape_node_id(node_id)}"
        short_name = node.get('shortName', 'unknown')
        formatted_name = self.format_node_name_no_map(node_id, short_name)
        neighbor_info = node['neighbors']
        return (f"👥 Neighbor information for node {formatted_name}:\n" +
                "\n".join(f"  {self.escape_value(k)}: {self.escape_value(v)}" for k, v in neighbor_info.items()))

    def get_node_sensor_info(self, node_id: str) -> str:
        node = self.get_node(node_id)
        if not node or 'sensor' not in node:
            return f"🔬 No sensor information available for node {self.escape_node_id(node_id)}"
        short_name = node.get('shortName', 'unknown')
        formatted_name = self.format_node_name_no_map(node_id, short_name)
        sensor_data = node['sensor']
        return (f"🔬 Sensor information for node {formatted_name}:\n" +
                "\n".join(f"  {self.escape_value(k)}: {self.escape_value(v)}" for k, v in sensor_data.items()))

    # --- Node Data Access ---
    def get_node(self, node_id: str) -> Optional[NodeData]:
        return self.nodes.get(node_id)

    def get_all_nodes(self) -> Dict[str, NodeData]:
        return self.nodes

    # --- Node Updates ---
    def update_node(self, node_id: str | None, data: Dict[str, Any]) -> None:
        # Guard against missing/None node ids (observed KeyError None)
        if not node_id or not isinstance(node_id, str) or node_id.strip() == '':
            self.logger.warning(f"Ignoring update_node with invalid node_id={node_id!r}")
            return
        if node_id not in self.nodes:
            # Initialize minimal NodeData structure
            self.nodes[node_id] = NodeData(  # type: ignore[call-arg]
                shortName='unknown', longName='unknown',
                last_updated=datetime.now().isoformat(),
            )
        if 'user' in data and isinstance(data['user'], dict):
            data = data['user']
        if "raw" in data:
            del data["raw"]
        node = self.nodes[node_id]
        for key, value in data.items():
            if node.get(key) != value:  # type: ignore[index]
                node[key] = value  # type: ignore[index]
        node['last_updated'] = datetime.now().isoformat()  # type: ignore[index]
        self.nodes[node_id] = node  # persist
        # self.logger.info(f"Updated node {self.escape_node_id(node_id)} with data: {data}")
    
    def update_node_telemetry(self, node_id: str, telemetry_data: Dict[str, Any]) -> None:
        telemetry_data['last_telemetry_update'] = datetime.now().isoformat()
        self.update_node(node_id, telemetry_data)

    def update_node_position(self, node_id: str, position_data: Dict[str, Any]) -> None:
        lat = position_data.get('latitudeI')
        lon = position_data.get('longitudeI')
        if lat is not None and lon is not None:
            self.update_node(node_id, {
                'latitude': lat / 1e7,
                'longitude': lon / 1e7,
                'last_position_update': datetime.now().isoformat()
            })

    def update_node_routing(self, node_id: str, routing_info: Dict[str, Any]) -> None:
        routing_info['last_routing_update'] = datetime.now().isoformat()
        self.update_node(node_id, {'routing': routing_info})

    def update_node_neighbors(self, node_id: str, neighbor_info: Dict[str, Any]) -> None:
        neighbor_info['last_neightbor_update'] = datetime.now().isoformat()
        self.update_node(node_id, {'neighbors': neighbor_info})

    def update_node_sensor(self, node_id: str, sensor_data: Dict[str, Any]) -> None:
        sensor_data['last_sensor_update'] = datetime.now().isoformat()
        self.update_node(node_id, {'sensor': sensor_data})

    def remove_node(self, node_id: str) -> None:
        self.nodes.pop(node_id, None)

    # --- Node Queries ---
    def get_node_position(self, node_id: str) -> str:
        node = self.get_node(node_id)
        if not node:
            return f"📍 No position available for node {self.escape_node_id(node_id)}"
        short_name = node.get('shortName', 'unknown')
        formatted_name = self.format_node_name_no_map(node_id, short_name)
        latitude = node.get('latitude', 'N/A')
        longitude = node.get('longitude', 'N/A')
        last_position_update = node.get('last_position_update', 'N/A')
        formatted_last = 'N/A'
        if last_position_update not in (None, 'N/A'):
            try:
                formatted_last = self._format_date(str(last_position_update))
            except Exception:
                formatted_last = str(last_position_update)
        return (
            f"📍 Position for node {formatted_name}:\n"
            f"🌎 Latitude: {self.escape_value(latitude)}\n"
            f"🌍 Longitude: {self.escape_value(longitude)}\n"
            f"🕒 Last updated: {self.escape_value(formatted_last)}"
        )

    def get_node_telemetry(self, node_id: str) -> str:
        node = self.get_node(node_id)
        if not node:
            return f"📊 No telemetry available for node {self.escape_node_id(node_id)}"
        short_name = node.get('shortName', 'unknown')
        formatted_name = self.format_node_name(node_id, short_name)
        battery_level = node.get('batteryLevel', 'N/A')
        battery_str = "PWR" if battery_level == 101 else f"{battery_level}%"
        air_util_tx = node.get('airUtilTx', 'N/A')
        air_util_tx_str = self._format_percentage(air_util_tx)
        channel_utilization = node.get('channelUtilization', 'N/A')
        channel_utilization_str = self._format_percentage(channel_utilization)
        uptime = node.get('uptimeSeconds', 'N/A')
        last_updated = node.get('last_updated', 'N/A')
        readable_uptime = self._format_uptime(uptime)
        return (
            f"📊 Telemetry for node {formatted_name}:\n"
            f"🔋 Battery: {self.escape_value(battery_str)}\n"
            f"📡 Air Utilization TX: {self.escape_value(air_util_tx_str)}\n"
            f"📊 Channel Utilization: {self.escape_value(channel_utilization_str)}\n"
            f"⏱️ Uptime: {self.escape_value(readable_uptime)}\n"
            f"🕒 Last updated: {self.escape_value(self._format_date(last_updated) if last_updated != 'N/A' else 'N/A')}"
        )

    def get_inactive_nodes(self, timeout: int = 300) -> List[str]:
        now = datetime.now()
        return [
            node_id for node_id, node in self.nodes.items()
            if 'last_updated' in node and 
            (now - datetime.fromisoformat(node['last_updated'])) > timedelta(seconds=timeout)
        ]

    # --- Helpers ---
    def escape_node_id(self, node_id: str) -> str:
        escaped = escape_markdown(node_id, version=2)
        escaped = escaped.replace("\\!", "!")
        return escaped

    def escape_value(self, value: Any) -> str:
        escaped = escape_markdown(str(value), version=2)
        escaped = escaped.replace("\\_", "_").replace("\\-", "-")
        escaped = escaped.replace("\\.", ".")
        escaped = escaped.replace("\\!", "!")
        return escaped

    def validate_node_id(self, node_id: str) -> bool:
        return len(node_id) == 8 and all(c in '0123456789abcdefABCDEF' for c in node_id)

    def _format_date(self, date_str: str) -> str:
        try:
            date = datetime.fromisoformat(date_str)
            return date.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return "Unknown"

    def _format_percentage(self, value: Any) -> str:
        match value:
            case int() | float():
                return f"{value:.2f}%"
            case _:
                return str(value)

    def _format_uptime(self, seconds: Any) -> str:
        try:
            seconds = int(seconds)
            days, remainder = divmod(seconds, 86400)
            hours, remainder = divmod(remainder, 3600)
            minutes, secs = divmod(remainder, 60)
            parts = []
            if days > 0:
                parts.append(f"{days}d")
            if hours > 0 or days > 0:
                parts.append(f"{hours}h")
            if minutes > 0 or hours > 0 or days > 0:
                parts.append(f"{minutes}m")
            parts.append(f"{secs}s")
            return " ".join(parts)
        except Exception:
            return str(seconds)

# --- Global access for logging enrichment ---
_GLOBAL_NODE_MANAGER: Optional[NodeManager] = None

def set_global_node_manager(nm: NodeManager) -> None:
    global _GLOBAL_NODE_MANAGER
    _GLOBAL_NODE_MANAGER = nm

def get_global_node_names(node_id: Any) -> Tuple[Optional[str], Optional[str]]:
    """Lookup short and long names for a node id using the global NodeManager.

    Returns (shortName, longName) or (None, None) if unknown/unavailable.
    Accepts either '!abcdef12' style or bare hex ids; lookup is performed as-is,
    with a fallback stripping a leading '!'.
    """
    try:
        nid = str(node_id) if node_id is not None else ""
    except Exception:
        return (None, None)
    if not nid:
        return (None, None)
    nm = _GLOBAL_NODE_MANAGER
    if nm is None:
        return (None, None)
    node = nm.nodes.get(nid) or (nm.nodes.get(nid[1:]) if nid.startswith('!') else None)
    if not node:
        return (None, None)
    sn = node.get('shortName')  # type: ignore[index]
    ln = node.get('longName')   # type: ignore[index]
    def _clean(v: Any) -> Optional[str]:
        if isinstance(v, str) and v.strip() and v.strip().lower() != 'unknown':
            return v.strip()
        return None
    return (_clean(sn), _clean(ln))
