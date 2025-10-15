from typing import Any, Optional, List, Dict
from pathlib import Path
from envyaml import EnvYAML
from logging_utils import configure_logging, get_logger  # re-export get_logger for compatibility
from pydantic import BaseModel, Field, ValidationError, ConfigDict


def _deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge dict b into dict a and return a (modified)."""
    for k, v in b.items():
        if k in a and isinstance(a[k], dict) and isinstance(v, dict):
            _deep_merge(a[k], v)
        else:
            a[k] = v
    return a


class TelegramSchema(BaseModel):
    bot_token: Optional[str] = None
    chat_id: Optional[int | str] = None
    authorized_users: Optional[List[int | str]] = None
    notify_on_start: Optional[bool] = None
    startup_topic: Optional[str] = None
    enable_message_forwarding: Optional[bool] = None
    use_topics: Optional[bool] = None
    raw_markdown: Optional[bool] = None
    ai_enabled: Optional[bool] = None
    triggers: Optional[List[Dict[str, Any]]] = None


class MeshtasticSchema(BaseModel):
    connection_type: Optional[str] = Field(default=None)
    device: Optional[str] = None
    default_node_id: Optional[str] = None
    default_channel_id: Optional[int] = None
    send_delay_ms: Optional[int] = None
    max_send_chunks: Optional[int] = None
    truncation_notice_template: Optional[str] = None
    commands: Optional[Dict[str, bool]] = None
    travel_template: Optional[str] = None
    admin_nodes: Optional[List[str]] = None
    ai_enabled: Optional[bool] = None
    reply_directly: Optional[bool] = None
    on_disconnect: Optional[str] = Field(default=None)
    health_max_failures: Optional[int] = Field(default=None)
    ignored_channels: Optional[List[int]] = None
    receive_only_channels: Optional[List[int]] = None
    triggers: Optional[List[Dict[str, Any]]] = None


class AiOpenAISchema(BaseModel):
    base_url: Optional[str] = None
    model: Optional[str] = None
    api_key: Optional[str] = None


class AiOllamaSchema(BaseModel):
    base_url: Optional[str] = None
    model: Optional[str] = None


class AiSchema(BaseModel):
    provider: Optional[str] = None
    enable_tools: Optional[bool] = None
    enable_thinking: Optional[bool] = None
    strip_thinking: Optional[bool] = None
    system_prompt: Optional[str] = None
    openai: Optional[AiOpenAISchema] = None
    ollama: Optional[AiOllamaSchema] = None


class TelemetrySchema(BaseModel):
    environment_enabled: Optional[bool] = None
    environment_script: Optional[str] = None
    environment_send_interval: Optional[int] = None


class RootSchema(BaseModel):
    config_version: Optional[int] = None
    telegram: Optional[TelegramSchema] = None
    meshtastic: Optional[MeshtasticSchema] = None
    telemetry: Optional[TelemetrySchema] = None
    channels: Optional[List[str]] = None
    topics: Optional[Dict[str, int | str]] = None
    reports: Optional[Dict[str, bool]] = None
    logging: Optional[Dict[str, Any]] = None
    ai: Optional[AiSchema] = None
    bbs: Optional[Dict[str, Any]] = None
    # Top-level channel-related settings (preferred location)
    default_channel_id: Optional[int] = None
    ignored_channels: Optional[List[int]] = None
    receive_only_channels: Optional[List[int]] = None

    model_config = ConfigDict(extra='allow')

class ConfigManager:
    """
    Loads and manages application configuration from a YAML file.
    Provides helpers for logging setup and config validation.
    """
    def __init__(self, config_path: str = 'config/config.yaml'):
        base_path = Path(config_path)
        # Determine config directory and base file
        if base_path.is_dir():
            cfg_dir = base_path
            base_file = cfg_dir / 'config.yaml'
        else:
            cfg_dir = base_path.parent
            base_file = base_path
        cfg: Dict[str, Any] = {}
        loaded_files: List[str] = []

        def load_yaml(p: Path) -> Dict[str, Any]:
            return dict(EnvYAML(str(p))) if p.exists() else {}

        # 1. Load main config (optional)
        if base_file.exists() and base_file.is_file():
            try:
                cfg = load_yaml(base_file)
                loaded_files.append(str(base_file))
            except Exception as e:
                raise ValueError(f"Failed to load configuration from {base_file}: {e}")
        else:
            cfg = {}

        # 2. Optional local overlay (gitignored)
        local_path = cfg_dir / 'config.local.yaml'
        if local_path.exists() and local_path.is_file():
            try:
                _deep_merge(cfg, load_yaml(local_path))
                loaded_files.append(str(local_path))
            except Exception as e:
                raise ValueError(f"Failed to load configuration from {local_path}: {e}")

        # 3. Load any other .yaml files in config dir (order-insensitive)
        try:
            for p in sorted(cfg_dir.glob('*.yaml')):
                name = p.name
                if name in ('config.yaml', 'config.local.yaml'):
                    continue
                if name.startswith('example.'):
                    continue
                if not p.is_file():
                    continue
                # Merge file content; special handling for triggers and channels
                content = load_yaml(p)
                if not content:
                    continue
                loaded_files.append(str(p))
                # If this file provides a top-level 'telegram.triggers' or 'meshtastic.triggers' block (like triggers.yaml),
                # merge those specifically to avoid overwriting the entire telegram/meshtastic sections unintentionally.
                tel_t = content.get('telegram', {}).get('triggers') if isinstance(content.get('telegram'), dict) else None
                mesh_t = content.get('meshtastic', {}).get('triggers') if isinstance(content.get('meshtastic'), dict) else None
                if tel_t is not None or mesh_t is not None:
                    if tel_t is not None:
                        cfg.setdefault('telegram', {})
                        cfg['telegram']['triggers'] = tel_t
                    if mesh_t is not None:
                        cfg.setdefault('meshtastic', {})
                        cfg['meshtastic']['triggers'] = mesh_t
                    # Also merge the rest of the file content in case it has other keys
                    _deep_merge(cfg, content)
                else:
                    _deep_merge(cfg, content)

                # If this file has channels/reports/topics, copy to top-level convenience keys
                if content.get('channels'):
                    cfg['channels'] = content['channels']
                if content.get('reports'):
                    cfg['reports'] = content['reports']
                if content.get('topics'):
                    cfg['topics'] = content['topics']
                for k in ('default_channel_id', 'ignored_channels', 'receive_only_channels'):
                    if k in content:
                        cfg[k] = content[k]
        except Exception as e:
            raise ValueError(f"Failed to load configuration from directory {cfg_dir}: {e}")

        # 4. Validate with Pydantic for friendlier errors
        try:
            _ = RootSchema(**cfg)
        except ValidationError as ve:
            raise ValueError(f"Invalid configuration: {ve}")

        # Store config
        self.config = cfg
        # Delegate logging configuration to logging_utils
        configure_logging(self.config)
        # Emit a concise config summary for diagnostics
        try:
            logger = get_logger(__name__)
            logger.info(
                f"config_loaded base={base_file} dir={cfg_dir} files={len(loaded_files)} has_meshtastic={isinstance(self.config.get('meshtastic'), dict)} has_telegram={isinstance(self.config.get('telegram'), dict)}",
                extra={"file_list": loaded_files},
            )
        except Exception:
            pass

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve a configuration value by key, supporting dotted paths (e.g.,
        'meshtastic.connection_type'). If not found, return default; if default
        is None, raise KeyError to match previous behavior.
        """
        if not isinstance(key, str) or not key:
            return default
        if '.' not in key:
            val = self.config.get(key, default)
            if val is None and default is None:
                raise KeyError(f"Configuration key '{key}' not found and no default value provided")
            return val
        cur: Any = self.config
        for part in key.split('.'):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                if default is None:
                    raise KeyError(f"Configuration key '{key}' not found and no default value provided")
                return default
        return cur

    def get_authorized_users(self) -> List[int]:
        """
        Returns a list of authorized Telegram user IDs as integers.
        """
        users = self.get('telegram.authorized_users', [])
        return [int(user) for user in users if str(user).isdigit()]


    def validate_config(self) -> None:
        """
        Ensure all required configuration keys are present.
        """
        required_keys = [
            'telegram.bot_token',
            'telegram.chat_id',
            'meshtastic.connection_type',
            'meshtastic.device',
        ]
        missing_keys = [key for key in required_keys if not self.get(key)]
        if missing_keys:
            raise ValueError(f"Missing required configuration: {', '.join(missing_keys)}")

    # Note: logging classes and helpers moved to logging_utils to centralize logging concerns.