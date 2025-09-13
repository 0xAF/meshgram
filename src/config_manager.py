from typing import Any, Optional, List, Dict
from envyaml import EnvYAML
from logging_utils import configure_logging, get_logger  # re-export get_logger for compatibility

class ConfigManager:
    """
    Loads and manages application configuration from a YAML file.
    Provides helpers for logging setup and config validation.
    """
    def __init__(self, config_path: str = 'config/config.yaml'):
        try:
            self.config: Dict[str, Any] = EnvYAML(config_path)
        except Exception as e:
            raise ValueError(f"Failed to load configuration from {config_path}: {e}")
        # Delegate logging configuration to logging_utils
        configure_logging(self.config)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        """
        Retrieve a configuration value by key, with optional default.
        Raises KeyError if not found and no default is provided.
        """
        value = self.config.get(key, default)
        if value is None and default is None:
            raise KeyError(f"Configuration key '{key}' not found and no default value provided")
        return value

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