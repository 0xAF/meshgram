import asyncio
import re
import logging
from typing import Dict, Any, Callable, TypedDict
from collections.abc import Awaitable
from telegram import Bot, Update
from telegram.ext import (
    Application, CommandHandler, ContextTypes,
    MessageHandler, MessageReactionHandler, filters
)
from telegram.constants import ParseMode
from telegram.helpers import escape_markdown
from telegram.error import BadRequest
from config_manager import ConfigManager, get_logger
from logging_utils import log_event, new_id

COMMAND_DEFAULT_TOPIC_ID = 1  # Fallback thread id when topics aren't in use
MARKDOWN_VERSION = 2


class CommandData(TypedDict):
    """Represents a Telegram command exposed by the bot."""
    description: str
    handler: Callable[[Update, ContextTypes.DEFAULT_TYPE], Awaitable[None]]

class TelegramInterface:
    config: ConfigManager
    logger: logging.Logger
    bot: Bot | None
    application: Application | None  # type: ignore[type-arg]
    message_queue: asyncio.Queue[dict[str, Any]]
    _stop_event: asyncio.Event
    chat_id: int | None
    last_messages: dict[str, int]
    is_polling: bool
    commands: dict[str, CommandData]

    def __init__(self, config: ConfigManager) -> None:
        """Create a Telegram interface (bot + application) but do not start polling yet."""
        self.config = config
        self.logger = get_logger(__name__)
        self.instance_id = new_id()
        self.bot = None
        self.application = None  # type: ignore[assignment]
        self.message_queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self.chat_id = None
        self.last_messages = {}
        self.is_polling = False
        self.commands = {
            'start':    {'description': 'Start the bot and see available commands', 'handler': self.start_command},
            'help':     {'description': 'Show help message', 'handler': self.help_command},
            'user':     {'description': 'Get information about your Telegram user', 'handler': self.user_command},
            'status':   {'description': 'Check the current status', 'handler': self.handle_command},
            'bell':     {'description': 'Send a bell to the meshtastic user', 'handler': self.handle_command},
            'node':     {'description': 'Get information about a specific node', 'handler': self.handle_command},
            'enable':   {'description': 'Enable a feature', 'handler': self.handle_command},
            'disable':  {'description': 'Disable a feature', 'handler': self.handle_command},
            'features': {'description': 'List features', 'handler': self.handle_command},
            'listnodes':{'description': 'List all known nodes', 'handler': self.handle_command},
        }

    async def setup(self) -> None:
        """Instantiate the Telegram Bot & Application and register command/message handlers."""
        # Structured event begins setup (removes redundant human-readable line)
        log_event(self.logger, logging.INFO, "telegram_setup_begin", instance=self.instance_id)
        try:
            token = self.config.get('telegram.bot_token')
            if not token:
                raise ValueError("Telegram bot token not found in configuration")
            self.bot = Bot(token=token)
            self.application = Application.builder().token(token).build()
            self._setup_handlers()
            await self.bot.set_my_commands([(cmd, data['description']) for cmd, data in self.commands.items()])
            self.chat_id = self.config.get('telegram.chat_id')
            if not self.chat_id:
                raise ValueError("Telegram chat id not found in configuration")
            log_event(self.logger, logging.INFO, "telegram_setup_complete", instance=self.instance_id, chat_id=self.chat_id)
        except Exception as e:
            self.logger.exception(f"Failed to set up telegram: {e}")
            log_event(self.logger, logging.ERROR, "telegram_setup_error", instance=self.instance_id, error=str(e))
            raise

    def _setup_handlers(self) -> None:
        """Register command, message, location and reaction handlers."""
        if self.application is None:
            raise RuntimeError("Application not initialized")
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self.on_telegram_message))
        self.application.add_handler(MessageHandler(filters.LOCATION, self.on_telegram_location))
        self.application.add_handler(MessageReactionHandler(self.on_telegram_reaction))
        for command, data in self.commands.items():
            # Type: telegram.ext expects a specific coroutine subtype; our handler matches at runtime.
            self.application.add_handler(CommandHandler(command, data['handler']))  # type: ignore[arg-type]

    async def start_polling(self) -> None:
        """Begin long-polling loop until stop event is set."""
        if not self.application:
            self.logger.error("Telegram application not initialized")
            return
        log_event(self.logger, logging.INFO, "telegram_polling_start", instance=self.instance_id)
        try:
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling(drop_pending_updates=True)
            self.is_polling = True
            await self._stop_event.wait()
        except Exception as e:
            self.logger.error(f"Error in Telegram polling: {e}", exc_info=True)
            log_event(self.logger, logging.ERROR, "telegram_polling_error", instance=self.instance_id, error=str(e))
        finally:
            await self._shutdown_polling()

    async def _shutdown_polling(self) -> None:
        """Stop polling and cleanly shut down the application."""
        log_event(self.logger, logging.INFO, "telegram_polling_stop_begin", instance=self.instance_id)
        if self.application and self.is_polling:
            try:
                self.is_polling = False
                await self.application.stop()
                await self.application.shutdown()
            except Exception as e:
                self.logger.error(f"Error during Telegram shutdown: {e}", exc_info=True)
                log_event(self.logger, logging.ERROR, "telegram_shutdown_error", instance=self.instance_id, error=str(e))
        # Only emit stopped events once
        if not self.is_polling:
            log_event(self.logger, logging.INFO, "telegram_polling_stopped", instance=self.instance_id)

    async def on_telegram_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle normal text message (non-command) from Telegram user."""
        if update.message is None or update.effective_user is None:
            return
        log_event(self.logger, logging.DEBUG, "tg_msg_rx", instance=self.instance_id, user_id=update.effective_user.id, message_id=update.message.message_id)
        await self.message_queue.put({
            'text': update.message.text,
            'sender': update.effective_user.username or update.effective_user.first_name,
            'type': 'telegram',
            'message_id': update.message.message_id,
            'thread_id': update.message.message_thread_id,
            'user_id': update.effective_user.id
        })

    async def on_telegram_location(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle location messages from the user and queue them for processing."""
        if update.message is None or update.message.location is None or update.effective_user is None:
            return
        log_event(self.logger, logging.DEBUG, "tg_loc_rx", instance=self.instance_id, user_id=update.effective_user.id, message_id=update.message.message_id)
        await self.message_queue.put({
            'location': {
                'latitude': update.message.location.latitude,
                'longitude': update.message.location.longitude
            },
            'sender': update.effective_user.username or update.effective_user.first_name,
            'type': 'location',
            'message_id': update.message.message_id
        })

    async def on_telegram_reaction(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle reaction events (reply-based)."""
        if update.message is None or update.message.reaction is None or update.effective_user is None:
            return
        self.logger.info(f"Received reaction: {update.message.reaction}")
        log_event(self.logger, logging.DEBUG, "tg_reaction_rx", instance=self.instance_id, user_id=update.effective_user.id)
        if update.message.reply_to_message:
            await self.message_queue.put({
                'type': 'reaction',
                'emoji': update.message.reaction.emoji,
                'user_id': update.effective_user.id,
                'original_message_id': update.message.reply_to_message.message_id
            })

    def get_topic_id(self, topic: str) -> int:
        """Resolve a configured topic name to an integer thread id.

        Returns COMMAND_DEFAULT_TOPIC_ID if topics are disabled or not found.
        """
        use_topics = self.config.get('telegram.use_topics', False)
        topics = self.config.get('topics', {})
        if use_topics and topic != "default":
            if isinstance(topic, int) or (isinstance(topic, str) and topic.isnumeric()):
                return int(topic)
            if topic in topics:
                return int(topics[topic])
        return COMMAND_DEFAULT_TOPIC_ID

    # --- Internal helpers ---

    def _normalize_escaped(self, text: str) -> str:
        """Post-process escaped markdown while allowing custom lightweight HTML-like markers."""
        text = (text
                .replace('<i\\>', '_').replace('</i\\>', '_')
                .replace('<b\\>', '*').replace('</b\\>', '*')
                .replace('<u\\>', '__').replace('</u\\>', '__')
                .replace('\\`', '`'))
        # Re-enable markdown links after escape_markdown forced escaping
        text = re.sub(r'\\\[([^\]]+)\\\]\\\(([^)]+)\\\)', r'[\1](\2)', text)
        return text

    def _escape_markdown(self, text: str) -> str:
        return self._normalize_escaped(escape_markdown(text, version=MARKDOWN_VERSION))

    def _html_tag_to_markdown(self, text: str) -> str:
        """Convert a limited subset of simple HTML-like tags (<b>, <i>, <u>) to Markdown V2.

        This supports legacy message formatting where upstream producers insert
        <b>/<u>/<i> tags. We map them to *, __, _ respectively so that when
        raw_markdown mode is enabled we still deliver valid Markdown.
        """
        replacements = {
            '<b>': '*', '</b>': '*',
            '<i>': '_', '</i>': '_',
            # Underline not supported in Markdown V2; drop tags instead of mapping to bold
            '<u>': '', '</u>': '',
        }
        # Fast path if no angle bracket present
        if '<' not in text:
            return text
        out = text
        for k, v in replacements.items():
            out = out.replace(k, v)
        return out

    def _escape_for_retry(self, original: str) -> str:
        """Escape full text for Telegram Markdown V2 as a last-resort fallback.

        We let python-telegram-bot's escape_markdown handle the heavy lifting but
        still translate legacy HTML tags first so basic emphasis survives.
        """
        converted = self._html_tag_to_markdown(original)
        return escape_markdown(converted, version=MARKDOWN_VERSION)

    # --- Raw markdown preparation ---

    def _prepare_raw_markdown(self, text: str) -> str:
        """Prepare pre-formatted (already escaped) Markdown V2 content.

        Goal: keep existing intentional formatting while only minimally
        escaping characters that commonly trigger parse errors when not part
        of a link. The main culprit causing fallback re-escape (and thus
        double escapes) has been literal square brackets used decoratively.

        Steps:
          1. Convert allowed lightweight HTML tags to markdown markers.
          2. Temporarily protect valid markdown links [text](url).
          3. Escape remaining '[' and ']'.
          4. Restore protected links.
        """
        converted = self._html_tag_to_markdown(text)

        # Protect existing links so we don't escape their brackets
        link_pattern = re.compile(r"\[[^\]]+\]\([^\)]+\)")
        placeholders: list[str] = []
        def _store(m: re.Match) -> str:  # type: ignore[name-defined]
            placeholders.append(m.group(0))
            return f"@@LINK{len(placeholders)-1}@@"
        converted = link_pattern.sub(_store, converted)

        # Escape decorative brackets not already escaped
        # (avoid double escaping if already has a preceding backslash)
        converted = re.sub(r"(?<!\\)\[", r"\\[", converted)
        converted = re.sub(r"(?<!\\)\]", r"\\]", converted)

        # Restore links intact
        for idx, original in enumerate(placeholders):
            converted = converted.replace(f"@@LINK{idx}@@", original)
        return converted

    def _thread_kwargs(self, topic: str) -> Dict[str, Any]:
        thread_id = self.get_topic_id(topic)
        return {'message_thread_id': thread_id} if thread_id != COMMAND_DEFAULT_TOPIC_ID else {}

    # --- Public messaging helpers ---

    async def send_or_edit_message(self, message_type: str, node_id: str, content: str) -> None:
        message_key = f"{message_type}:{node_id}"
        topic = 'nodes' if message_type == 'nodeinfo' else message_type
        if message_key in self.last_messages:
            success = await self.edit_message(self.last_messages[message_key], content)
            if not success:
                message_id = await self.send_message(text=content, topic=topic)
                if message_id:
                    self.last_messages[message_key] = message_id
        else:
            message_id = await self.send_message(text=content, topic=topic)
            if message_id:
                self.last_messages[message_key] = message_id

    async def send_message(self, text: str, disable_notification: bool = False, topic="default", force_escape: bool | None = None) -> int | None:
        """Send a Telegram message and return its id.

        Restores legacy behavior (raw Markdown V2 formatting) by default so that
        pre-formatted strings produced elsewhere are not double-escaped. To opt
        out (and enable auto-escaping) set telegram.raw_markdown: false.
        """
        if self.bot is None or self.chat_id is None:
            self.logger.error("Bot or chat_id not initialized")
            return None
        raw_markdown_config: bool = self.config.get('telegram.raw_markdown', False)
        if force_escape is not None and force_escape:
            raw_markdown = False
        else:
            raw_markdown = raw_markdown_config
        t = self.get_topic_id(topic)
        if raw_markdown:
            content = self._prepare_raw_markdown(text)
        else:
            content = self._escape_markdown(text)
        send_kwargs: Dict[str, Any] = {
            'chat_id': self.chat_id,
            'disable_notification': disable_notification,
            'disable_web_page_preview': True,
            'parse_mode': ParseMode.MARKDOWN_V2,
            'text': content,
        }
        if t != 1:
            send_kwargs['message_thread_id'] = t

        # Attempt 1: raw (or lightly processed) content
        try:
            message = await self.bot.send_message(**send_kwargs)
            log_event(self.logger, logging.DEBUG, "tg_send_success", instance=self.instance_id, message_id=message.message_id, topic=topic, attempt=1)
            return message.message_id
        except BadRequest as e:
            if "Can't parse entities" in str(e):
                # Fallback attempt with fully escaped markdown
                send_kwargs['text'] = self._escape_for_retry(text)
                try:
                    message = await self.bot.send_message(**send_kwargs)
                    log_event(self.logger, logging.DEBUG, "tg_send_success", instance=self.instance_id, message_id=message.message_id, topic=topic, attempt=2, fallback="escaped")
                    return message.message_id
                except Exception as e2:
                    self.logger.error(f"Failed fallback send Telegram message: {e2}", exc_info=True)
                    log_event(self.logger, logging.ERROR, "tg_send_failure", instance=self.instance_id, error=str(e2), attempt=2)
                    return None
            # Non-parse error BadRequest propagate to generic handler below
            self.logger.error(f"Failed to send Telegram message (BadRequest): {e}", exc_info=True)
            log_event(self.logger, logging.ERROR, "tg_send_failure", instance=self.instance_id, error=str(e), attempt=1)
            return None
        except Exception as e:
            if "Timed out" in str(e):
                self.logger.error(f"TimedOut sending Telegram message: {e}")
            else:
                self.logger.error(f"Failed to send Telegram message: {e}", exc_info=True)
            log_event(self.logger, logging.ERROR, "tg_send_failure", instance=self.instance_id, error=str(e), attempt=1)
            return None

    async def edit_message(self, message_id: int, text: str) -> bool:
        """Edit an existing message; returns True if edited (or unchanged), else False.

        Honors telegram.raw_markdown flag similar to send_message.
        """
        if self.bot is None or self.chat_id is None:
            self.logger.error("Bot or chat_id not initialized")
            return False
        raw_markdown: bool = self.config.get('telegram.raw_markdown', True)
        if raw_markdown:
            content = self._prepare_raw_markdown(text)
        else:
            content = self._escape_markdown(text)
        edit_kwargs: Dict[str, Any] = {
            'chat_id': self.chat_id,
            'message_id': message_id,
            'parse_mode': ParseMode.MARKDOWN_V2,
            'text': content,
        }
        try:
            await self.bot.edit_message_text(**edit_kwargs)
            log_event(self.logger, logging.DEBUG, "tg_edit_success", instance=self.instance_id, message_id=message_id, attempt=1)
            return True
        except BadRequest as e:
            msg = str(e)
            if "Can't parse entities" in msg:
                edit_kwargs['text'] = self._escape_for_retry(text)
                try:
                    await self.bot.edit_message_text(**edit_kwargs)
                    log_event(self.logger, logging.DEBUG, "tg_edit_success", instance=self.instance_id, message_id=message_id, attempt=2, fallback="escaped")
                    return True
                except Exception as e2:
                    self.logger.error(f"Failed fallback edit Telegram message: {e2}", exc_info=True)
                    log_event(self.logger, logging.ERROR, "tg_edit_failure", instance=self.instance_id, error=str(e2), message_id=message_id, attempt=2)
                    return False
            if "Message to edit not found" in msg:
                self.logger.warning(f"Message {message_id} not found for editing. Will send as new message.")
                return False
            if "Message is not modified" in msg:
                self.logger.info(f"Message {message_id} is not modified, no edit needed.")
                return True
            self.logger.error(f"BadRequest error when editing message: {e}", exc_info=True)
            log_event(self.logger, logging.WARNING, "tg_edit_warning", instance=self.instance_id, error=str(e), message_id=message_id, attempt=1)
            return False
        except Exception as e:
            self.logger.error(f"Failed to edit Telegram message: {e}", exc_info=True)
            log_event(self.logger, logging.ERROR, "tg_edit_failure", instance=self.instance_id, error=str(e), message_id=message_id, attempt=1)
            return False

    def is_user_authorized(self, user_id: int) -> bool:
        authorized_users = self.config.get_authorized_users()
        return not authorized_users or user_id in authorized_users

    async def add_reaction(self, message_id: int, emoji: str) -> None:
        if self.bot is None or self.chat_id is None:
            self.logger.error("Bot or chat_id not initialized")
            return
        try:
            await self.bot.set_message_reaction(
                chat_id=self.chat_id,
                message_id=message_id,
                reaction=[emoji]
            )
        except Exception as e:
            self.logger.error(f"Failed to add reaction to Telegram message: {e}", exc_info=True)
            log_event(self.logger, logging.ERROR, "tg_reaction_failure", instance=self.instance_id, error=str(e), message_id=message_id)

    # --- Command Handlers ---

    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        await self.help_command(update, context)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Reply with aggregated command list."""
        if update.message is None:
            return
        help_text = "📚 Available commands:\n\n"
        help_text += "\n".join(f"/{command} - {data['description']}" for command, data in self.commands.items())
        escaped_help_text = self._escape_markdown(help_text)
        await update.message.reply_text(escaped_help_text, parse_mode=ParseMode.MARKDOWN_V2)

    async def user_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Return user information (mirrors /user)."""
        if update.message is None or update.effective_user is None:
            return
        user = update.effective_user
        user_info = (
            f"🆔 ID: {user.id}\n"
            f"👤 Username: @{user.username}\n"
            f"📛 Name: {user.full_name}\n"
            f"🤖 Is Bot: {'Yes' if user.is_bot else 'No'}"
        )
        escaped_user_info = self._escape_markdown(user_info)
        await update.message.reply_text(escaped_user_info, parse_mode=ParseMode.MARKDOWN_V2)

    async def handle_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Convert a Telegram command into a queued command message for the processor."""
        if update.message is None or update.effective_user is None:
            return
        command = update.message.text.split()[0][1:].partition('@')[0]
        args = context.args or []
        user_id = update.effective_user.id

        if not self.is_user_authorized(user_id) and command not in [
            'start', 'help', 'user', 'node', 'status', 'features'
        ]:
            await update.message.reply_text(
                escape_markdown("You are not authorized to use this command.", version=2),
                parse_mode=ParseMode.MARKDOWN_V2
            )
            return

        await self.message_queue.put({
            'type': 'command',
            'command': command,
            'args': args,
            'user_id': user_id,
            'update': update
        })

    async def close(self) -> None:
        """Stop polling and cleanup resources."""
        self.logger.info("Stopping telegram interface...")
        self._stop_event.set()
        await self._shutdown_polling()
        self.logger.info("Telegram interface stopped.")