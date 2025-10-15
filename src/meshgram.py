import argparse
import asyncio
import os
import threading
from typing import Optional, List
from asyncio import Task

from meshtastic_interface import MeshtasticInterface
from telegram_interface import TelegramInterface
from message_processor import MessageProcessor
from config_manager import ConfigManager
from logging_utils import get_logger, StructuredLogger
from logging_utils import new_id

class Meshgram:
    def __init__(self, config: ConfigManager) -> None:
        self.config = config
        from typing import cast as _cast
        self.logger: StructuredLogger = _cast(StructuredLogger, get_logger(__name__))
        self.meshtastic: Optional[MeshtasticInterface] = None
        self.telegram: Optional[TelegramInterface] = None
        self.message_processor: Optional[MessageProcessor] = None
        self.tasks: List[Task] = []
        self.is_shutting_down = False
        self.run_id: int = new_id()

    async def setup(self) -> None:
        """Initialize all components."""
        self.logger.info("startup_begin", run_id=self.run_id)
        try:
            self.meshtastic = await self._setup_meshtastic()
            self.telegram = await self._setup_telegram()
            self.message_processor = MessageProcessor(self.meshtastic, self.telegram, self.config)
            self.logger.info("startup_complete", run_id=self.run_id)
        except Exception as e:
            self.logger.error(f"Error during setup: {e}", exc_info=True)
            await self.shutdown()
            raise

    async def _setup_meshtastic(self) -> MeshtasticInterface:
        meshtastic = MeshtasticInterface(self.config, on_reconnect_storm=self.shutdown)
        await meshtastic.setup()
        return meshtastic

    async def _setup_telegram(self) -> TelegramInterface:
        telegram = TelegramInterface(self.config)
        await telegram.setup()
        return telegram

    async def shutdown(self) -> None:
        """Shutdown all components and cancel running tasks."""
        def _force_kill() -> None:
            try:
                self.logger.error("shutdown_forced_exit", run_id=self.run_id, after_seconds=3)
            except Exception:
                pass
            os._exit(1)

        try:
            timer = threading.Timer(3.0, _force_kill)
            timer.daemon = True  # don't keep process alive if we exit cleanly
            timer.start()
            self.logger.warning("shutdown_force_exit_armed", run_id=self.run_id, after_seconds=3)
        except Exception:
            # Best-effort; if arming fails, do nothing.
            pass

        if self.is_shutting_down:
            self.logger.info("Shutdown already in progress; skipping duplicate request.")
            return

        self.is_shutting_down = True
        self.logger.info("shutdown_begin", run_id=self.run_id, tasks=len(self.tasks))

        # 1. Request cooperative stops first (polling & processor) before brute cancelling.
        try:
            if self.telegram:
                await self.telegram.close()
        except Exception as e:
            self.logger.error(f"Error closing TelegramInterface early: {e}", exc_info=True)

        try:
            if self.message_processor:
                await self.message_processor.close()
        except Exception as e:
            self.logger.error(f"Error closing MessageProcessor early: {e}", exc_info=True)

        # 2. Cancel remaining tasks (meshtastic loops, etc.).
        # Avoid cancelling the current task to prevent recursive cancellation.
        try:
            current = asyncio.current_task()
        except Exception:
            current = None
        cancel_targets = []
        for task in self.tasks:
            if task is current:
                continue
            if not task.done():
                task.cancel()
            cancel_targets.append(task)
        if cancel_targets:
            await asyncio.gather(*cancel_targets, return_exceptions=True)

        # 3. Close Meshtastic last (hardware/network resource).
        try:
            if self.meshtastic:
                await self.meshtastic.close()
        except Exception as e:
            self.logger.error(f"Error closing MeshtasticInterface: {e}", exc_info=True)

        # Structured shutdown completion event
        self.logger.info("shutdown_complete", run_id=self.run_id)

        # Force-exit watchdog: if we haven't exited cleanly within 5 seconds,
        # kill the process to avoid hanging due to stray tasks/threads.
        

    async def run(self) -> None:
        """Run the main application loop."""
        try:
            await self.setup()
        except Exception as e:
            self.logger.error(f"Failed to set up Meshgram: {e}", exc_info=True)
            return

        self.logger.info("run_started", run_id=self.run_id)
        self.tasks = [
            asyncio.create_task(self.message_processor.process_messages()),
            asyncio.create_task(self.meshtastic.process_thread_safe_queue()),
            asyncio.create_task(self.meshtastic.process_outgoing_messages()),
            asyncio.create_task(self.telegram.start_polling()),
            asyncio.create_task(self.meshtastic.periodic_health_check()),
            asyncio.create_task(self.meshtastic.periodic_telemetry_report()),
        ]

        # Fire-and-forget startup notifications once background workers are active
        try:
            asyncio.create_task(self._send_startup_notifications())
        except Exception:
            pass

        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            self.logger.info("Received cancellation signal.")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            await self.shutdown()

    async def _send_startup_notifications(self) -> None:
        """Notify admins on the mesh and users in Telegram that the bot started."""
        # Small delay to ensure workers are accepting work
        await asyncio.sleep(0.5)
        try:
            from datetime import datetime, timezone
            ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        except Exception:
            ts = "now"
        # Compose common markers
        try:
            bot_sn = getattr(self.meshtastic, 'my_short_name', '') if self.meshtastic else ''
            bot_id = getattr(self.meshtastic, 'my_node_id', '') if self.meshtastic else ''
        except Exception:
            bot_sn = ''
            bot_id = ''

        # Telegram announcement in default topic (or configured one)
        try:
            if self.telegram and self.config.get('telegram.notify_on_start', True):
                topic = self.config.get('telegram.startup_topic', 'default')
                # Build a node label that includes both short name and id when available
                node_label = ""
                try:
                    if (bot_sn or bot_id):
                        node_label = f" — node {bot_sn} ({bot_id})" if (bot_sn and bot_id) else f" — node {bot_sn or bot_id}"
                except Exception:
                    node_label = ""
                text = f"✅ Bot is up and running. (run_id={self.run_id}){node_label} — {ts}"
                _ = await self.telegram.send_message(text=text, topic=str(topic))
                self.logger.info("startup_notify_telegram", run_id=self.run_id, topic=str(topic))
        except Exception as e:
            self.logger.error(f"startup_notify_telegram_error: {e}", exc_info=True)

        # Meshtastic DMs to admins (if any)
        try:
            if self.meshtastic and self.config.get('meshtastic.notify_admins_on_start', True):
                admins = self.config.get('meshtastic.admin_nodes', [])
                if isinstance(admins, list) and admins:
                    # Reuse the same node label formatting as Telegram
                    node_label = ""
                    try:
                        if (bot_sn or bot_id):
                            node_label = f" — node {bot_sn} ({bot_id})" if (bot_sn and bot_id) else f" — node {bot_sn or bot_id}"
                    except Exception:
                        node_label = ""
                    dm_text = f"BOT online ✅ (run_id={self.run_id}){node_label} — {ts}"
                    for raw_id in admins:
                        try:
                            if not isinstance(raw_id, str):
                                continue
                            rid = raw_id.strip()
                            if not rid:
                                continue
                            recipient = rid if rid.startswith('!') else f"!{rid}"
                            _ = await self.meshtastic.send_message(dm_text, recipient, channel=0)
                            self.logger.info("startup_notify_meshtastic", run_id=self.run_id, recipient=recipient)
                            # brief pacing to avoid burst on startup
                            await asyncio.sleep(0.1)
                        except Exception as e:
                            self.logger.error(f"startup_notify_meshtastic_error for {raw_id}: {e}")
        except Exception as e:
            self.logger.error(f"startup_notify_meshtastic_error: {e}", exc_info=True)

async def main() -> None:
    parser = argparse.ArgumentParser(description='Meshgram: Meshtastic-Telegram Bridge')
    parser.add_argument('-c', '--config', default='config', help='Path to configuration file or directory')
    args = parser.parse_args()

    config = ConfigManager(args.config)
    from typing import cast as _cast
    logger: StructuredLogger = _cast(StructuredLogger, get_logger(__name__))

    app = Meshgram(config)
    # Py3.11+ has ExceptionGroup; on 3.10 fallback treat as plain Exception
    try:
        await app.run()
    except Exception as eg:  # type: ignore[no-redef]
        for i, e in enumerate(eg.exceptions, 1):
            logger.error("run_exception", run_id=app.run_id, index=i, error=str(e), exc_info=e)
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt", run_id=app.run_id)
    # No explicit second shutdown call; run() already performs cleanup.

if __name__ == '__main__':
    asyncio.run(main())