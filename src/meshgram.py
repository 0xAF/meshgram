import argparse
import asyncio
import sys
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
        for task in self.tasks:
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

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

        try:
            await asyncio.gather(*self.tasks)
        except asyncio.CancelledError:
            self.logger.info("Received cancellation signal.")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            await self.shutdown()

async def main() -> None:
    parser = argparse.ArgumentParser(description='Meshgram: Meshtastic-Telegram Bridge')
    parser.add_argument('-c', '--config', default='config/config.yaml', help='Path to configuration file')
    args = parser.parse_args()

    config = ConfigManager(args.config)
    from typing import cast as _cast
    logger: StructuredLogger = _cast(StructuredLogger, get_logger(__name__))

    app = Meshgram(config)
    try:
        await app.run()
    except ExceptionGroup as eg:
        for i, e in enumerate(eg.exceptions, 1):
            logger.error("run_exception", run_id=app.run_id, index=i, error=str(e), exc_info=e)
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt", run_id=app.run_id)
    # No explicit second shutdown call; run() already performs cleanup.

if __name__ == '__main__':
    asyncio.run(main())