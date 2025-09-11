import argparse
import asyncio
import sys
import os
from typing import Optional, List
from asyncio import Task

from meshtastic_interface import MeshtasticInterface
from telegram_interface import TelegramInterface
from message_processor import MessageProcessor
from config_manager import ConfigManager, get_logger

class Meshgram:
    def __init__(self, config: ConfigManager) -> None:
        self.config = config
        self.logger = get_logger(__name__)
        self.meshtastic: Optional[MeshtasticInterface] = None
        self.telegram: Optional[TelegramInterface] = None
        self.message_processor: Optional[MessageProcessor] = None
        self.tasks: List[Task] = []
        self.is_shutting_down = False

    async def setup(self) -> None:
        """Initialize all components."""
        self.logger.info("Setting up meshgram...")
        try:
            self.meshtastic = await self._setup_meshtastic()
            self.telegram = await self._setup_telegram()
            self.message_processor = MessageProcessor(self.meshtastic, self.telegram, self.config)
            self.logger.info("Meshgram setup complete.")
        except Exception as e:
            self.logger.error(f"Error during setup: {e}", exc_info=True)
            await self.shutdown()
            raise

    async def _setup_meshtastic(self) -> MeshtasticInterface:
        meshtastic = MeshtasticInterface(self.config)
        await meshtastic.setup()
        return meshtastic

    async def _setup_telegram(self) -> TelegramInterface:
        telegram = TelegramInterface(self.config)
        await telegram.setup()
        return telegram

    async def shutdown(self) -> None:
        """Shutdown all components and cancel running tasks."""
        if self.is_shutting_down:
            self.logger.info("Shutdown already in progress, loading the timer to kill us.")
            await asyncio.sleep(5)
            self.logger.info("Timeout reached during shutdown, exiting.")
            os._exit(0)

        self.is_shutting_down = True
        self.logger.info("Shutting down meshgram...")

        # Cancel all running tasks
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

        # Shutdown components in reverse order of creation
        for component in [self.message_processor, self.telegram, self.meshtastic]:
            if component:
                try:
                    await component.close()
                except Exception as e:
                    self.logger.error(f"Error closing {component.__class__.__name__}: {e}", exc_info=True)

        self.logger.info("Meshgram shutdown complete.")

    async def run(self) -> None:
        """Run the main application loop."""
        try:
            await self.setup()
        except Exception as e:
            self.logger.error(f"Failed to set up Meshgram: {e}", exc_info=True)
            return

        self.logger.info("Meshgram is running.")
        self.tasks = [
            asyncio.create_task(self.message_processor.process_messages()),
            asyncio.create_task(self.meshtastic.process_thread_safe_queue()),
            asyncio.create_task(self.meshtastic.process_pending_messages()),
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
    logger = get_logger(__name__)

    app = Meshgram(config)
    try:
        await app.run()
    except ExceptionGroup as eg:
        for i, e in enumerate(eg.exceptions, 1):
            logger.error(f"Exception {i}: {e}", exc_info=e)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt.")
    finally:
        await app.shutdown()

if __name__ == '__main__':
    asyncio.run(main())