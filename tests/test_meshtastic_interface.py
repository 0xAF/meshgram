import pytest
import asyncio
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock
from meshtastic_interface import MeshtasticInterface
from config_manager import ConfigManager

@pytest.fixture
def mock_config():
    config = MagicMock(spec=ConfigManager)
    config.get.return_value = 'serial'
    return config

@pytest.mark.asyncio
async def test_meshtastic_interface_setup(mock_config):
    interface = MeshtasticInterface(mock_config)
    interface._create_interface = AsyncMock()
    interface._fetch_node_info = AsyncMock()

    await interface.setup()

    assert interface.is_setup
    interface._create_interface.assert_called_once()
    interface._fetch_node_info.assert_called_once()

@pytest.mark.asyncio
async def test_meshtastic_interface_send_message(mock_config):
    interface = MeshtasticInterface(mock_config)

    # Provide a stub sync interface since production uses asyncio.to_thread
    class StubResult:
        def __init__(self, id: int) -> None:
            self.id = id

    class StubInterface:
        def sendText(self, text: str, destinationId: str, channelIndex: int | None = None):
            return StubResult(123)

    interface.interface = StubInterface()

    # Start the outgoing worker
    worker = asyncio.create_task(interface.process_outgoing_messages())
    try:
        mid = await interface.send_message("Test message", "!4e19d9a4")
        assert mid == 123
    finally:
        worker.cancel()
        with suppress(asyncio.CancelledError):
            await worker

# Add more tests for other methods...