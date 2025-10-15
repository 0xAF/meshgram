import os
import stat
import textwrap
import pytest
from unittest.mock import MagicMock

from ai_common import get_local_weather_from_script


@pytest.mark.asyncio
async def test_weather_script_happy_path(tmp_path):
    script_path = tmp_path / "weather.sh"
    script_path.write_text(textwrap.dedent(
        """
        #!/usr/bin/env bash
        echo "temperature: 20.0"
        echo "relative_humidity: 55"
        echo "wind_speed: 3.0"
        echo "wind_direction: 180"
        echo "rainfall_24h: 0"
        echo "lux: 200"
        """
    ).strip() + "\n", encoding="utf-8")
    os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IXUSR)

    logger = MagicMock()
    data, summary = await get_local_weather_from_script(
        script=str(script_path), timeout=5.0, logger=logger, instance="t1"
    )
    assert data["temperature"] == 20.0
    assert data["relative_humidity"] == 55
    assert "T=20.0°C" in summary
    assert "RH=55%" in summary

    # logger should have been called with weather_exec_ok at least once
    logger.info.assert_any_call("weather_exec_ok", keys=len(data.keys()), instance="t1")


@pytest.mark.asyncio
async def test_weather_script_timeout(tmp_path):
    script_path = tmp_path / "slow.sh"
    script_path.write_text(textwrap.dedent(
        """
        #!/usr/bin/env bash
        sleep 2
        echo "temperature: 1"
        """
    ).strip() + "\n", encoding="utf-8")
    os.chmod(script_path, os.stat(script_path).st_mode | stat.S_IXUSR)

    logger = MagicMock()
    data, summary = await get_local_weather_from_script(
        script=str(script_path), timeout=0.1, logger=logger, instance="t2"
    )
    assert data == {}
    assert summary.startswith("[weather_timeout]")
    logger.info.assert_any_call("weather_exec_timeout", timeout=0.1, instance="t2")


@pytest.mark.asyncio
async def test_weather_script_not_configured():
    logger = MagicMock()
    data, summary = await get_local_weather_from_script(
        script=None, timeout=1.0, logger=logger, instance="t3"
    )
    assert data == {}
    assert summary.startswith("[weather_error] environment script not configured")
    logger.info.assert_any_call("weather_script_not_configured", instance="t3")
