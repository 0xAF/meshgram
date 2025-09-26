import pytest
from ai_common import strip_thinking_blocks, parse_kv_lines, format_weather_summary, convert_units_inplace


def test_strip_thinking_blocks_variants():
    cases = [
        ("Hello<think>secret</think> world", "Hello world"),
        ("""Thinking:\nstep 1\nstep 2\n\nAnswer here""", "Answer here"),
        ("""```thinking\ninternal\n```\nVisible""", "Visible"),
        ("<reasoning>nope</reasoning>Visible", "Visible"),
        ("Multi\n\n\n\nBlank", "Multi\n\nBlank"),
    ]
    for src, expected in cases:
        assert strip_thinking_blocks(src) == expected


def test_parse_kv_lines_numbers_and_strings():
    text = """
    temperature: 21.5
    relative_humidity: 48
    status: ok
    sci: 1.2e3
    bad: not_a_number
    nanv: NaN
    """
    d = parse_kv_lines(text)
    assert d["temperature"] == 21.5
    assert d["relative_humidity"] == 48
    assert d["status"] == "ok"
    assert d["sci"] == pytest.approx(1200.0)
    assert d["bad"] == "not_a_number"
    assert d["nanv"].lower() == "nan"


def test_format_weather_summary_and_aliases():
    data = {
        "temperature_c": 22.0,
        "relative_humidity": 40,
        "wind_kmh": 18.0,  # 5 m/s
        "wind_direction": 270,
        "rainfall_24h": 0.0,
        "lux": 1234,
    }
    summary = format_weather_summary(dict(data))
    # Check expected substrings without being overly strict on commas/spacing
    assert "Weather in Varna, Bulgaria" in summary
    assert "T=22.0°C" in summary
    assert "RH=40%" in summary
    assert "Wind=5.0 m/s @ 270°" in summary
    assert "Rain24h=0.0 mm" in summary
    assert "Lux=1234" in summary


def test_convert_units_inplace_imperial():
    d = {"temperature": 25.0, "wind_speed": 10.0}
    convert_units_inplace(d, "imperial")
    assert d.get("temperature_f") == 77.0
    assert d.get("wind_speed_mph") == pytest.approx(22.3693629, rel=1e-6)


def test_convert_units_inplace_metric_noop():
    d = {"temperature": 25.0, "wind_speed": 10.0}
    convert_units_inplace(d, "metric")
    assert "temperature_f" not in d
    assert "wind_speed_mph" not in d
