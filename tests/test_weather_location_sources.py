from __future__ import annotations

import importlib.util
import sys
import time
import types
import unittest
from pathlib import Path


SHOP_ROOT = Path(__file__).resolve().parents[1]


def _load_module(path: Path, module_name: str, stubs: dict[str, types.ModuleType]):
    previous = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, old_module in previous.items():
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module


def _tateros_stub() -> types.ModuleType:
    integration_store = types.SimpleNamespace(integration_module=lambda _integration_id: None)
    module = types.ModuleType("tateros")
    module.integration_store = integration_store
    return module


def load_weather_forecast():
    dotenv = types.ModuleType("dotenv")
    dotenv.load_dotenv = lambda: None

    verba_base = types.ModuleType("verba_base")
    verba_base.ToolVerba = object

    helpers = types.ModuleType("helpers")
    helpers.get_tater_name = lambda: ("Tater", "")

    diagnostics = types.ModuleType("verba_diagnostics")
    diagnostics.needs_from_diagnosis = lambda *_args, **_kwargs: []

    result = types.ModuleType("verba_result")
    result.action_failure = lambda **kwargs: {"ok": False, **kwargs}
    result.action_success = lambda **kwargs: {"ok": True, **kwargs}

    return _load_module(
        SHOP_ROOT / "verba" / "weather_forecast.py",
        "weather_forecast_location_test_module",
        {
            "dotenv": dotenv,
            "verba_base": verba_base,
            "helpers": helpers,
            "tateros": _tateros_stub(),
            "verba_diagnostics": diagnostics,
            "verba_result": result,
        },
    )


def load_environment_core():
    helpers = types.ModuleType("helpers")
    helpers.extract_json = lambda value: str(value or "")
    helpers.redis_client = None
    return _load_module(
        SHOP_ROOT / "cores" / "environment_core.py",
        "environment_core_weather_source_test_module",
        {"helpers": helpers, "tateros": _tateros_stub()},
    )


def _reading(
    *,
    key: str,
    label: str,
    category: str,
    value: object,
    display: str,
    provider: str,
    provider_label: str,
    source_name: str,
    area: str,
) -> dict[str, object]:
    return {
        "key": key,
        "label": label,
        "category": category,
        "value": value,
        "display": display,
        "provider": provider,
        "provider_label": provider_label,
        "source_id": f"{provider}:test",
        "source_name": source_name,
        "area": area,
    }


class WeatherForecastLocationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_weather_forecast()
        cls.plugin = cls.module.WeatherForecastPlugin()

    def test_local_phrases_use_the_configured_default_location(self) -> None:
        for phrase in (
            "outside",
            "outside right now",
            "home",
            "at home",
            "my location",
            "near me",
            "where I am",
            "here?",
        ):
            with self.subTest(phrase=phrase):
                self.assertEqual(self.plugin._normalize_location_value(phrase), "")

    def test_request_parser_does_not_turn_local_wording_into_an_override(self) -> None:
        self.assertIsNone(self.plugin._extract_location_override("What is the weather at home?"))
        self.assertIsNone(self.plugin._extract_location_override("Weather for my location right now"))
        self.assertEqual(self.plugin._extract_location_override("Weather in Chicago"), "Chicago")
        self.assertEqual(self.plugin._extract_location_override("Weather for 60614"), "60614")
        self.assertEqual(self.plugin._extract_location_override("Weather at 41.88,-87.63"), "41.88,-87.63")

    def test_tool_guidance_reserves_location_overrides_for_real_places(self) -> None:
        guidance = " ".join(
            (self.plugin.description, self.plugin.when_to_use, self.plugin.usage)
        ).lower()
        self.assertIn("outside", guidance)
        self.assertIn("configured default", guidance)
        self.assertIn("city", guidance)


class EnvironmentKernelCurrentSourceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.core = load_environment_core()

    def _payload(self) -> dict[str, object]:
        now = time.time()
        ecowitt = {
            "provider": "ecowitt",
            "model": "Backyard Station",
            "received_at": now,
            "sample_time": now,
            "readings": [
                _reading(
                    key="tempf",
                    label="Outdoor Temperature",
                    category="temperature",
                    value=105,
                    display="105 °F",
                    provider="ecowitt",
                    provider_label="Ecowitt",
                    source_name="Backyard Station",
                    area="Outside",
                ),
                _reading(
                    key="tempinf",
                    label="Indoor Temperature",
                    category="temperature",
                    value=72,
                    display="72 °F",
                    provider="ecowitt",
                    provider_label="Ecowitt",
                    source_name="Backyard Station",
                    area="Inside",
                ),
                _reading(
                    key="humidity",
                    label="Outdoor Humidity",
                    category="humidity",
                    value=20,
                    display="20%",
                    provider="ecowitt",
                    provider_label="Ecowitt",
                    source_name="Backyard Station",
                    area="Outside",
                ),
            ],
        }
        weather_api = {
            "provider": "weather_api",
            "model": "Configured WeatherAPI Location",
            "received_at": now,
            "sample_time": now,
            "readings": [
                _reading(
                    key="weather_api_condition",
                    label="Current Condition",
                    category="condition",
                    value="Partly Cloudy",
                    display="Partly Cloudy",
                    provider="weather_api",
                    provider_label="WeatherAPI.com",
                    source_name="Configured WeatherAPI Location",
                    area="Forecast",
                ),
                _reading(
                    key="tempf",
                    label="WeatherAPI Temperature",
                    category="temperature",
                    value=78,
                    display="78 °F",
                    provider="weather_api",
                    provider_label="WeatherAPI.com",
                    source_name="Configured WeatherAPI Location",
                    area="Forecast",
                ),
            ],
        }
        home_assistant = {
            "provider": "homeassistant",
            "model": "Home Assistant Sensors",
            "received_at": now,
            "sample_time": now,
            "readings": [
                _reading(
                    key="homeassistant_sensor_bedroom_temperature_state",
                    label="Bedroom Temperature",
                    category="temperature",
                    value=69,
                    display="69 °F",
                    provider="homeassistant",
                    provider_label="Home Assistant",
                    source_name="Bedroom Temperature",
                    area="Bedroom",
                )
            ],
        }
        provider_snapshots = {
            "ecowitt": ecowitt,
            "weather_api": weather_api,
            "homeassistant": home_assistant,
        }
        combined = self.core._combined_snapshot(provider_snapshots)
        return {
            "provider_snapshots": provider_snapshots,
            "snapshot": combined,
            "settings": {
                "current_live_source": "provider:ecowitt",
                "current_condition_source": "provider:weather_api",
                "forecast_provider": "weather_api",
            },
            "selected_sensors": [],
            "provider_status": [],
            "received_at": combined["received_at"],
            "stale": False,
        }

    def test_current_temperature_matches_the_ui_readings_source(self) -> None:
        result = self.core._environment_conditions_kernel(
            {"category": "temperature", "area": "outside"},
            payload=self._payload(),
        )
        self.assertTrue(result["ok"])
        self.assertEqual([row["value"] for row in result["readings"]], ["105 °F"])
        self.assertEqual(result["readings"][0]["provider"], "Ecowitt")
        self.assertEqual(result["current_sources"]["readings"]["selection"], "provider:ecowitt")
        self.assertNotIn("area", result["filters"])

    def test_broad_current_weather_combines_configured_readings_and_artwork_sources(self) -> None:
        result = self.core._environment_conditions_kernel(
            {"category": "weather", "location": "home"},
            payload=self._payload(),
        )
        values = {row["value"] for row in result["readings"]}
        self.assertIn("105 °F", values)
        self.assertIn("Partly Cloudy", values)
        self.assertNotIn("78 °F", values)
        self.assertNotIn("72 °F", values)
        self.assertEqual(result["current_sources"]["conditions"]["selection"], "provider:weather_api")

    def test_explicit_provider_or_room_still_searches_all_sources(self) -> None:
        weather_api = self.core._environment_conditions_kernel(
            {"category": "temperature", "provider": "weather_api"},
            payload=self._payload(),
        )
        self.assertEqual([row["value"] for row in weather_api["readings"]], ["78 °F"])
        self.assertEqual(weather_api["current_sources"], {})

        bedroom = self.core._environment_conditions_kernel(
            {"category": "temperature", "area": "Bedroom"},
            payload=self._payload(),
        )
        self.assertEqual([row["value"] for row in bedroom["readings"]], ["69 °F"])
        self.assertEqual(bedroom["readings"][0]["provider"], "Home Assistant")

    def test_kernel_description_explains_current_card_source_behavior(self) -> None:
        tool = next(
            row
            for row in self.core.get_hydra_kernel_tools()
            if row["id"] == "environment_conditions"
        )
        self.assertIn("Current Card Readings Source", tool["description"])
        self.assertIn("forecast provider", tool["description"])


if __name__ == "__main__":
    unittest.main()
