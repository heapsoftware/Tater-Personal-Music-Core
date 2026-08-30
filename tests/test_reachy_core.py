from __future__ import annotations

import importlib.util
import threading
from pathlib import Path

import pytest


CORE_PATH = Path(__file__).parents[1] / "cores" / "reachy_core.py"
SPEC = importlib.util.spec_from_file_location("reachy_core_under_test", CORE_PATH)
assert SPEC is not None and SPEC.loader is not None
reachy_core = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(reachy_core)


def test_reachy_core_exposes_runnable_shop_contract() -> None:
    stop_event = threading.Event()
    stop_event.set()

    reachy_core.run(stop_event=stop_event)

    assert callable(reachy_core.run)


def _client(*, supported: bool = True) -> dict:
    capabilities = {"speaker": True}
    if supported:
        capabilities.update({"reachy_settings": True, "reachy_settings_version": 1})
    return {
        "selector": "native:reachy-office",
        "connected": True,
        "device_name": "Office Reachy",
        "room": "office",
        "board": "reachy_mini",
        "capabilities": capabilities,
    }


def _settings_response() -> dict:
    return {
        "ok": True,
        "protocol_version": 1,
        "app": {"kind": "satellite", "version": "0.4.0"},
        "status": {"connection_state": "connected"},
        "settings": {
            "reachy": {
                "allow_vision_snapshots": True,
                "auto_level_head": True,
                "head_level_gain": 0.65,
                "head_level_max_correction_deg": 15,
            },
            "motion": {
                "enabled": True,
                "intensity": 0.65,
                "idle_enabled": True,
                "speaking_enabled": True,
                "thinking_enabled": True,
                "music_dance_enabled": True,
                "music_dance_intensity": 0.78,
                "music_reactions_enabled": True,
            },
            "watch": {"enabled": True, "track_during_voice": True},
            "idle_life": {"enabled": True, "look_around_enabled": True},
        },
    }


def test_reachy_core_builds_settings_cards_for_compatible_reachy(monkeypatch) -> None:
    monkeypatch.setattr(
        reachy_core,
        "_native_snapshot",
        lambda: {"clients": {"native:reachy-office": _client()}},
    )
    monkeypatch.setattr(reachy_core, "_native_request", lambda *_args, **_kwargs: _settings_response())

    result = reachy_core.get_htmlui_tab_data()

    assert result["stats"] == [
        {"label": "Reachys", "value": 1},
        {"label": "Ready", "value": 1},
        {"label": "Update Needed", "value": 0},
    ]
    forms = result["ui"]["item_forms"]
    assert [form["title"] for form in forms] == [
        "Office Reachy",
        "Vision and Head",
        "Person Tracking",
        "Motion and Music",
        "Idle Life",
    ]
    idle_fields = {field["key"]: field["value"] for field in forms[-1]["fields"]}
    assert idle_fields["enabled"] is True
    assert idle_fields["look_around_enabled"] is True


def test_reachy_core_shows_update_needed_for_old_reachy(monkeypatch) -> None:
    monkeypatch.setattr(
        reachy_core,
        "_native_snapshot",
        lambda: {"clients": {"native:reachy-office": _client(supported=False)}},
    )

    result = reachy_core.get_htmlui_tab_data()

    assert result["stats"][2]["value"] == 1
    assert result["ui"]["item_forms"][0]["subtitle"] == "Reachy app update required"


def test_reachy_core_routes_section_save_to_native_settings(monkeypatch) -> None:
    calls = []

    def request(selector, message_type, payload, **_kwargs):
        calls.append((selector, message_type, payload))
        return {"ok": True, "message": "saved"}

    monkeypatch.setattr(reachy_core, "_native_request", request)
    card_id = reachy_core._card_id("native:reachy-office", "idle_life")

    result = reachy_core.handle_htmlui_tab_action(
        action="reachy_save_settings",
        payload={"id": card_id, "values": {"enabled": True}},
    )

    assert result["ok"] is True
    assert calls == [
        (
            "native:reachy-office",
            "reachy.settings.update",
            {"sections": {"idle_life": {"enabled": True}}},
        )
    ]


def test_reachy_core_surfaces_native_errors(monkeypatch) -> None:
    monkeypatch.setattr(
        reachy_core,
        "_native_request",
        lambda *_args, **_kwargs: {"ok": False, "error": "Reachy is restarting"},
    )
    card_id = reachy_core._card_id("native:reachy-office", "motion")

    with pytest.raises(ValueError, match="Reachy is restarting"):
        reachy_core.handle_htmlui_tab_action(
            action="reachy_save_settings",
            payload={"id": card_id, "values": {"enabled": True}},
        )
