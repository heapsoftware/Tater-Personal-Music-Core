from __future__ import annotations

import asyncio
import base64
import importlib.util
import sys
import threading
import time
import types
from pathlib import Path
from types import SimpleNamespace

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
            "face_id": {
                "enabled": True,
                "capture_mode": "clip",
                "greetings_enabled": True,
                "conversation_identity_enabled": True,
                "scan_interval_seconds": 60,
                "greeting_cooldown_seconds": 1800,
                "context_ttl_seconds": 300,
            },
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
        "Face ID",
    ]
    idle_fields = {field["key"]: field["value"] for field in forms[-2]["fields"]}
    assert idle_fields["enabled"] is True
    assert idle_fields["look_around_enabled"] is True
    reachy_tab = result["ui"]["manager_tabs"][0]
    assert reachy_tab["source"] == "grouped_items"
    assert [group["label"] for group in reachy_tab["groups"]] == [
        "Overview",
        "Vision & Head",
        "Tracking",
        "Motion & Music",
        "Idle Life",
        "Face ID",
    ]
    assert forms[0]["group"].endswith("::overview")
    assert forms[-1]["group"].endswith("::face_id")
    face_fields = {field["key"]: field for field in forms[-1]["fields"]}
    assert face_fields["enabled"]["value"] is True
    assert face_fields["capture_mode"]["value"] == "clip"
    assert face_fields["capture_mode"]["options"] == [
        {"value": "snapshot", "label": "Snapshot"},
        {"value": "clip", "label": "Short Clip"},
    ]


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


def test_reachy_core_matches_a_visible_person_with_shared_face_id(monkeypatch) -> None:
    selector = "native:reachy-office"
    state = reachy_core._face_state(selector)
    state.update(
        {
            "settings": _settings_response()["settings"],
            "scan_active": True,
            "next_scan_at": 0.0,
        }
    )
    row = {
        **_client(),
        "last_status": {
            "reachy": {"face_visible": True, "reported_at": time.time()}
        },
        "voice": {"active": False},
        "media_session": {"active": False},
        "audio_overlay": {"active": False},
    }
    fake_face_identity = types.ModuleType("face_identity")
    fake_face_identity.recognize_image = lambda *_args, **_kwargs: {
        "status": "recognized",
        "identity_ids": ["face-1"],
        "person_ids": ["person-1"],
        "people": ["Spud Lord"],
        "faces_detected": 1,
    }
    monkeypatch.setitem(sys.modules, "face_identity", fake_face_identity)
    monkeypatch.setattr(reachy_core, "_capture_face_frames", lambda *_args: [b"jpeg"])
    monkeypatch.setattr(reachy_core, "_current_client", lambda *_args: row)
    monkeypatch.setattr(reachy_core, "_speak_face_greeting", lambda *_args: False)

    reachy_core._recognize_face_worker(selector, "Office Reachy")

    assert state["scan_active"] is False
    assert state["person_id"] == "person-1"
    assert state["person_name"] == "Spud Lord"
    assert state["identity_ids"] == ["face-1"]
    assert state["expires_at"] > time.time()
    assert state["greeting_pending"] is True
    assert "Spud Lord" in state["pending_greeting_text"]


def test_matched_person_is_not_rechecked_until_tracking_is_lost(monkeypatch) -> None:
    selector = "native:reachy-session-lock"
    row = {
        **_client(),
        "selector": selector,
        "capabilities": {
            **_client()["capabilities"],
            "camera_snapshot": True,
        },
        "last_seen_ts": time.time(),
        "last_status": {
            "reachy": {"face_visible": True, "face_id_ready": True}
        },
        "voice": {"active": False},
        "media_session": {"active": False},
        "audio_overlay": {"active": False},
    }
    state = reachy_core._face_state(selector)
    state.update(
        {
            "settings": _settings_response()["settings"],
            "next_settings_at": float("inf"),
            "next_scan_at": 0.0,
            "scan_active": False,
            "person_id": "person-1",
            "person_name": "Spud Lord",
            "identity_ids": ["face-1"],
            "expires_at": 1.0,
            "tracking_visible": True,
        }
    )
    started = []

    class FakeThread:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def start(self) -> None:
            started.append(self.kwargs)

    monkeypatch.setattr(reachy_core, "_connected_reachys", lambda: [row])
    monkeypatch.setattr(reachy_core.threading, "Thread", FakeThread)

    try:
        reachy_core._face_id_tick()

        assert started == []
        assert state["person_id"] == "person-1"
        assert state["expires_at"] > time.time()

        row["last_status"]["reachy"]["face_visible"] = False
        row["last_status"]["reachy"]["face_id_ready"] = False
        row["last_seen_ts"] = time.time()
        reachy_core._face_id_tick()

        assert state["person_id"] == ""
        assert state["tracking_visible"] is False

        row["last_status"]["reachy"]["face_visible"] = True
        row["last_status"]["reachy"]["face_id_ready"] = True
        row["last_seen_ts"] = time.time()
        reachy_core._face_id_tick()

        assert len(started) == 1
        assert state["scan_active"] is True
    finally:
        reachy_core._FACE_STATES.pop(selector, None)


def test_random_greeting_runs_once_per_tracking_session(monkeypatch) -> None:
    selector = "native:reachy-greeting-session"
    row = {
        **_client(),
        "selector": selector,
        "capabilities": {
            **_client()["capabilities"],
            "camera_snapshot": True,
        },
        "last_seen_ts": time.time(),
        "last_status": {
            "reachy": {"face_visible": True, "face_id_ready": True}
        },
        "voice": {"active": False},
        "media_session": {"active": False},
        "audio_overlay": {"active": False},
    }
    state = reachy_core._face_state(selector)
    state.update(
        {
            "settings": _settings_response()["settings"],
            "next_settings_at": float("inf"),
            "person_id": "person-1",
            "person_name": "Spud Lord",
            "identity_ids": ["face-1"],
            "tracking_visible": True,
        }
    )
    reachy_core._queue_face_greeting(state, "Spud Lord")
    first_text = state["pending_greeting_text"]
    spoken = []

    class ImmediateThread:
        def __init__(self, *, target, args, **_kwargs):
            self.target = target
            self.args = args

        def start(self) -> None:
            self.target(*self.args)

    monkeypatch.setattr(reachy_core, "_connected_reachys", lambda: [row])
    monkeypatch.setattr(reachy_core.threading, "Thread", ImmediateThread)
    monkeypatch.setattr(
        reachy_core,
        "_speak_face_greeting",
        lambda *args: spoken.append(args) or True,
    )

    try:
        reachy_core._face_id_tick()
        reachy_core._face_id_tick()

        assert len(spoken) == 1
        assert state["greeting_pending"] is False
        assert state["last_greeting_template"]

        row["last_status"]["reachy"]["face_visible"] = False
        row["last_status"]["reachy"]["face_id_ready"] = False
        row["last_seen_ts"] = time.time()
        reachy_core._face_id_tick()
        assert state["person_id"] == ""

        state["person_id"] = "person-1"
        state["person_name"] = "Spud Lord"
        state["tracking_visible"] = True
        reachy_core._queue_face_greeting(state, "Spud Lord")

        assert state["pending_greeting_text"] != first_text
    finally:
        reachy_core._FACE_STATES.pop(selector, None)


def test_reachy_core_requires_reachys_local_good_face_signal() -> None:
    now = time.time()
    row = {
        "last_seen_ts": now,
        "last_status": {
            "reachy": {
                "face_visible": True,
                "face_id_ready": True,
            }
        },
    }

    assert reachy_core._face_id_ready(row, now=now) is True
    row["last_status"]["reachy"]["face_id_ready"] = False
    assert reachy_core._face_id_ready(row, now=now) is False


def test_reachy_core_does_not_request_camera_without_a_good_face(monkeypatch) -> None:
    now = time.time()
    row = {
        "last_seen_ts": now,
        "last_status": {
            "reachy": {"face_visible": True, "face_id_ready": False}
        },
    }
    requests = []
    monkeypatch.setattr(reachy_core, "_current_client", lambda *_args: row)
    monkeypatch.setattr(
        reachy_core,
        "_native_request",
        lambda *args, **_kwargs: requests.append(args) or {
            "ok": True,
            "image_base64": base64.b64encode(b"jpeg").decode("ascii"),
        },
    )

    assert reachy_core._capture_face_frames("native:reachy-office", "snapshot") == []
    assert requests == []

    row["last_status"]["reachy"]["face_id_ready"] = True
    assert reachy_core._capture_face_frames("native:reachy-office", "snapshot") == [b"jpeg"]
    assert len(requests) == 1


def test_reachy_core_injects_matched_person_into_reachy_voice_session(monkeypatch) -> None:
    selector = "native:reachy-office"
    session = SimpleNamespace(
        session_id="voice-session-1",
        speaker_id="",
        speaker_name="",
        speaker_score=0.0,
        speaker_match_reason="",
        context={},
    )
    runtime = {"lock": asyncio.Lock(), "session": session}
    attached = []

    fake_people = types.ModuleType("people")
    fake_people.attach_alias = lambda **kwargs: attached.append(kwargs)
    fake_voice_pipeline = types.ModuleType("tater_voice.voice_pipeline")
    fake_voice_pipeline._selector_runtime = lambda _selector: runtime
    fake_native = types.ModuleType("tater_voice.native_satellite")
    fake_native.run_on_runtime_loop = lambda coroutine, timeout=0: asyncio.run(coroutine)
    fake_tater_voice = types.ModuleType("tater_voice")
    fake_tater_voice.voice_pipeline = fake_voice_pipeline
    fake_tater_voice.native_satellite = fake_native
    monkeypatch.setitem(sys.modules, "people", fake_people)
    monkeypatch.setitem(sys.modules, "tater_voice", fake_tater_voice)
    monkeypatch.setitem(sys.modules, "tater_voice.voice_pipeline", fake_voice_pipeline)
    monkeypatch.setitem(sys.modules, "tater_voice.native_satellite", fake_native)

    state = {
        "person_id": "person-1",
        "person_name": "Spud Lord",
        "alias_person_id": "",
        "injected_session_id": "",
    }
    reachy_core._inject_face_identity(selector, session.session_id, state)

    assert attached[0]["person_id"] == "person-1"
    assert attached[0]["platform"] == "voice_core"
    assert session.speaker_name == "Spud Lord"
    assert session.context["speaker_id"].startswith("reachy-face:")
    assert state["injected_session_id"] == session.session_id
