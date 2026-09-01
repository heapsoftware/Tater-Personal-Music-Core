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
                "store_photos": False,
                "greetings_enabled": True,
                "conversation_identity_enabled": True,
                "context_ttl_seconds": 300,
            },
        },
    }


def test_reachy_core_builds_settings_cards_for_compatible_reachy(monkeypatch) -> None:
    fake_people = types.ModuleType("people")
    fake_people.load_store = lambda *_args, **_kwargs: {
        "people": [{"id": "person-1", "display_name": "Spud Lord"}]
    }
    monkeypatch.setitem(sys.modules, "people", fake_people)
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
        "Add a Face",
    ]
    idle_card = next(form for form in forms if form["title"] == "Idle Life")
    idle_fields = {field["key"]: field["value"] for field in idle_card["fields"]}
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
    face_card = next(form for form in forms if form["title"] == "Face ID")
    face_fields = {field["key"]: field for field in face_card["fields"]}
    assert face_fields["enabled"]["value"] is True
    assert face_fields["store_photos"]["value"] is False
    assert "capture_mode" not in face_fields
    assert "scan_interval_seconds" not in face_fields
    enrollment = forms[-1]
    enrollment_fields = {field["key"]: field for field in enrollment["fields"]}
    assert enrollment["save_action"] == "reachy_face_enroll"
    assert enrollment_fields["person_id"]["options"][-1] == {
        "value": "person-1",
        "label": "Spud Lord",
    }
    assert enrollment_fields["face_image"]["camera_capture"] is True


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


def _face_upload(payload: bytes = b"jpeg-data") -> dict:
    return {
        "filename": "face.jpg",
        "content_type": "image/jpeg",
        "size": len(payload),
        "data_b64": base64.b64encode(payload).decode("ascii"),
    }


def test_reachy_core_adds_uploaded_face_to_selected_person(monkeypatch) -> None:
    fake_people = types.ModuleType("people")
    fake_people.load_store = lambda *_args, **_kwargs: {
        "people": [{"id": "person-1", "display_name": "Spud Lord"}]
    }
    fake_face_identity = types.ModuleType("face_identity")
    record_values = []

    def recognize_image(image_bytes, **kwargs):
        assert image_bytes == b"jpeg-data"
        record_values.append(kwargs.get("record"))
        return {
            "status": "unrecognized",
            "identity_ids": ["face-new"] if kwargs.get("record") else [],
            "person_ids": [],
            "people": [],
            "faces_detected": 1,
        }

    saved = []
    fake_face_identity.recognize_image = recognize_image
    fake_face_identity.identity_rows = lambda *_args, **_kwargs: {}
    fake_face_identity.save_profile = lambda identity_id, **kwargs: saved.append(
        (identity_id, kwargs)
    ) or {"id": identity_id, "person_id": kwargs["person_id"]}
    fake_face_identity.person_name = lambda *_args, **_kwargs: ""
    monkeypatch.setitem(sys.modules, "people", fake_people)
    monkeypatch.setitem(sys.modules, "face_identity", fake_face_identity)

    result = reachy_core.handle_htmlui_tab_action(
        action="reachy_face_enroll",
        payload={
            "id": reachy_core._card_id("native:reachy-office", "face_enrollment"),
            "values": {"person_id": "person-1", "face_image": _face_upload()},
        },
        redis_client="redis-test",
    )

    assert result["ok"] is True
    assert result["identity_id"] == "face-new"
    assert result["person_name"] == "Spud Lord"
    assert record_values == [False, True]
    assert saved == [
        (
            "face-new",
            {
                "person_id": "person-1",
                "person_link_supplied": True,
                "redis_client": "redis-test",
            },
        )
    ]


def test_reachy_core_rejects_face_photo_with_multiple_people(monkeypatch) -> None:
    fake_people = types.ModuleType("people")
    fake_people.load_store = lambda *_args, **_kwargs: {
        "people": [{"id": "person-1", "display_name": "Spud Lord"}]
    }
    fake_face_identity = types.ModuleType("face_identity")
    record_values = []

    def recognize_image(*_args, **kwargs):
        record_values.append(kwargs.get("record"))
        return {"status": "unrecognized", "identity_ids": [], "faces_detected": 2}

    fake_face_identity.recognize_image = recognize_image
    monkeypatch.setitem(sys.modules, "people", fake_people)
    monkeypatch.setitem(sys.modules, "face_identity", fake_face_identity)

    with pytest.raises(ValueError, match="More than one face"):
        reachy_core.handle_htmlui_tab_action(
            action="reachy_face_enroll",
            payload={
                "id": reachy_core._card_id("native:reachy-office", "face_enrollment"),
                "values": {"person_id": "person-1", "face_image": _face_upload()},
            },
        )

    assert record_values == [False]


def test_reachy_core_does_not_reassign_a_face_linked_to_another_person(monkeypatch) -> None:
    fake_people = types.ModuleType("people")
    fake_people.load_store = lambda *_args, **_kwargs: {
        "people": [
            {"id": "person-1", "display_name": "Spud Lord"},
            {"id": "person-2", "display_name": "Tater Tot"},
        ]
    }
    fake_face_identity = types.ModuleType("face_identity")
    record_values = []

    def recognize_image(*_args, **kwargs):
        record_values.append(kwargs.get("record"))
        return {"status": "recognized", "identity_ids": ["face-2"], "faces_detected": 1}

    fake_face_identity.recognize_image = recognize_image
    fake_face_identity.identity_rows = lambda *_args, **_kwargs: {
        "face-2": {"id": "face-2", "person_id": "person-2"}
    }
    fake_face_identity.person_name = lambda *_args, **_kwargs: "Tater Tot"
    monkeypatch.setitem(sys.modules, "people", fake_people)
    monkeypatch.setitem(sys.modules, "face_identity", fake_face_identity)

    with pytest.raises(ValueError, match="already linked to Tater Tot"):
        reachy_core.handle_htmlui_tab_action(
            action="reachy_face_enroll",
            payload={
                "id": reachy_core._card_id("native:reachy-office", "face_enrollment"),
                "values": {"person_id": "person-1", "face_image": _face_upload()},
            },
        )

    assert record_values == [False]


def test_reachy_core_matches_a_visible_person_with_shared_face_id(monkeypatch) -> None:
    selector = "native:reachy-office"
    settings = _settings_response()["settings"]
    settings["face_id"]["store_photos"] = True
    state = reachy_core._face_state(selector)
    state.update(
        {
            "settings": settings,
            "scan_active": True,
            "next_scan_at": 0.0,
            "tracking_visible": True,
            "face_currently_visible": True,
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
    record_values = []

    def recognize_image(*_args, **kwargs):
        record_values.append(kwargs.get("record"))
        return {
            "status": "recognized",
            "identity_ids": ["face-1"],
            "person_ids": ["person-1"],
            "people": ["Spud Lord"],
            "faces_detected": 1,
        }

    fake_face_identity.recognize_image = recognize_image
    monkeypatch.setitem(sys.modules, "face_identity", fake_face_identity)
    monkeypatch.setattr(reachy_core, "_capture_face_image", lambda *_args: b"jpeg")
    monkeypatch.setattr(reachy_core, "_current_client", lambda *_args: row)
    monkeypatch.setattr(reachy_core, "_speak_face_greeting", lambda *_args: False)

    reachy_core._recognize_face_worker(selector, "Office Reachy")

    assert state["scan_active"] is False
    assert state["person_id"] == "person-1"
    assert state["person_name"] == "Spud Lord"
    assert state["identity_ids"] == ["face-1"]
    assert state["expires_at"] > time.time()
    assert state["face_scan_complete"] is True
    assert state["greeting_pending"] is True
    assert "Spud Lord" in state["pending_greeting_text"]
    assert record_values == [False, True]


def test_reachy_core_never_stores_a_multi_person_snapshot(monkeypatch) -> None:
    selector = "native:reachy-multiple-faces"
    settings = _settings_response()["settings"]
    settings["face_id"]["store_photos"] = True
    state = reachy_core._face_state(selector)
    state.update(
        {
            "settings": settings,
            "scan_active": True,
            "tracking_visible": True,
            "face_currently_visible": True,
        }
    )
    row = {
        **_client(),
        "selector": selector,
        "last_status": {"reachy": {"face_visible": True}},
    }
    record_values = []
    fake_face_identity = types.ModuleType("face_identity")

    def recognize_image(*_args, **kwargs):
        record_values.append(kwargs.get("record"))
        return {
            "status": "unrecognized",
            "identity_ids": [],
            "person_ids": [],
            "people": [],
            "faces_detected": 2,
        }

    fake_face_identity.recognize_image = recognize_image
    monkeypatch.setitem(sys.modules, "face_identity", fake_face_identity)
    monkeypatch.setattr(reachy_core, "_capture_face_image", lambda *_args: b"jpeg")
    monkeypatch.setattr(reachy_core, "_current_client", lambda *_args: row)

    try:
        reachy_core._recognize_face_worker(selector, "Office Reachy")

        assert record_values == [False]
        assert state["person_id"] == ""
        assert state["face_scan_complete"] is True
    finally:
        reachy_core._FACE_STATES.pop(selector, None)


@pytest.mark.parametrize(
    ("existing_identity_ids", "expected_record_values"),
    [
        (["face-existing-unknown"], [False]),
        ([], [False]),
    ],
)
def test_unknown_face_completes_tracking_session_without_saving_images(
    monkeypatch,
    existing_identity_ids,
    expected_record_values,
) -> None:
    suffix = "existing" if existing_identity_ids else "new"
    selector = f"native:reachy-unknown-{suffix}"
    state = reachy_core._face_state(selector)
    state.update(
        {
            "settings": _settings_response()["settings"],
            "next_settings_at": float("inf"),
            "scan_active": True,
            "tracking_visible": True,
        }
    )
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
    record_values = []

    fake_face_identity = types.ModuleType("face_identity")

    def recognize_image(*_args, **kwargs):
        record = bool(kwargs.get("record"))
        record_values.append(record)
        return {
            "status": "unrecognized",
            "identity_ids": (
                ["face-new-unknown"] if record else list(existing_identity_ids)
            ),
            "person_ids": [],
            "people": [],
            "faces_detected": 1,
        }

    fake_face_identity.recognize_image = recognize_image
    monkeypatch.setitem(sys.modules, "face_identity", fake_face_identity)
    monkeypatch.setattr(reachy_core, "_capture_face_image", lambda *_args: b"jpeg")
    monkeypatch.setattr(reachy_core, "_current_client", lambda *_args: row)

    started = []

    class FakeThread:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def start(self) -> None:
            started.append(self.kwargs)

    monkeypatch.setattr(reachy_core, "_connected_reachys", lambda: [row])
    monkeypatch.setattr(reachy_core.threading, "Thread", FakeThread)

    try:
        reachy_core._recognize_face_worker(selector, "Office Reachy")

        assert record_values == expected_record_values
        assert state["person_id"] == ""
        assert state["face_scan_complete"] is True

        reachy_core._face_id_tick()
        assert started == []
    finally:
        reachy_core._FACE_STATES.pop(selector, None)


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

        assert state["person_id"] == "person-1"
        assert state["tracking_visible"] is True
        assert state["face_scan_complete"] is False
        assert state["face_missing_since"] > 0.0

        row["last_status"]["reachy"]["face_visible"] = True
        row["last_status"]["reachy"]["face_id_ready"] = True
        row["last_seen_ts"] = time.time()
        reachy_core._face_id_tick()

        assert state["person_id"] == "person-1"
        assert state["face_missing_since"] == 0.0
        assert started == []

        row["last_status"]["reachy"]["face_visible"] = False
        row["last_status"]["reachy"]["face_id_ready"] = False
        row["last_seen_ts"] = time.time()
        state["face_missing_since"] = time.time() - reachy_core._FACE_SESSION_LOSS_SECONDS - 1.0
        reachy_core._face_id_tick()

        assert state["person_id"] == ""
        assert state["tracking_visible"] is False
        assert state["face_scan_complete"] is False

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
        assert state["person_id"] == "person-1"

        row["last_status"]["reachy"]["face_visible"] = True
        row["last_status"]["reachy"]["face_id_ready"] = True
        row["last_seen_ts"] = time.time()
        reachy_core._face_id_tick()
        assert state["person_id"] == "person-1"
        assert len(spoken) == 1

        row["last_status"]["reachy"]["face_visible"] = False
        row["last_status"]["reachy"]["face_id_ready"] = False
        row["last_seen_ts"] = time.time()
        state["face_missing_since"] = time.time() - reachy_core._FACE_SESSION_LOSS_SECONDS - 1.0
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

    assert reachy_core._capture_face_image("native:reachy-office") == b""
    assert requests == []

    row["last_status"]["reachy"]["face_id_ready"] = True
    assert reachy_core._capture_face_image("native:reachy-office") == b"jpeg"
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
