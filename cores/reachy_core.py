"""Manage compatible Reachy Mini apps through Tater's native satellite link."""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List


__version__ = "1.1.0"
MIN_TATER_VERSION = "99.5"
CORE_DESCRIPTION = (
    "Control Reachy Tater Satellite and Reachy Tater Embedded behavior directly "
    "from Tater, including tracking, motion, music reactions, vision, head leveling, "
    "and Idle Life."
)
TAGS = ["reachy", "reachy-mini", "robot", "satellite", "idle-life", "motion"]

CORE_SETTINGS = {
    "category": "Reachy Core Settings",
    "required": {},
}

CORE_WEBUI_TAB = {
    "label": "Reachy",
    "order": 24,
    "requires_running": False,
}

_SETTINGS_CAPABILITY = "reachy_settings"
_SETTINGS_PROTOCOL_VERSION = 1
_SUPPORTED_SECTIONS = {"motion", "watch", "idle_life", "reachy"}

logger = logging.getLogger("reachy_core")


def _text(value: Any) -> str:
    return str(value or "").strip()


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _native_snapshot() -> Dict[str, Any]:
    from tater_voice import native_satellite

    result = native_satellite.status_snapshot_sync()
    return result if isinstance(result, dict) else {}


def _native_request(
    selector: str,
    message_type: str,
    payload: Dict[str, Any] | None = None,
    *,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    from tater_voice import native_satellite

    result = native_satellite.run_on_runtime_loop(
        native_satellite.send_request(
            selector,
            message_type,
            payload or {},
            timeout_s=timeout,
        ),
        timeout=timeout + 1.0,
    )
    return result if isinstance(result, dict) else {}


def _connected_reachys() -> List[Dict[str, Any]]:
    try:
        snapshot = _native_snapshot()
    except Exception:
        return []
    raw_clients = snapshot.get("clients")
    if not isinstance(raw_clients, dict):
        return []
    rows: List[Dict[str, Any]] = []
    for selector, raw in raw_clients.items():
        row = raw if isinstance(raw, dict) else {}
        board = _text(row.get("board")).lower().replace("-", "_")
        if not _as_bool(row.get("connected"), False) or board != "reachy_mini":
            continue
        capabilities = row.get("capabilities") if isinstance(row.get("capabilities"), dict) else {}
        rows.append(
            {
                **row,
                "selector": _text(row.get("selector") or selector),
                "capabilities": capabilities,
                "settings_supported": _as_bool(capabilities.get(_SETTINGS_CAPABILITY), False),
                "settings_version": int(_as_float(capabilities.get("reachy_settings_version"), 0)),
            }
        )
    return sorted(rows, key=lambda row: (_device_name(row).lower(), _text(row.get("selector"))))


def _device_name(row: Dict[str, Any]) -> str:
    return _text(row.get("device_name") or row.get("name") or row.get("selector")) or "Reachy Mini"


def _card_id(selector: str, section: str) -> str:
    return json.dumps(
        {"selector": selector, "section": section},
        separators=(",", ":"),
        sort_keys=True,
    )


def _section_group(selector: str, section: str) -> str:
    return f"{selector}::{section}"


def _parse_card_id(value: Any) -> tuple[str, str]:
    try:
        parsed = json.loads(_text(value))
    except Exception as exc:
        raise ValueError("Reachy card identifier is invalid. Refresh the page and try again.") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Reachy card identifier is invalid. Refresh the page and try again.")
    selector = _text(parsed.get("selector"))
    section = _text(parsed.get("section"))
    if not selector or not section:
        raise ValueError("Reachy card identifier is incomplete. Refresh the page and try again.")
    return selector, section


def _field(
    key: str,
    label: str,
    field_type: str,
    value: Any,
    *,
    description: str = "",
    minimum: float | None = None,
    maximum: float | None = None,
    step: float | None = None,
    suffix: str = "",
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        "key": key,
        "label": label,
        "type": field_type,
        "value": value,
    }
    if description:
        row["description"] = description
    if minimum is not None:
        row["min"] = minimum
    if maximum is not None:
        row["max"] = maximum
    if step is not None:
        row["step"] = step
    if suffix:
        row["suffix"] = suffix
    return row


def _device_card_base(
    row: Dict[str, Any],
    *,
    section: str,
    title: str,
    subtitle: str,
) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    return {
        "id": _card_id(selector, section),
        "group": _section_group(selector, section),
        "title": title,
        "subtitle": subtitle,
        "save_action": "reachy_save_settings",
        "save_label": "Save and Apply",
        "actions": [
            {
                "action": "reachy_reset_settings",
                "label": "Reset Section",
                "confirm": f"Restore the default {title.lower()} settings on {_device_name(row)}?",
            }
        ],
    }


def _vision_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="reachy",
        title="Vision and Head",
        subtitle="Camera permission and automatic head leveling owned by the Reachy app.",
    )
    card["fields"] = [
        _field(
            "allow_vision_snapshots",
            "Allow Vision Snapshots",
            "checkbox",
            _as_bool(values.get("allow_vision_snapshots")),
            description="Allow this paired Tater to request fresh still images for Reachy Vision and ambient comments.",
        ),
        _field(
            "auto_level_head",
            "Automatically Straighten Head",
            "checkbox",
            _as_bool(values.get("auto_level_head"), True),
            description="Measure and correct Reachy's resting head roll when the app starts.",
        ),
        _field(
            "head_level_gain",
            "Head Level Correction Strength",
            "number",
            _as_float(values.get("head_level_gain"), 0.65),
            minimum=0,
            maximum=1,
            step=0.05,
        ),
        _field(
            "head_level_max_correction_deg",
            "Maximum Head Level Correction",
            "number",
            _as_float(values.get("head_level_max_correction_deg"), 15),
            minimum=0,
            maximum=45,
            step=1,
            suffix="°",
        ),
    ]
    return card


def _motion_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="motion",
        title="Motion and Music",
        subtitle="Control expressive animations, talking motion, and music personality.",
    )
    card["fields"] = [
        _field("enabled", "Enable Reachy Motion", "checkbox", _as_bool(values.get("enabled"), True)),
        _field("intensity", "Overall Motion Intensity", "number", _as_float(values.get("intensity"), 0.65), minimum=0, maximum=1, step=0.05),
        _field("idle_enabled", "Idle Motion", "checkbox", _as_bool(values.get("idle_enabled"), True)),
        _field("speaking_enabled", "Talking Animations", "checkbox", _as_bool(values.get("speaking_enabled"), True)),
        _field("thinking_enabled", "Thinking and Tool Animations", "checkbox", _as_bool(values.get("thinking_enabled"), True)),
        _field("music_dance_enabled", "Dance to Music", "checkbox", _as_bool(values.get("music_dance_enabled"), True)),
        _field("music_dance_intensity", "Music Dance Intensity", "number", _as_float(values.get("music_dance_intensity"), 0.78), minimum=0, maximum=1, step=0.05),
        _field("music_reactions_enabled", "Spoken Song Reactions", "checkbox", _as_bool(values.get("music_reactions_enabled"), True)),
    ]
    return card


def _tracking_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="watch",
        title="Person Tracking",
        subtitle="Keep Reachy's gaze on people while the head leads and the body follows only when needed.",
    )
    card["fields"] = [
        _field("enabled", "Track People", "checkbox", _as_bool(values.get("enabled"), True)),
        _field("track_during_voice", "Track During Voice Turns", "checkbox", _as_bool(values.get("track_during_voice"), True)),
        _field("poll_seconds", "Tracking Update Interval", "number", _as_float(values.get("poll_seconds"), 0.2), minimum=0.1, maximum=2, step=0.05, suffix="sec"),
        _field("tracking_weight", "Idle Tracking Strength", "number", _as_float(values.get("tracking_weight"), 1), minimum=0, maximum=1, step=0.05),
        _field("listening_tracking_weight", "Listening Tracking Strength", "number", _as_float(values.get("listening_tracking_weight"), 1), minimum=0, maximum=1, step=0.05),
        _field("voice_tracking_weight", "Speaking/Thinking Tracking Strength", "number", _as_float(values.get("voice_tracking_weight"), 1), minimum=0, maximum=1, step=0.05),
        _field("body_follow_start_deg", "Body Follow Start", "number", _as_float(values.get("body_follow_start_deg"), 32), minimum=5, maximum=90, step=1, suffix="°"),
        _field("body_follow_target_deg", "Body Follow Target", "number", _as_float(values.get("body_follow_target_deg"), 18), minimum=0, maximum=60, step=1, suffix="°"),
        _field("body_follow_gain", "Body Follow Gain", "number", _as_float(values.get("body_follow_gain"), 0.45), minimum=0.05, maximum=1, step=0.05),
        _field("body_follow_max_step_deg", "Maximum Body Step", "number", _as_float(values.get("body_follow_max_step_deg"), 5), minimum=0.25, maximum=20, step=0.25, suffix="°"),
        _field("body_yaw_limit_deg", "Body Turn Limit", "number", _as_float(values.get("body_yaw_limit_deg"), 120), minimum=30, maximum=180, step=5, suffix="°"),
        _field("body_recenter_delay_seconds", "Body Recenter Delay", "number", _as_float(values.get("body_recenter_delay_seconds"), 3), minimum=0, maximum=60, step=0.5, suffix="sec"),
        _field("body_recenter_step_deg", "Body Recenter Step", "number", _as_float(values.get("body_recenter_step_deg"), 3), minimum=0.25, maximum=20, step=0.25, suffix="°"),
    ]
    return card


def _idle_life_card(row: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
    card = _device_card_base(
        row,
        section="idle_life",
        title="Idle Life",
        subtitle="Give Reachy natural room exploration, spontaneous observations, and optional naps.",
    )
    card["actions"].insert(
        0,
        {
            "action": "reachy_test_ambient_comment",
            "label": "Test Ambient Comment",
        },
    )
    card["fields"] = [
        _field("enabled", "Enable Idle Life", "checkbox", _as_bool(values.get("enabled"))),
        _field("look_around_enabled", "Explore the Room", "checkbox", _as_bool(values.get("look_around_enabled"))),
        _field("ambient_comments_enabled", "Spontaneous Camera Comments", "checkbox", _as_bool(values.get("ambient_comments_enabled"))),
        _field("sleep_enabled", "Occasionally Go to Sleep", "checkbox", _as_bool(values.get("sleep_enabled"))),
        _field("activation_delay_seconds", "Initial Activity Delay", "number", _as_float(values.get("activation_delay_seconds"), 20), minimum=5, maximum=600, step=5, suffix="sec"),
        _field("look_around_min_seconds", "Room Look Minimum Delay", "number", _as_float(values.get("look_around_min_seconds"), 60), minimum=15, maximum=3600, step=5, suffix="sec"),
        _field("look_around_max_seconds", "Room Look Maximum Delay", "number", _as_float(values.get("look_around_max_seconds"), 150), minimum=15, maximum=7200, step=5, suffix="sec"),
        _field("ambient_comment_min_seconds", "Comment Minimum Delay", "number", _as_float(values.get("ambient_comment_min_seconds"), 1200), minimum=300, maximum=86400, step=60, suffix="sec"),
        _field("ambient_comment_max_seconds", "Comment Maximum Delay", "number", _as_float(values.get("ambient_comment_max_seconds"), 2700), minimum=300, maximum=172800, step=60, suffix="sec"),
        _field("sleep_after_seconds", "Sleep After No Activity", "number", _as_float(values.get("sleep_after_seconds"), 900), minimum=60, maximum=86400, step=60, suffix="sec"),
        _field("sleep_min_seconds", "Minimum Sleep Time", "number", _as_float(values.get("sleep_min_seconds"), 90), minimum=30, maximum=3600, step=30, suffix="sec"),
        _field("sleep_max_seconds", "Maximum Sleep Time", "number", _as_float(values.get("sleep_max_seconds"), 300), minimum=30, maximum=7200, step=30, suffix="sec"),
        _field("wake_on_sound", "Wake from Nearby Sound", "checkbox", _as_bool(values.get("wake_on_sound"), True)),
        _field("sound_wake_threshold", "Sound Wake Threshold", "number", _as_float(values.get("sound_wake_threshold"), 0.035), minimum=0.005, maximum=0.5, step=0.005),
        _field("quiet_hours_enabled", "Ambient Comment Quiet Hours", "checkbox", _as_bool(values.get("quiet_hours_enabled"))),
        _field("quiet_hours_start", "Quiet Hours Start", "time", _text(values.get("quiet_hours_start")) or "22:00"),
        _field("quiet_hours_end", "Quiet Hours End", "time", _text(values.get("quiet_hours_end")) or "07:00"),
    ]
    return card


def _status_card(row: Dict[str, Any], response: Dict[str, Any]) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    app = response.get("app") if isinstance(response.get("app"), dict) else {}
    status = response.get("status") if isinstance(response.get("status"), dict) else {}
    app_kind = _text(app.get("kind")) or "satellite"
    app_version = _text(app.get("version")) or _text(row.get("firmware_version")) or "unknown"
    return {
        "id": _card_id(selector, "all"),
        "group": _section_group(selector, "overview"),
        "title": _device_name(row),
        "subtitle": f"Reachy Tater {app_kind.title()} · app {app_version}",
        "detail": _text(status.get("connection_error")) or "Connected through Tater's authenticated native satellite link.",
        "hero_badges": [
            {"label": "CONNECTED", "tone": "good"},
            {"label": app_kind.upper(), "tone": "muted"},
            {"label": f"SETTINGS V{int(response.get('protocol_version') or 1)}", "tone": "muted"},
        ],
        "summary_rows": [
            {"label": "Room", "value": _text(row.get("room")) or "Unassigned"},
            {"label": "Selector", "value": selector},
            {"label": "Bridge", "value": _text(status.get("connection_state")) or "connected"},
        ],
        "actions": [
            {
                "action": "reachy_restart_bridge",
                "label": "Restart Reachy Bridge",
                "confirm": f"Restart the satellite bridge on {_device_name(row)}?",
            },
            {
                "action": "reachy_reset_settings",
                "label": "Reset All Reachy Settings",
                "confirm": f"Restore all managed Reachy settings on {_device_name(row)} to defaults?",
                "tone": "danger",
            },
        ],
    }


def _unsupported_card(row: Dict[str, Any]) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    return {
        "id": _card_id(selector, "unsupported"),
        "group": _section_group(selector, "overview"),
        "title": _device_name(row),
        "subtitle": "Reachy app update required",
        "detail": (
            "This Reachy is connected, but its app does not advertise remote settings. "
            "Update Reachy Tater Satellite or Reachy Tater Embedded, then restart the app."
        ),
        "hero_badges": [
            {"label": "CONNECTED", "tone": "good"},
            {"label": "UPDATE NEEDED", "tone": "warn"},
        ],
    }


def _error_card(row: Dict[str, Any], error: str) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    return {
        "id": _card_id(selector, "error"),
        "group": _section_group(selector, "overview"),
        "title": _device_name(row),
        "subtitle": "Reachy settings are temporarily unavailable",
        "detail": error or "The Reachy app did not answer the settings request.",
        "hero_badges": [{"label": "RETRY", "tone": "warn"}],
    }


def _read_settings(row: Dict[str, Any]) -> Dict[str, Any]:
    response = _native_request(_text(row.get("selector")), "reachy.settings.get", {}, timeout=5.0)
    if not _as_bool(response.get("ok"), False):
        raise RuntimeError(_text(response.get("error")) or "Reachy rejected the settings request.")
    version = int(_as_float(response.get("protocol_version"), 0))
    if version < _SETTINGS_PROTOCOL_VERSION:
        raise RuntimeError("The connected Reachy settings protocol is too old. Update the Reachy app.")
    return response


def _device_manager_tab(
    row: Dict[str, Any],
    index: int,
    *,
    sections: List[str],
) -> Dict[str, Any]:
    selector = _text(row.get("selector"))
    labels = {
        "overview": "Overview",
        "reachy": "Vision & Head",
        "watch": "Tracking",
        "motion": "Motion & Music",
        "idle_life": "Idle Life",
    }
    return {
        "key": f"reachy-{index + 1}",
        "label": _device_name(row),
        "source": "grouped_items",
        "groups": [
            {
                "key": section,
                "label": labels[section],
                "item_group": _section_group(selector, section),
                "selector": False,
                "empty_message": f"No {labels[section].lower()} settings are available.",
            }
            for section in sections
        ],
    }


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    del redis_client
    reachys = _connected_reachys()
    compatible = [row for row in reachys if row.get("settings_supported")]
    forms: List[Dict[str, Any]] = []
    tabs: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []

    for index, row in enumerate(reachys):
        if not row.get("settings_supported"):
            forms.append(_unsupported_card(row))
            tabs.append(
                _device_manager_tab(row, index, sections=["overview"])
            )
            continue
        try:
            response = _read_settings(row)
            settings = response.get("settings") if isinstance(response.get("settings"), dict) else {}
            forms.extend(
                [
                    _status_card(row, response),
                    _vision_card(row, settings.get("reachy") if isinstance(settings.get("reachy"), dict) else {}),
                    _tracking_card(row, settings.get("watch") if isinstance(settings.get("watch"), dict) else {}),
                    _motion_card(row, settings.get("motion") if isinstance(settings.get("motion"), dict) else {}),
                    _idle_life_card(row, settings.get("idle_life") if isinstance(settings.get("idle_life"), dict) else {}),
                ]
            )
            tabs.append(
                _device_manager_tab(
                    row,
                    index,
                    sections=["overview", "reachy", "watch", "motion", "idle_life"],
                )
            )
        except Exception as exc:
            forms.append(_error_card(row, _text(exc)))
            tabs.append(
                _device_manager_tab(row, index, sections=["overview"])
            )

    for row in reachys:
        items.append(
            {
                "title": _device_name(row),
                "subtitle": _text(row.get("room")) or "Unassigned room",
                "detail": (
                    "Remote settings ready"
                    if row.get("settings_supported")
                    else "Reachy app update required"
                ),
            }
        )

    ui: Dict[str, Any] = {
        "kind": "settings_manager",
        "title": "Reachy Mini Control",
        "empty_message": (
            "No connected Reachy Mini was found. Start Reachy Tater Satellite or "
            "Reachy Tater Embedded and confirm it is connected to this Tater."
        ),
        "item_forms": forms,
    }
    if tabs:
        ui["manager_tabs"] = tabs
        ui["default_tab"] = tabs[0]["key"]

    return {
        "summary": "Manage settings stored on connected Reachy Mini apps.",
        "stats": [
            {"label": "Reachys", "value": len(reachys)},
            {"label": "Ready", "value": len(compatible)},
            {"label": "Update Needed", "value": len(reachys) - len(compatible)},
        ],
        "items": items,
        "empty_message": ui["empty_message"],
        "ui": ui,
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values")
    return values if isinstance(values, dict) else {}


def _checked_result(result: Dict[str, Any]) -> Dict[str, Any]:
    if not _as_bool(result.get("ok"), False):
        raise ValueError(_text(result.get("error")) or "Reachy rejected the request.")
    return result


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    del redis_client
    action_name = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    selector, section = _parse_card_id(body.get("id"))

    if action_name == "reachy_save_settings":
        if section not in _SUPPORTED_SECTIONS:
            raise ValueError("This Reachy settings card cannot be saved.")
        values = _payload_values(body)
        if not values:
            raise ValueError("No Reachy settings were provided.")
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.update",
                {"sections": {section: values}},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Reachy settings saved.",
        }

    if action_name == "reachy_reset_settings":
        reset_sections: Any = "all" if section == "all" else section
        if reset_sections != "all" and reset_sections not in _SUPPORTED_SECTIONS:
            raise ValueError("This Reachy settings card cannot be reset.")
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.reset",
                {"sections": reset_sections},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Reachy defaults restored.",
        }

    if action_name == "reachy_test_ambient_comment":
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.action",
                {"action": "test_ambient_comment"},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Ambient comment queued.",
        }

    if action_name == "reachy_restart_bridge":
        result = _checked_result(
            _native_request(
                selector,
                "reachy.settings.action",
                {"action": "restart_bridge"},
            )
        )
        return {
            **result,
            "message": _text(result.get("message")) or "Reachy bridge restarting.",
        }

    raise KeyError(f"Unknown Reachy Core action: {action_name or 'missing'}")


def run(stop_event=None) -> None:
    """Keep the installed core lifecycle active; control remains request-driven."""
    logger.info("[Reachy Core] Started; waiting for compatible Reachy satellites.")
    try:
        while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            wait = getattr(stop_event, "wait", None) if stop_event is not None else None
            if callable(wait):
                wait(5.0)
            else:
                time.sleep(5.0)
    finally:
        logger.info("[Reachy Core] Stopped.")
