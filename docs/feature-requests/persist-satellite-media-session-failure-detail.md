# Feature request: persist the reason when a satellite media session fails

## Problem

When a native satellite fails to fetch or decode a media stream, its
`media.session.finished` payload is collapsed in
`tater_voice/native_satellite.py` (the `media.session.finished` handler) to:

```python
row["media_session"] = {
    **previous,
    "active": False,
    "session_id": _text(payload.get("session_id") or previous.get("session_id")),
    "ok": _as_bool(payload.get("ok"), False),
    "finished_ts": _now(),
}
```

Any error/reason field the satellite reports in that same payload (or in the
preceding `media.session.playhead`/log lines) is discarded. Cores and the WebUI
can therefore see *that* playback failed (via
`personal_music_core._reconcile_native_playback` scanning
`status_snapshot_sync()`), but never *why* — the queue just says
"Playback failed on Kitchen" and the Logs tab has nothing.

## Request

Persist the failure detail from the `media.session.finished` payload on the
`media_session` row, e.g.:

```python
row["media_session"] = {
    **previous,
    ...,
    "ok": _as_bool(payload.get("ok"), False),
    "finished_ts": _now(),
    "error": _text(payload.get("error") or payload.get("message")),
}
```

Cores that already scan `ok is False` (Personal Music Core does, and appends
`state.get("error")` when present) will pick the detail up with no further
change, and the Logs tab + queue warnings immediately become actionable.

## Real-world case this cost debugging time on (2026-10-03)

Emby's transcoding side was failing (`Unable to find a suitable output format
for '/var/lib/emby/transcoding-temp/…'` → 500 on `Audio/<id>/stream` sync
requests). The satellite fetched the (dead) sync URL, aborted before starting,
and reported `media.session.finished ok=False` — Tater stored only the ok
flag, so the music core marked the queue ERROR with no explanation anywhere in
the logs, despite knowing exactly which sessions failed on which targets.