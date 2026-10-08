# Feature request: record active Sendspin stream failures so cores can detect mid-stream playback failures

## Summary

`Tater v1.3.0` moved native satellite playback to Sendspin: one host-side
timeline (`tater_voice/sendspin_playback.py`) decodes the media with ffmpeg and
streams timestamped PCM to every member satellite. When such a stream fails,
the host logs one line and drops the incident:

```
logger.warning(
    "[sendspin] live stream ended with an error stream=%s error=%s",
    clean_id,
    error,
)
```

(`start_live_pcm_stream`'s `_finished` done-callback, currently reached with a
non-empty `error` only from its `logger.warning` branch.) The stream id is then
popped from `_active_live_streams`, its ownership entries are removed from
`_live_target_owners`, and nothing records that the stream ever failed — nor
that its members were involved. A music Core that started the stream has no
programmatic way to learn that playback died mid-track; the queue keeps
reporting "playing" per the clock until the track's stored duration elapses.

`start_live_pcm_stream` already accepts an `on_finished(stream_id, error)`
callback, but the host's own music route
(`media_playback._voice_core_play_sendspin_media_sync`, called for
`source_owner="music_core"`) does not pass one, and the failure would still
evaporate with the stream even if it did.

## Why a core needs this

Before Sendspin, each satellite fetched and decoded the track itself and
reported per-satellite outcome through the `media_session` row in
`native_satellite.status_snapshot_sync()`. Personal Music Core's
skip-on-failure logic (its `_reconcile_native_playback`) consumed exactly that:
when every dispatched session finished with `ok=False`, it logged a named-file
WARNING ("the file may be damaged or the target could not decode it"),
auto-skipped to the next queued track, and after 3 consecutive failures parked
the queue as an error instead of machine-gunning a dead server.

On a Sendspin host there is no per-satellite playback state at all, so that
reconcile comes back empty (this matches upstream Music Core 3.6.0, which
simply skips sendspin-transport session rows during reconcile). The loss is
narrow but real:

- Start-time failures are fine — the ffmpeg reader raises during the started
  wait and `play_media_url_targets` returns `ok=False`, so the Core sees the
  error with real detail.
- **Mid-stream failures** (upstream server drops the connection, network blip,
  seek range that ffmpeg only chokes on later) are silent. The music stops,
  the queue stays "playing" for the remainder of the stored duration, the
  next track then starts on schedule, and nothing is logged with the file or
  the failure. The damaged-file skip and the 3-strike circuit breaker cannot
  trigger.

A persisted, queryable stream outcome is all the reconcile needs to work again
over Sendspin timeline: the session rows a Core already records in its queue
player (`voice_core_sessions` with `session_id == stream_id`, `transport ==
"sendspin"`, and `start_unix_ms`) identify the stream; a lookup that answers
"did this stream end early with an error?" lets the Core restore the
named-file skip and the circuit breaker.

## Requested host change

In `tater_voice/sendspin_playback.py`, record the terminal outcome of every
live stream in a bounded registry keyed so selectors can query it, e.g.:

```python
_STREAM_OUTCOMES: Dict[str, Dict[str, Any]] = {}      # stream_id -> outcome
_STREAM_OUTCOME_OWNERS: Dict[str, list[str]] = {}     # member selector -> recent stream_ids

@dataclass
class _StreamOutcome:
    stream_id: str
    error: str                 # "" = natural end (played to EOF)
    started_unix_ms: int
    ended_unix_ms: int
    duration_s: float          # seconds actually sent
    expected_duration_s: float # as requested by the caller (0 = unknown)
    members: list[str]
    group_id: str
```

- Populate it in the `_finished` done-callback (and for manual
  `stop_live_stream` calls, mark `error: "stopped"` or a distinct reason so
  intended stops are not reported as failures).
- Cap the registry (e.g. 128 outcomes with an 10-minute minimum age for
  eviction is plenty — failures need to outlive a track's duration plus a
  reconcile tick, not forever).
- Expose queries, either:

```python
def stream_outcome(stream_id: str) -> Dict[str, Any] | None: ...
async def failed_stream_ids_for_selectors(
    selectors: Iterable[str], *, since_unix_ms: int = 0
) -> Dict[str, Any]:
    """{ok: True, failures: {stream_id: outcome_row}} for streams that ended
    with an error while owning one of the selectors since since_unix_ms."""
```

A synchronous, snapshot-shaped query function is enough — Cores call it via
`native_satellite.run_on_runtime_loop` like every other host module entry
point today, no HTTP surface is required.

### Nice to have (separate, not blocking)

- Per-member failure attribution inside one stream: today a single failing
  peer (send-side error, goodbye/`client/goodbye` with a reason) fails the
  whole `_broadcast_audio` gather and tears the stream down for every member.
  Keeping the per-peer error on the outcome row would let a Core distinguish
  "one satellite dropped" from "the source died" (only the former warrants a
  warn-and-continue; only the latter should drive a track skip).

## Notes

- Cores already stop whole streams by recorded id and by current ownership
  (`stop_live_streams_for_targets`) and manage pause/next/prev by
  stop-plus-replay from the saved position; the outcome registry changes no
  existing behavior, it only preserves what currently vanishes.
- Intended stops need a distinguishable reason (see above) or Cores will
  misread their own stop/pause/next actions as failures.
- Related filed request: `per-member-sendspin-stream-stop.md` (detaching one
  member of a live stream). A detach that ends the *stream* would also want an
  outcome row saying `detached_members: [...]` so Cores do not count the
  remaining rooms' silence as a failure.
- The `on_finished` callback signature suffices for the registry's error
  field; a caller-supplied hook is not an adequate substitute for persistence
  — the host's own music route does not pass one, and hooks die with the
  stream the way current state does.