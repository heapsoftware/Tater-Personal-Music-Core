# Sendspin playback start waits ~15s for eight clock exchanges that converge in 0.3s

Observed on the live host (v1.9.3) against a firmware v4.0.0 Tater Echo biscuit
(Go firmware, noise-capable). Measured 2026-10-31 by driving the host's own
`sendspin_playback._SendspinPeer` exactly as `play_media_url_targets` does, from
inside the container:

- websocket connect + noise handshake + `server/hello` + `client/hello`: **0.1s**
- `server/activate` sent; device replies `client/state state=synchronized` at **0.27s**
- `time_event` (the host's `time_responses >= 8` gate,
  `tater_voice/sendspin_playback.py` `client/time` handler) fires at **15.47s**

So the whole start path waits for `time_event`, and `time_event` needs eight
`client/time` exchanges. The device only needs two to be fully synchronized,
but once it is, its clock error is already <1ms and it slows its exchange
cadence to 3s. Timestamped trace of every exchange from activation:

```
[ 0.063] client/time #1
[ 0.263] client/time #2          (200ms: not yet converged)
[ 0.265] client/state synchronized
[ 0.464] client/time #3
[ 3.465] client/time #4          (device converged, error <1ms -> 3s ladder)
[ 6.466] client/time #5
[ 9.466] client/time #6
[12.467] client/time #7
[15.468] client/time #8          -> host starts the stream
```

Every music play, skip, and next-track event opens a fresh Sendspin session,
so users pay ~13–18s of silent dead time on each one (the Hydra ledger shows
`tool_ms` of 15.9–18.0s for `personal_music_play` and `personal_music_control`
"next" turns; non-music tools on the same host run in 0.3s).

## Why the fast cadence is lost

The firmware (`device/internal/sendspin/session.go`, v4.0.0) special-cases the
bootstrap cadence:

```go
// Tater waits for eight clock exchanges before it schedules a shared
// timeline. Keep that short bootstrap cadence compatible with the ESP
// clients; after it, use the reference client's adaptive interval.
if s.plaintext && requests < 8 {
    interval = 200 * time.Millisecond
}
```

That 200ms fast lane is gated on `s.plaintext` — the ESP-era session mode.
Modern firmware sessions with the Tater host complete the Noise handshake
(v4.0.0 devices advertise and use it), so `s.plaintext` is false, the fast
lane never applies, and the adaptive ladder rules immediately: after two
exchanges the filter is synchronized with sub-millisecond error, and the
ladder in `timeFilter.syncIntervalMs()` returns 3000ms. Eight exchanges at
the ladder's 3s spacing take ~15s.

## Suggested fixes (either side alone is sufficient, both together are best)

1. **Firmware**: drop the plaintext gate for the bootstrap window —
   `if requests < 8 { interval = 200 * time.Millisecond }` regardless of
   session mode. Eight exchanges then cost ~1.6s on Noise sessions, matching
   what the ESP clients on plaintext already get.
2. **Host**: in `sendspin_playback._SendspinPeer`, don't require eight
   exchanges to set `time_event`. The device's own `client/state
   synchronized` plus two exchanges is what the shared timeline actually
   consumes (`_stream_live_pcm` schedules the start from the state and the
   time filter of the client); starting at state-synchronized would cut the
   start to ~0.5s with the v4.0.0 device. If eight exchanges are a hard
   accuracy requirement for mixed fleets, treat two exchanges with the
   device's own `synchronized` state as sufficient for scheduling and keep
   collecting the rest in the background.

## Repro

From inside the Tater container (host v1.9.3):

```python
import asyncio, aiohttp, sys, time
sys.path.insert(0, "/app")
import tater_voice.sendspin_playback as sp

async def main():
    async with aiohttp.ClientSession() as s:
        peer = sp._SendspinPeer(s, {"selector": "x", "host": "<sat-ip>", "port": 8928})
        t0 = time.time()
        await peer.open(server_id="tater", server_name="Tater")
        await peer.activate_supported_presentation_roles()
        await asyncio.wait_for(
            asyncio.gather(peer.state_event.wait(), peer.time_event.wait()),
            timeout=40.0,
        )
        print("state+8 exchanges took", round(time.time() - t0, 2), "s")

asyncio.run(main())
# state_event alone: ~0.3s; time_event: ~15.5s
```