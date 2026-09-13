# Feature request: Smart fading (BPM-aware crossfade)

**Requested in:** Personal Music Core (tracks the same capability Music Assistant
offers via its crossfade/fade options)
**Scope:** Tater host playback engine — not implementable in a core alone.

## The ask

Smart fading makes the handover between two songs sound natural by fading the
outgoing track while fading the incoming one, with the fade shape and duration
derived from each song's **BPM** (and optionally its energy/last-note decay) so
transitions land on the beat instead of mid-phrase.

## Why it needs a host change

Personal Music Core hands a **stream URL** to the playback target (Tater Native
satellites, Sonos groups, AirPlay players) through the host's
`media_playback.play_media_url_targets`. The core never touches the audio
samples, so a core cannot:

- start the next track's stream before the current one ends on the same target,
- mix two streams into one output, or
- apply volume automation to the outgoing stream as it ends.

All three live in the host's playback engine. The core's own stream server could
transcode, but mixing two independent sources with per-target sync (Sonos
groups, mixed sync groups, stereo pairs) is engine territory.

## What the host would need

1. **BPM metadata surface.** Track dicts gain an optional `bpm` field; a
   host-side or core-side analysis pass (e.g. libebur128/aubio-style onset
   detection, or tags like `TXXX:BPM`/ID3 `TBPM`) fills it. Cores that know the
   BPM (from tags or provider metadata) pass it through.
2. **Overlap scheduling in `media_playback`.** A `play_media_url_targets(...)`
   option such as `crossfade_seconds` / `crossfade_profile` that:
   - starts the next track's stream `fade_seconds` before the current one ends,
   - ramps the outgoing stream down and the incoming one up (equal-power curves),
   - aligns the switch point to beat distance when both BPMs are known
     (e.g. start the incoming track on the nearest bar line).
3. **Per-target support negotiation.** Targets that cannot mix (some Sonos
   routes, AirPlay bridges) fall back to a gapless stop/start with a short
   fade-out only, so the feature degrades gracefully.
4. **A way for cores to declare "I will send the next stream early".** The
   session/queue handoff currently assumes one stream per target session; the
   engine needs a documented overlap window (e.g. `media_playback` accepts a
   queued "next" URL plus transition policy per target).

## Core-side plan once the host supports it

Personal Music Core would add per-Person **Smart Fading** settings (fade mode:
off / simple / smart-BPM; max fade seconds) on the Person link card with a
global default, exactly like the other per-Person settings, and pass
`crossfade_seconds` (computed from the two tracks' BPMs) to the host call. The
selection logic is core-side and ready to build; only the final
`play_media_url_targets` call depends on this host feature.