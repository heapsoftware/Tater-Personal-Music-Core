# Feature request: treat Personal Music Core stream URLs as tater-encoded shared group sources

**Upstream repo:** Tater (host) · `media_playback.py`, `play_media_url_targets`
**Found while auditing** Tater v1.2.6 for Personal Music Core v3.13.0
(2026-10-04)

## What v1.2.6 shipped

The new shared media relay (`tater_voice/shared_media_relay.py`) feeds native
stereo pairs and mixed speaker groups from **one shared upstream stream** so
every member receives the same encoded bytes and recovery timeline.
`play_media_url_targets` registers the relay when either:

```python
has_native_stereo_pair                      # stereo pair among the targets
synchronized_group_targets > 1
    and tater_encoded_group_source          # OR a "Tater-encoded" source URL
```

where `tater_encoded_group_source` matches only two URL markers:

- `/api/external-audio/v1/streams/` (AirPlay Input live audio)
- `/api/cores/music_core/webhook/native-mp3` (the built-in music_core)

and applies an extra completion-wait carve-out (spool up to 20 s for a
complete file) **only** to `/api/cores/music_core/webhook/native-mp3`.

## The gap

The Personal Music Core serves its library through its own stream server
(configured `stream_host:stream_bind_port`, path prefix `/stream/<token>/…`,
including the `audio_sync` WAV transcodes at `*.sync.wav`). Those URLs are not
in the marker list, so:

- **Stereo pairs are fine** — the pair check triggers the relay regardless of
  the source URL, so two-member pairs already share one stream. Thank you —
  that also collapses two upstream fetches (one per member) against the
  personal core's proxy into one.
- **Mixed native groups of 3+ speakers are not** — with the Personal Music
  Core as the source, each member still opens its own upstream
  (`tater_encoded_group_source` stays false), so the group misses the
  shared-byte guarantee the release notes describe, and the personal core's
  proxy (and, behind it, the media server) serves N independent streams —
  including N independent sync-transcode sessions whenever the source is a
  server that has to transcode.

## Requested host change

1. Extend the `tater_encoded_group_source` recognition to include the Personal
   Music Core's stream origin URLs (e.g. match the host:port pair the core
   advertises in its settings as `stream_host`/`stream_bind_port`, or give
   cores a small registration hook for "my URLs are Tater-encoded") so mixed
   native groups share one upstream stream like stereo pairs already do.
2. Include those URLs in the finite-track completion-wait carve-out: personal
   music tracks are finite files that usually spool faster than real time, so
   starting the group from a complete, seekable file is possible just as it is
   for the built-in music_core's native-mp3 webhook.

No Personal Music Core change is expected for this; the core already passes
`duration_seconds` and `media_content_type="music"` to
`play_media_url_targets`, which is everything the relay needs. Note for the
implementation: the core's stream URLs carry a core-specific access token in
the path — the relay's own token-gated `/api/media/shared/…` URL replaces the
upstream for the group members, so nothing sensitive leaks through this path
(the relay re-fetches server-side with the credential it was given).