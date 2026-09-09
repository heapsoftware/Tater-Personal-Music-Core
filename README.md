# Custom Music Core for Tater

A standalone, unofficial [Tater](https://github.com/TaterTotterson/Tater) core that
gives every Person their own music: link each Person to their own **Emby** user or
their own folder on a **network share** (SMB/CIFS or NFS), then browse and play
their library with voice control, per-person recommendations, and multi-room
playback across clock-synchronized satellites, native Sonos groups, stereo pairs,
and media players.

It is built from Tater_Shop's `music_core.py` (a pure-rename commit plus separate
feature commits), so it can run **side by side** with the stock Music Core —
Redis keys, settings, provider ids, tool ids, and the WebUI tab are all namespaced
away from the original (verified by tests).

## Install

1. Push this repo to GitHub (the manifest CI regenerates `core_manifest.json` on
   every core change).
2. In Tater's **Core Shop**, add this repo's raw manifest URL as an additional
   shop repo (Core Shop → repos / `POST /api/shop/cores/repos`), e.g.
   `https://raw.githubusercontent.com/heapsoftware/Tater_Custom_Music_Core/main/core_manifest.json`.
3. Install **Custom Music Core** from the shop and enable it.

Alternatively, copy `cores/custom_music_core.py` into your Tater `cores/` directory
yourself.

## Music sources

Open Tater → **Custom Music** tab → **Sources**.

### Emby

- **Username & password** (recommended): the core signs in per user, honors each
  Emby user's library access, and streams through this core's own token-gated,
  Range-capable stream server so the Emby token never appears in any URL a
  playback target fetches.
- **Server API key**: streams directly from Emby; set the Emby User ID when the
  server has more than one user.

### Network share (SMB/CIFS or NFS)

Tater does **not** mount shares itself. Mount the share on the Tater **host** (or
bind-mount it into the container), then point the core at the mounted folder:

```yaml
# docker-compose.yml (Tater service)
    volumes:
      - /mnt/music:/mnt/music:ro
```

The core scans the folder with a stdlib tag reader (ID3v2 MP3, FLAC/Vorbis, Ogg
Vorbis/Opus, MP4/M4A, WAV — no extra packages needed), reads embedded or
`cover.jpg`/`folder.jpg` artwork, and streams files with full Range support.

## Per-person links

**Custom Music** tab → **People** → **Link a Person**. Each Person can get:

- their own Emby user/library on a shared server, or
- their own subfolder on a mounted share (e.g. `/mnt/music/<person>`).

A linked Person gets their own catalog, listening history, AI-named
recommendations, and prompt-ready music profile, scoped under
`custom_music_core:*:<person_id>` keys. Everyone else follows the global source.
Voice requests resolve the speaking Person automatically.

## Settings worth knowing

| Setting | Default | Notes |
| --- | --- | --- |
| Stream Server Port | `8621` | Local HTTP port the core serves token-authenticated Emby streams and share files from. Must be reachable from your playback targets on the LAN. |
| Stream Host | auto | Override only if the auto-detected LAN address is wrong (e.g. multiple NICs). |
| Catalog Sync Interval | `900` s | Also drives per-person catalog refreshes. |

## Limitations (v1)

- Little Spud client music is not switched over — the Tater host currently links
  client music to the stock `music_core` only.
- Network-share playback serves original files; there is no on-the-fly transcode
  for mixed sync groups (Emby username sign-in does transcode to WAV when needed).
- One global player queue (matching the stock core); per-person queues are a
  possible future step.

## Development

```sh
python3 -m unittest discover -s tests
```

The test suite runs offline (fake Redis, fake Emby server, hand-crafted audio tag
fixtures) and includes coexistence tests that load the upstream `music_core.py`
snapshot alongside this core.

### Keeping in sync with upstream `music_core.py`

The repo history seeds from Tater_Shop with a pure-rename commit, and `upstream`
points at `https://github.com/TaterTotterson/Tater_Shop.git`:

```sh
git fetch upstream
git diff upstream/main:cores/music_core.py cores/custom_music_core.py
```

Port relevant hunks by hand (do not merge — upstream contains shop files this
repo does not carry). The goal is absorbing upstream bug fixes, not staying
byte-identical.

## License / provenance

Derived from TaterTotterson's Tater_Shop `cores/music_core.py` (see
`tests/fixtures/upstream_music_core.py` for the pre-divergence snapshot this
build started from).