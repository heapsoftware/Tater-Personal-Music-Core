# Personal Music Core for Tater

A standalone, unofficial [Tater](https://github.com/TaterTotterson/Tater) core that
gives every Person their own music: link each Person to their own **Emby**,
**Jellyfin**, **Subsonic** (Navidrome/Airsonic/Gonic), or **Plex** account, or
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
   `https://raw.githubusercontent.com/heapsoftware/Tater-Personal-Music-Core/main/core_manifest.json`.
3. Install **Personal Music Core** from the shop and enable it.

Alternatively, copy `cores/personal_music_core.py` into your Tater `cores/` directory
yourself.

## Music sources

Open Tater → **Personal Music** tab → **Sources**.

**Sources is the single, global music source for the household** — one Emby,
Jellyfin, Subsonic, or Plex login (or an Emby/Jellyfin API key), or one mounted
share folder. This is the library that
People *without* their own link hear, and what the dashboard player bar and
client music use. It is **not** how each Person gets their own library: giving
a Person their own provider account or share folder is done per-Person in the
**People** section (see [Per-person links](#per-person-links)), which
overrides the global source for that Person only.

### Emby

- **Username & password** (recommended): the core signs in per user, honors each
  Emby user's library access, and streams through this core's own token-gated,
  Range-capable stream server so the Emby token never appears in any URL a
  playback target fetches.
- **Server API key**: streams directly from Emby; set the Emby User ID when the
  server has more than one user.
- **Library Name** (optional): the Emby *library* to sync — the name of a
  home-screen tile, which the Emby admin defines under Dashboard → Libraries
  (it is never a folder path; folder names on disk do not create libraries).
  Leave it blank to auto-pick the first library whose content type is Music
  that the account can see; set it when the account can see several libraries
  or when the library is mixed content (Emby does not tag those as music).
- **Library Folder** (optional): scope the sync to one subfolder of the
  library — by name (`Music`) or by full server path
  (`/mnt/media/<user>/Music`). Use this when one
  mixed-content library holds a Person's music, TV, and movies and only the
  music should be indexed. Blank means the whole library is synced.
- **Test Emby Connection** (on Person link cards) signs in and verifies the
  Library Name and Library Folder resolve — a mismatch is reported there with
  the failing name, before anything is saved or synced.

### Jellyfin

Works like the Emby source against a Jellyfin server (10.8–10.10 tested header
styles):

- **Username & password** (recommended): the core signs in per user and honors
  each Jellyfin user's library access, with streams proxied so the token never
  appears in any URL a playback target fetches.
- **Server API key**: streams directly from Jellyfin; set the Jellyfin User ID
  when the server has more than one user.
- **Library Name / Library Folder** (optional): the same optional library and
  subfolder scoping as Emby (see above).

### Subsonic (Navidrome, Airsonic, Gonic)

- **Server URL, Username, Password**: the core speaks the Subsonic REST API
  with its salted-token auth (the password is never sent as-is; each request
  carries a fresh `md5(password + salt)` token). Navidrome, Airsonic, and
  Gonic speak the same API — just point the core at whichever one you run.
- The whole music library is synced (albums are walked page by page, capped at
  the catalog limit); the server decides what the account can see.
- **Test Subsonic Connection** pings the server with the credentials before
  anything is saved.

### Plex

One music library on a Plex Media Server, with three sign-in styles (a
**Sign-In Style** select on both the Sources card and each Person's link card;
all of them converge on a server URL plus a token):

- **Home user** (default): the owner signs in to plex.tv once, and the source
  plays as one **named Plex Home member** (with that member's PIN when the
  member is PIN-protected). The Server URL can be left blank — plex.tv
  discovery fills in a reachable server (a manual entry always wins).
- **Own account**: signs in with that account's **own** plex.tv username and
  password — the route for shared or family accounts the owner cannot switch
  into (access is naturally scoped to the libraries shared with that account).
- **Manual token**: paste an `X-Plex-Token` and the server URL directly; the
  escape hatch when the plex.tv sign-in flows misbehave.
- The resolved token stays **server-side**: streams and artwork are proxied
  through this core's stream server, so the token never appears in any URL a
  playback target fetches. Only `artist`-type music sections are synced, and a
  shared account sees only the sections shared with it.
- **Test Plex Connection** resolves the credentials (including the plex.tv
  sign-in and member switch) and checks the server answers before anything is
  saved.

### Supported audio formats

Satellite speakers decode **WAV, MP3, and FLAC** natively (verified against the
satellite firmware). **AAC, OGG, Opus, and M4A have no decoder** and will not
play, so libraries are assumed to be WAV/MP3/FLAC. The core itself never
transcodes and providers serve your files as they are: Subsonic, Plex, and
network shares always serve the original bytes. Emby/Jellyfin are the one
partial exception: Tater's clock-synchronized playback asks those servers for
a normalized WAV source (`AudioCodec=wav&AudioSampleRate=44100&AudioChannels=2`)
so grouped targets stay aligned — but the **server decides** whether to honor
that. A server with transcoding disabled (or that declines the request) just
serves the original MP3/FLAC, and the satellites decode it natively; either
way the file's format never decides whether it plays. Subsonic additionally
asks for `format=wav` on non-decodable containers only (Navidrome and friends
transcode those server-side).

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

When a track has no **AlbumArtist** ID3 tag, the core falls back to a
Kodi-style `album.nfo` in the album's folder (the `<albumartist>` element, then
`<artist>`) before using the track artist — so compilations and "Various
Artists" albums group correctly.

### Playlists

Three kinds of playlist can play by name (voice: *"play my Christmas Music
playlist"*) and feed the **Tracks from a playlist** Endless Playback mode:

- **AI-named mixes** — the dynamic playlists on the Recommendations tab.
- **User-created playlists** — playlists you built in Emby, Jellyfin, Subsonic,
  or Plex, and `.m3u`/`.m3u8`
  files anywhere on the share (entries resolve relative to the playlist file's
  folder or the share root; remote URLs are skipped). Names are matched
  case-insensitively; a person's own source is searched.
- **Folder Playlists** — a core setting that turns library folders into
  always-current playlists: `Christmas Music=Christmas, Road Trip=Tunes/Road`
  builds a "Christmas Music" playlist from every track under the Christmas
  folder (subfolders included). The playlist is rebuilt from the library on
  every sync and every play, so songs added to the folder join automatically.
  Each linked Person can replace the global list with their own on their link
  card in the People section (blank inherits the global list); folders match
  against that Person's own library.

With no playlist picked, the newest AI mix is used (or the first user-created
playlist when there are no mixes yet).

The **Playlist Order** setting decides how these playlists play: shuffled
(each play, the default) or in a fixed order — by track number, title, artist,
or album, ascending or descending — so the playlist sounds the same every
time.

## Per-person links

**Personal Music** tab → **People** → **Add Person Link** (Edit opens the
form in a modal; **Edit** on a linked Person's card reopens it). Each Person can get:

- their own Emby or Jellyfin user/library on a shared server,
- their own Subsonic account (Navidrome/Airsonic/Gonic included),
- their own Plex account (three sign-in styles — see [Plex](#plex)),
- their own subfolder on a mounted share (e.g. `/mnt/music/<person>`), or
- **two sources at once** — a second linked source (any mix of the above)
  merges into one catalog under the same card's **Second Music Source**
  setting. The two sources are **parallel, not master/backup**: a track plays
  from wherever it lives, so "play Justin Timberlake" plays from whichever
  linked source has the artist.

A linked Person gets their own catalog, listening history, AI-named
recommendations, and prompt-ready music profile, scoped under
`personal_music_core:*:<person_id>` keys. Everyone else follows the global source.
Voice requests resolve the speaking Person automatically.

### Endless Playback

When a Person's queue reaches its final track, the queue keeps playing. Pick how
globally in the core settings, or per Person on their link card (**Their Endless
Playback**):

- **Automatic** — fetches similar tracks from a connected streaming provider and
  falls back to an **Infinite Mix** drawn from the library.
- **Basic Auto (LLM)** — the original behaviour: Tater's AI model picks similar
  library tracks and names the radio station.
- **Infinite Mix from your library** — works entirely offline: prioritises
  least-played tracks, biases toward the genres the queue was just hearing, and
  fills the gaps with varied random picks. No streaming provider needed.
- **Similar to what you played** — relies purely on streaming providers; until
  one is connected it falls back to the library mix so the music never stops.
- **Tracks from a playlist** — continuously loops one chosen playlist: an AI-named
  mix from the Recommendations tab, a playlist you created in Emby, Jellyfin,
  Subsonic, or Plex, an `.m3u`
  file on the share, or a [Folder Playlists](#playlists) entry (pick it globally
  or per Person).

Streaming providers (Spotify, Apple Music, …) are not built in yet, but the
core carries the provider scaffolding for them: the provider-backed modes call a
provider registry (`STREAMING_PROVIDER_CLASSES`) that fans out to every
connected streaming provider, so plugging one in later is an additive change.

### Smart Shuffle

Per Person (or globally) with the **Smart Shuffle** settings. Instead of a purely
random shuffle, Smart Shuffle pushes recently played tracks to the back of the
queue to keep things fresh, and it handles multiple sources at once: queue two
albums and two playlists together (voice: *"add this album to the queue"* /
`personal_music_control` with `action:"add"`) and Smart Shuffle mixes them on the
fly — the on-screen queue stays a rolling window while a pool feeds tracks from
all queued sources in turn, instead of building one enormous queue up front.

### Sleep timers

From a Person's card (**Sleep 30m / Sleep 60m / Cancel Timer**), from the Music
Player card's **Sleep Timer (minutes)** field, or by voice (*"play my music for
an hour"*). The countdown tracks the active player; when it hits zero the timer
**force-stops the stream** — a hard override that bypasses Endless Playback,
Smart Shuffle pools, and any in-flight radio refill, so nothing keeps streaming
while you doze off.

### Group volume, mute all / unmute all

The Music Player card's **Volume** slider sets **every selected speaker to the
same absolute level** — a group at 12/10/7/15 dragged to 10 ends up at
10/10/10/10, and the level is remembered per speaker for the next track. The
card also carries **🔇 All / 🔊 All** buttons: one action mutes every member of
the current group, the other unmutes them and restores the pre-mute volume.
The same works by voice (`personal_music_control`):

- *"Set all speakers to 70 percent"* → the `volume` action sets every
  destination in the group to 70.
- *"Mute all speakers" / "Unmute all speakers"* → the `mute_all` / `unmute_all`
  actions; muting remembers the previous volume so unmuting puts the group back
  where it was. Mute is applied as volume 0, which every supported target type
  honors.

A **MUTED** badge appears on the player card while the group is muted.

### Room-to-room transfer

*"Transfer my music to the Master Bedroom"* (`personal_music_move`, or the
control tool's `move`/`set_targets` action, or the Music Player card's
**Play On / Set Player** destinations): the whole populated queue and the exact
spot in the song move with you.

**Gaining-room resume delay** (optional): **Transfer Resume Delay (sec)** in
settings makes the room you moved to wait that many seconds before the music
resumes — time to walk from the kitchen to the bedroom without missing the
start of the song. During the wait the queue shows as paused at the exact spot;
pressing play, pause, or stop in the meantime cancels the wait and acts
immediately. Each Person can set their own delay on their link card (blank =
use the global setting; 0 = resume immediately).

### Filling in a provider link on a Person's card

For **Emby (own user/library)**, fill in the Person's card in **People**:

- **Emby Server URL** is pre-filled with the global server from Sources — leave
  it as-is when the Person's account is on the same Emby server (the common
  case); change it only if their account lives on a different server.
- **Emby Username / Password**: that Person's own Emby account. Emby decides
  which libraries that account can see, so their browsing, search, and
  recommendations only draw from libraries their account is allowed to access.
  (An **API key** can be used instead, but an API key sees the server's whole
  library, so per-person accounts are the recommended route.)
- **Emby Library Name** (optional): the Emby *library* (home-screen tile) to
  sync — a name Emby's admin chose, not a folder path. Needed when the account
  can see several libraries, or when the library is mixed content (Emby only
  auto-picks libraries whose content type is Music).
- **Emby Library Folder** (optional): sync only one subfolder of the library —
  by name (`Music`) or full server path. Handy for mixed-content libraries
  (music + TV + movies in one library) where only the music should be indexed.
- **Test Emby Connection** checks the URL and credentials against Emby without
  saving anything (it also verifies the Library Name and Library Folder
  resolve); **Save Person Link** stores the link and immediately loads that
  Person's catalog (any failure shows on the card's sync line).
- The **password field stays blank on the saved card** — re-enter it only when
  changing it; a blank field keeps the saved password.

For **Jellyfin**, the card is the same as Emby's — Server URL, Username /
Password (or an API key), Library Name, and Library Folder — against the
Jellyfin server. **Test Jellyfin Connection** verifies the credentials and
library scoping the same way.

For **Subsonic**, fill in the Server URL, Username, and Password
(Navidrome/Airsonic/Gonic included); **Test Subsonic Connection** pings the
server with those credentials.

For **Plex**, pick a **Sign-In Style** (see [Plex](#plex)): **Home user**
(owner signs in, plus the Home member's name and PIN when protected), **own
account** (that Person's own plex.tv username and password), or **manual
token** (a pasted token plus server URL). The Server URL can be left blank for
plex.tv discovery. Plex link cards never show a saved token — enter one only
for manual mode; a blank field keeps the saved token.

### Per-person queues

Every Person also gets their **own playback queue** — the shared household queue
only serves requests where no Person is identified (dashboards, client music,
and the stock-like global path):

- **Independent queues and timelines.** Each Person's queue keeps its own
  current track, position, shuffle/repeat, and continuous-radio state, so two
  People can listen to different music in different rooms at the same time.
- **Follow-me handoff.** "Move my music to the kitchen" (`personal_music_move`)
  hands the stream off to the new room at the same spot in the track. Room
  transport commands ("next", "pause", "stop") act on whatever is playing in
  the speaking room first, then on that Person's own queue.
- **Room bindings.** Bind a room to a Person ("the Kitchen plays my music") via
  the `personal_music_control` tool (`bind_room` / `unbind_room`); bound rooms
  become that Person's default destination.
- **Resume in Another Room.** When someone says "resume my music" from a room
  other than the one their paused music is in, each Person picks what happens
  (global default in settings, per-Person override on the link card):
  - **Stay where it was** (default): the music resumes in the room it was
    paused in, at the exact spot.
  - **Follow me to this room**: the queue moves to the room they're speaking
    in and resumes there at the same spot — unless another queue is already
    playing (or paused) in that room, in which case the move is abandoned and
    the music stays where it was. The Transfer Resume Delay applies to the
    gaining room, same as any transfer.
  - **Ask me each time**: the room they're in is asked over TTS ("resume it
    here, or resume it there?") and their answer decides; if the room they're
    in is busy, there is nothing to ask and the music just resumes where it
    was.
- **Conflict behavior.** When the rooms someone asks for are already playing
  someone else's music — or their own music is playing elsewhere — each Person
  chooses on their link card (with a global default in settings):
  - **Ask before taking over** (default): Tater asks over TTS and waits for a
    yes/no (or "start the new music instead"); the pending request expires
    after 10 minutes.
  - **Auto-move / take over**: the requested rooms are freed automatically and
    the other queue keeps playing, paused at its position, on any rooms it has
    left.

### View Music As

By default the dashboard tabs (Playlist, Browse Library, Recommendations) show
the household's shared view. **View Music As** switches the whole dashboard to
one linked Person: their own library to browse and search, **their own playback
queue** on the Playlist tab (with its own transport controls), and **their own
AI-named mixes** on the Recommendations tab.

- Reach it from the **View Music As** card on the Browse Library,
  Recommendations, and People tabs, or from **View Their Music** on a Person's
  card (or inside their Edit modal); only linked People are
  offered, plus "Back to Household".
- While viewing, playing from the dashboard goes to **that Person's queue** (and
  records their listening history), and the stats row shows who you're viewing.
- Voice is unaffected — requests always follow the speaking Person.

### Follow-Me presence

Optionally, a linked Person's music can **follow them room to room**. Enable
**Follow-Me Presence** in the core settings, then give the Person a
**Home Assistant Person** entity in their People card (e.g. `person.john`) —
any presence stack works, including BLE trackers that compute the closest node
and update the person entity (Bermuda, ESPHome, phone GPS, …). The core
reuses Tater's built-in Home Assistant integration for the base URL and token
(no separate credentials) and polls HA's REST API for the entity's state,
which is the friendly name of the zone the Person is in.

- **Follows zones, resolves Tater rooms.** When the zone changes and holds for
  the move delay (default 20 s), the Person's queue hands off to that room at
  the same spot in the track — using Tater's room model (which Sat/player is
  in which room). Zone names that differ from Tater room names can be mapped
  per Person with **Zone to Room Overrides** (`The Kitchen=Kitchen, …`).
- **Resume delays (time to walk between rooms).** Three settings put a pause
  between the move and the music, each overridable per Person on the link card
  (blank = use the global setting; 0 = resume immediately):
  - **Follow-Me Move Resume Delay (sec)**: when the queue hands off to the room
    they just walked into, that room waits this many seconds before the music
    resumes at the same spot (on top of the Move Delay that decides *when* the
    move happens).
  - **Follow-Me Resume Delay (sec)**: when a follow-me pause ends because they
    reappear in a room with speakers, the music waits this many seconds before
    resuming there.
  - **Transfer Resume Delay (sec)**: the same idea for manual room-to-room
    transfers (see [Room-to-room transfer](#room-to-room-transfer)).
  During any wait the queue sits paused at the exact spot, and play / pause /
  stop in the meantime cancels the wait.
- **Room takeover is user-selectable.** When the room they walk into is already
  playing someone else's music: **Auto take over** (default) frees the room
  immediately, or **Ask before taking over** asks over TTS in that room and
  waits for the Person's yes/no (expires after 10 minutes, like other
  conflicts). Settable globally in the core settings and per Person on their
  card.
- **Away behavior is user-selectable (3 options).** In a zone with no
  speakers, or when the person is `not_home`:
  - **Keep in dead rooms, pause when away** (default)
  - **Pause whenever they leave a speaker room**
  - **Never pause; only move into rooms**
  A follow-me pause resumes automatically when they reappear in a room with
  speakers, at the same spot in the track.
- **Visibility.** The **Follow-Me Presence** system task shows the last check
  and errors; each Person card shows their current state (e.g. "Follow-me: in
  Kitchen → Kitchen", "paused (away from home)", "person entity not found in
  Home Assistant").

Follow-Me is off by default and requires Tater's Home Assistant integration to
be configured (base URL + token); without it the system task explains what to
enable.

## Settings worth knowing

| Setting | Default | Notes |
| --- | --- | --- |
| Stream Server Port | `8621` | Local HTTP port the core serves token-authenticated provider streams and share files from. Must be reachable from your playback targets on the LAN. |
| Stream Host | auto | Override only if the auto-detected LAN address is wrong (e.g. multiple NICs). |
| Catalog Sync Interval | `900` s | Also drives per-person catalog refreshes (each linked source gets its own). |
| Endless Playback | `Basic Auto (LLM)` | How queues keep playing after their final track (see [Endless Playback](#endless-playback)); per-Person override on the link card. |
| Endless Playback Playlist | blank | Playlist looped by the "Tracks from a playlist" mode (AI mix, server playlist, share `.m3u`, or Folder Playlists entry). |
| Playlist Order | `Shuffle each play` | Fixed ordering for AI mixes and picked playlists (track number, title, artist, album; asc/desc) instead of shuffling every play. |
| Folder Playlists | blank | `Name=Folder` pairs that turn library folders into always-current playlists (see [Playlists](#playlists)); per-Person override on the link card (blank inherits the global list). |
| Smart Shuffle | off | History-aware shuffle with on-the-fly multi-source mixing; per-Person override on the link card. |
| Resume in Another Room | `Stay where it was` | What "resume my music" does from a room other than the paused queue's room: stay, follow to the speaking room, or ask over TTS (per-Person override on the link card). |
| Follow-Me Presence | off | Master switch for following linked People's Home Assistant person entities (see [Follow-Me presence](#follow-me-presence)). |
| Follow-Me Poll Interval | `15` s | How often HA is polled for each tracked Person (5–3600 s). |
| Follow-Me Move Delay | `20` s | How long a new zone must hold before the music moves (prevents hallway flicker). |
| Transfer Resume Delay | `0` s | Wait before music resumes in the room a transfer moved to — time to walk between rooms (per-Person override on the link card). |
| Follow-Me Move Resume Delay | `0` s | Same wait, but for follow-me zone handoffs. |
| Follow-Me Resume Delay | `0` s | Same wait, but for resuming after a follow-me away pause. |

## Limitations

- **Smart fading (BPM-aware crossfade) is not implemented** — it needs changes
  to the Tater host's playback engine (`media_playback`), so it is written up as
  a feature request in
  `docs/feature-requests/playback-engine-smart-bpm-crossfade.md` instead.
- A Person can link **two** sources (one primary plus one second source, any
  mix of Emby/Jellyfin/Subsonic/Plex accounts and share folders); more would
  need a longer form.
- Streaming providers (Spotify, Apple Music, …) are not built in; the
  provider-backed Endless Playback modes fall back to the library until one is
  added via the provider registry.
- **Audio formats:** satellite speakers decode WAV, MP3, and FLAC natively;
  AAC, OGG, Opus, and M4A have no decoder and will not play — libraries are
  assumed to be WAV/MP3/FLAC (see
  [Supported audio formats](#supported-audio-formats)). There is no core-side
  transcode stage: Subsonic, Plex, and network shares serve original files
  (Subsonic only asks the server for WAV on non-decodable containers), and on
  Emby/Jellyfin the clock-sync path requests a normalized WAV source but the
  server decides whether to transcode — with transcoding disabled it serves
  the original file.
- Little Spud client music is not switched over — the Tater host currently links
  client music to the stock `music_core` only, and this core's client music
  surface (`run_client_music_action`, `get_client_music_stream_source`) follows
  the shared household queue, not a Person queue.
- Network-share playback serves original files; there is no on-the-fly transcode
  (Emby/Jellyfin's clock-sync path requests a normalized WAV source from the
  server, but the server decides whether to honor it).
- Volume and per-target calibrations are shared per destination; two queues
  playing different rooms at once keep their own volume, but the same room's
  calibration is shared.
- By default the dashboard player bar and Playlist tab show the shared
  household queue; per-Person queue state is visible on each Person's card in
  the People section, or dashboard-wide via [View Music As](#view-music-as)
  (one viewer at a time — Tater's WebUI doesn't yet tell cores who is viewing,
  so the core can't scope the dashboard per viewer automatically).
- Follow-Me tracks one zone per Person (their Home Assistant person entity's
  state) and moves the whole queue to the single room that zone resolves to.
  Zones with no matching Tater room are dead zones (handled by the selected
  away action), and Follow-Me reuses Tater's Home Assistant credentials — it
  cannot track a second Home Assistant instance.

## Future directions

- **Routing by provider name** ("play Pandora", "play it from the Plex
  library") — when several sources are linked, a request could pick the named
  provider. That needs host-side voice/UI support in Tater, so it is noted
  here, not built; every linked source is already independently addressable
  by its provider id, and multi-source catalogs already play from wherever a
  track lives.
- **More providers.** The provider registry is data-driven, so a new catalog
  provider joins as one class plus one form entry. A server-side-transcode
  capability is expected of new providers; the core's ffmpeg stage is the
  designated fallback for providers that cannot transcode on their own.

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
git diff upstream/main:cores/music_core.py cores/personal_music_core.py
```

Port relevant hunks by hand (do not merge — upstream contains shop files this
repo does not carry). The goal is absorbing upstream bug fixes, not staying
byte-identical.

## License / provenance

Derived from TaterTotterson's Tater_Shop `cores/music_core.py` (see
`tests/fixtures/upstream_music_core.py` for the pre-divergence snapshot this
build started from).