"""Per-person Emby and network-share music library, voice playback, queue, and built-in player for Tater.

Derived from Tater_Shop's ``cores/music_core.py`` (pure-rename commit) so upstream
changes stay diffable, but rebuilt around a provider abstraction:

- ``EmbyMusicProvider`` — a per-configuration Emby server (one server, one music
  view) speaking Emby's REST API directly.
- ``NetworkShareMusicProvider`` — a locally mounted SMB/NFS share: the Tater host
  (or container, via a bind mount) is expected to mount the share, and this core
  scans it like a local folder. Tags are read with a stdlib parser (ID3v2,
  FLAC/Vorbis, MP4, Ogg/Opus, WAV) so no extra image packages are required, and
  files stream to playback targets through this core's own Range-capable stream
  server.

Per-person linkage (Emby user or share subfolder per Person) is layered on top
of the provider abstraction in a later phase using core-owned Redis keys.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import importlib.util
import io
import json
import logging
import math
import os
import random
import re
import socket
import struct
import tempfile
import threading
import time
import uuid
import wave
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlparse
from xml.etree import ElementTree

import requests

from helpers import extract_json, get_llm_client_from_env, redis_client

try:
    from helpers import get_primary_llm_client_from_env as _get_primary_llm_client_from_env
except Exception:  # pragma: no cover - compatibility with older Tater runtimes.
    _get_primary_llm_client_from_env = get_llm_client_from_env


__version__ = "3.7.0"
MIN_TATER_VERSION = "1.2.0"
CORE_DESCRIPTION = (
    "Per-person music for Tater: link each Person to their own Emby, Jellyfin, Subsonic, or Plex account or "
    "network-share folder (or two sources at once), browse "
    "and play their library with voice control, and build AI-named recommendations from each Person's listening "
    "history across clock-synchronized satellites, native Sonos groups, stereo pairs, and media players — with "
    "selectable Endless Playback modes (AI radio, offline Infinite Mix, or looping a chosen playlist), Smart "
    "Shuffle that mixes several queued sources on the fly, per-Person sleep timers, and optional Follow-Me "
    "presence that moves a Person's music to the room their Home Assistant person entity reports."
)
TAGS = [
    "music",
    "player",
    "emby",
    "jellyfin",
    "subsonic",
    "plex",
    "network-share",
    "per-person",
    "satellite",
    "stereo",
    "multi-room",
    "queue",
    "recommendations",
    "album-art",
    "follow-me",
]

logger = logging.getLogger("personal_music_core")
logger.setLevel(logging.INFO)

# Endless Playback: how a queue keeps playing once it reaches its final track.
# Modes that need a streaming provider (Spotify, Apple Music, …) fall back to
# the library mix until one is connected, so music never stops.
ENDLESS_PLAYBACK_MODES = (
    "llm_auto",  # the original behaviour: AI (LLM) picks similar tracks from the library
    "automatic",  # streaming providers first, then an Infinite Mix from the library
    "library_mix",  # Infinite Mix drawn straight from the library (works fully offline)
    "similar_played",  # streaming providers only (library fallback keeps music playing)
    "playlist_loop",  # loop tracks from one chosen playlist
)
DEFAULT_ENDLESS_PLAYBACK_MODE = "llm_auto"
# Listening-history window that counts as "recently played" for the library mix
# and Smart Shuffle (events, not tracks).
SMART_SHUFFLE_RECENT_EVENTS = 40
# Smart Shuffle keeps the on-screen queue to a rolling window and holds the rest
# of the selection in a pool it draws from as the queue drains (mixing sources
# on the fly instead of building one enormous queue up front).
SMART_SHUFFLE_QUEUE_WINDOW = 30

CORE_SETTINGS = {
    "category": "Personal Music Core Settings",
    "hydra_tools_require_running": True,
    "required": {
        "stream_bind_port": {
            "label": "Stream Server Port",
            "type": "number",
            "default": 8621,
            "description": (
                "Local HTTP port this core serves music streams from so playback targets can fetch "
                "Emby token-authenticated audio and network-share files. Requires a free port on the host."
            ),
        },
        "stream_host": {
            "label": "Stream Host (IP or name)",
            "type": "text",
            "default": "",
            "description": (
                "Address playback targets use to reach this Tater's stream server. Leave blank to use the "
                "auto-detected LAN address."
            ),
        },
        "catalog_sync_interval_seconds": {
            "label": "Catalog Sync Interval (sec)",
            "type": "number",
            "default": 900,
            "description": "How often Personal Music Core refreshes artists, albums, genres, and tracks.",
        },
        "default_targets": {
            "label": "Default Speakers",
            "type": "text",
            "default": "",
            "description": "Fallback playback destinations when a room or speaking satellite is unavailable.",
        },
        "default_volume_percent": {
            "label": "Default Volume (%)",
            "type": "number",
            "default": 75,
            "description": "Starting volume for Tater satellite media sessions.",
        },
        "mixed_sync_default_adjustment_ms": {
            "label": "Default Mixed Sync Adjustment (ms)",
            "type": "number",
            "default": 0,
            "description": (
                "Fine-tunes mixed Sonos and Tater satellite groups. Positive values delay satellites; "
                "negative values start them earlier."
            ),
        },
        "default_shuffle": {
            "label": "Shuffle Broad Requests",
            "type": "checkbox",
            "default": True,
            "description": "Shuffle genre, artist, and general music requests by default.",
        },
        "maximum_queue_tracks": {
            "label": "Maximum Queue Tracks",
            "type": "number",
            "default": 200,
            "description": "Maximum number of matched tracks placed in one queue.",
        },
        "airplay_receiver_enabled": {
            "label": "AirPlay Receiver",
            "type": "checkbox",
            "default": False,
            "description": "Let Apple devices send live audio to selected Tater Native, Sonos, and AirPlay speakers.",
        },
        "airplay_receiver_name": {
            "label": "AirPlay Receiver Name",
            "type": "text",
            "default": "Tater Music",
            "description": "The name advertised in the AirPlay speaker picker.",
        },
        "airplay_receiver_pin": {
            "label": "AirPlay Pairing PIN",
            "type": "password",
            "default": "",
            "description": "Optional fixed four-digit PIN used when a device pairs for the first time.",
        },
        "airplay_receiver_targets": {
            "label": "AirPlay Destinations",
            "type": "text",
            "default": "",
            "description": "Tater Native satellites, stereo pairs, AirPlay-capable Sonos players, or AirPlay speakers that play incoming audio.",
        },
        "recommendations_enabled": {
            "label": "Music Recommendations",
            "type": "checkbox",
            "default": True,
            "description": "Use listening history and Tater's primary AI model to prepare named music mixes.",
        },
        "recommendation_interval_hours": {
            "label": "Recommendation Refresh (hours)",
            "type": "number",
            "default": 12,
            "description": "How often Personal Music Core refreshes music recommendations in the background.",
        },
        "recommendation_playlist_count": {
            "label": "Recommendation Playlists",
            "type": "number",
            "default": 3,
            "description": "Number of AI-named recommendation playlists to prepare.",
        },
        "recommendation_items_per_playlist": {
            "label": "Albums & Songs Per Playlist",
            "type": "number",
            "default": 6,
            "description": "Maximum recommended albums and songs in each playlist.",
        },
        "prompt_context_enabled": {
            "label": "Music Prompt Context",
            "type": "checkbox",
            "default": True,
            "description": "Share the selected Person's compact music profile with Tater when that Person is speaking.",
        },
        "prompt_profile_interval_hours": {
            "label": "Music Profile Refresh (hours)",
            "type": "number",
            "default": 12,
            "description": "How often Personal Music Core refreshes the selected Person's prompt-ready listening profile.",
        },
        "queue_conflict_mode": {
            "label": "Playback Conflicts",
            "type": "select",
            "default": "ask",
            "options": [
                {"value": "ask", "label": "Ask before taking over"},
                {"value": "auto_move", "label": "Auto-move / take over"},
            ],
            "description": (
                "What to do when requested rooms are already playing someone else's music, or a "
                "Person asks for music while their own music plays elsewhere. Each Person can "
                "override this on their link card in the People section."
            ),
        },
        "follow_me_enabled": {
            "label": "Follow-Me (Home Assistant Presence)",
            "type": "checkbox",
            "default": False,
            "description": (
                "Track each Person's Home Assistant person entity (for example one your BLE "
                "trackers update as they move between rooms) and automatically move their music "
                "to the room they're in. Requires the Home Assistant integration configured in "
                "Tater (base URL and token); set each Person's entity on their card in the "
                "People section."
            ),
        },
        "follow_me_poll_interval_seconds": {
            "label": "Follow-Me Check Interval (sec)",
            "type": "number",
            "default": 15,
            "description": (
                "How often Personal Music Core checks the linked People's Home Assistant person "
                "entities for a new room."
            ),
        },
        "follow_me_move_delay_seconds": {
            "label": "Follow-Me Move Delay (sec)",
            "type": "number",
            "default": 20,
            "description": (
                "A new room must remain the Person's detected location for this long before "
                "their music moves, so brief BLE flaps between rooms don't bounce the music."
            ),
        },
        "resume_room_mode": {
            "label": "Resume in Another Room",
            "type": "select",
            "default": "stay",
            "options": [
                {"value": "stay", "label": "Stay where it was"},
                {"value": "follow", "label": "Follow me to this room"},
                {"value": "ask", "label": "Ask me each time"},
            ],
            "description": (
                "When someone says \"resume my music\" from a different room than the one "
                "their paused music is in: Stay where it was resumes it in the original room; "
                "Follow me to this room moves it to the room they're speaking in (unless "
                "another queue is already playing there, in which case it stays put); Ask me "
                "each time asks over TTS. Each Person can override this on their link card."
            ),
        },
        "transfer_resume_delay_seconds": {
            "label": "Transfer Resume Delay (sec)",
            "type": "number",
            "default": 0,
            "description": (
                "Room-to-room transfers and follow-me handoffs (\"transfer/move my music to "
                "the Kitchen\", the control tool's move/set player actions, or changing the "
                "Play On destinations while music is playing) pause the music and let the "
                "gaining room wait this many seconds before it resumes at the same spot — "
                "time to walk between rooms. 0 resumes immediately. Each Person can override "
                "this on their link card."
            ),
        },
        "follow_me_move_resume_delay_seconds": {
            "label": "Follow-Me Move Resume Delay (sec)",
            "type": "number",
            "default": 0,
            "description": (
                "Follow-Me presence: when the music hands off to the room the Person just "
                "walked into, the new room waits this many seconds before it resumes at the "
                "same spot — on top of the Move Delay that decides when the move happens. "
                "0 resumes immediately. Each Person can override this on their link card."
            ),
        },
        "follow_me_resume_delay_seconds": {
            "label": "Follow-Me Resume Delay (sec)",
            "type": "number",
            "default": 0,
            "description": (
                "Follow-Me presence: when a follow-me pause resumes automatically (the Person "
                "reappears in a room with speakers), that room waits this many seconds before "
                "the music resumes at the same spot. 0 resumes immediately. Each Person can "
                "override this on their link card."
            ),
        },
        "follow_me_takeover_mode": {
            "label": "Follow-Me Room Takeover",
            "type": "select",
            "default": "auto",
            "options": [
                {"value": "auto", "label": "Auto take over"},
                {"value": "ask", "label": "Ask before taking over"},
            ],
            "description": (
                "Default for what Follow-Me does when the room a Person walks into is already "
                "playing someone else's music: take it over automatically (the other queue is "
                "paused in place), or ask over TTS first. Each Person can override this on "
                "their link card."
            ),
        },
        "follow_me_away_action": {
            "label": "Follow-Me Away Behavior",
            "type": "select",
            "default": "keep_pause",
            "options": [
                {"value": "keep_pause", "label": "Keep in dead rooms, pause when away"},
                {"value": "pause", "label": "Pause whenever they leave a speaker room"},
                {"value": "keep", "label": "Never pause; only move into rooms"},
            ],
            "description": (
                "Default for what happens to a Person's music when they're in a zone with no "
                "speakers (a dead room) or outside the home. Music paused by Follow-Me resumes "
                "automatically when they reappear in a room with speakers. Each Person can "
                "override this on their link card."
            ),
        },
        "endless_playback_mode": {
            "label": "Endless Playback",
            "type": "select",
            "default": DEFAULT_ENDLESS_PLAYBACK_MODE,
            "options": [
                {"value": "automatic", "label": "Automatic"},
                {"value": "llm_auto", "label": "Basic Auto (LLM)"},
                {"value": "library_mix", "label": "Infinite Mix from your library"},
                {"value": "similar_played", "label": "Similar to what you played"},
                {"value": "playlist_loop", "label": "Tracks from a playlist"},
            ],
            "description": (
                "How queues keep playing after their final track. Automatic fetches similar "
                "tracks from a connected streaming provider and falls back to an Infinite Mix "
                "from your library; Basic Auto (LLM) has the AI model pick similar library "
                "tracks (the original behaviour); Infinite Mix draws from your library "
                "prioritising least-played tracks in the genres you were just hearing; Similar "
                "to what you played relies on streaming providers (until one is connected it "
                "falls back to the library mix); Tracks from a playlist loops one chosen "
                "playlist. Each Person can override this on their link card."
            ),
        },
        "endless_playback_playlist": {
            "label": "Endless Playback Playlist",
            "type": "text",
            "default": "",
            "description": (
                "Playlist name for the \"Tracks from a playlist\" Endless Playback mode — one of "
                "the AI-named mixes on the Recommendations tab, a playlist you created in Emby, "
                "an .m3u/.m3u8 playlist file on the share, or a Folder Playlists entry (matched "
                "by name, case-insensitive). Leave blank to use the newest mix. Each Person can "
                "pick their own on their link card."
            ),
        },
        "recommendation_playlist_order": {
            "label": "Playlist Order",
            "type": "select",
            "default": "shuffle",
            "options": [
                {"value": "shuffle", "label": "Shuffle each play"},
                {"value": "track_asc", "label": "Track number (1 → 9)"},
                {"value": "track_desc", "label": "Track number (9 → 1)"},
                {"value": "title_asc", "label": "Title (A → Z)"},
                {"value": "title_desc", "label": "Title (Z → A)"},
                {"value": "artist_asc", "label": "Artist (A → Z)"},
                {"value": "artist_desc", "label": "Artist (Z → A)"},
                {"value": "album_asc", "label": "Album (A → Z)"},
                {"value": "album_desc", "label": "Album (Z → A)"},
            ],
            "description": (
                "How AI-named mixes (dynamic playlists) and the \"Tracks from a playlist\" "
                "loop are ordered when they play. Shuffle each play keeps today's behaviour; "
                "the other options play the playlist in a fixed order — by track number, "
                "title, artist, or album, ascending or descending — so it sounds the same "
                "every time."
            ),
        },
        "folder_playlists": {
            "label": "Folder Playlists",
            "type": "text",
            "default": "",
            "description": (
                "Turn library folders into always-up-to-date playlists: \"Christmas Music="
                "Christmas, Road Trip=Tunes/Road\" builds a \"Christmas Music\" playlist from "
                "every track under the Christmas folder and a \"Road Trip\" playlist from the "
                "Tunes/Road folder (Name=Folder pairs, comma-separated; subfolders are "
                "included). The playlists are rebuilt from the library on every sync and "
                "play, so songs added to the folder join automatically. They work everywhere "
                "a picked playlist works — the \"Tracks from a playlist\" Endless Playback "
                "mode and voice (\"play my Christmas Music playlist\"). Each linked Person can "
                "replace this list with their own on their link card in the People section."
            ),
        },
        "smart_shuffle_enabled": {
            "label": "Smart Shuffle",
            "type": "checkbox",
            "default": False,
            "description": (
                "History-aware shuffle: recently played tracks are pushed to the back of the "
                "queue, and queueing several albums or playlists mixes them on the fly instead "
                "of building one enormous queue up front. Each Person can override this on "
                "their link card."
            ),
        },
    },
    "tags": TAGS,
}

CORE_WEBUI_TAB = {
    "label": "Personal Music",
    "order": 37,
    "requires_running": True,
}

# Data keys use the "personal_music_core:" prefix so Tater's core data cleanup
# (core_store.clear_core_redis_data) sweeps them for any installed core without
# needing an entry in the host's audited ownership table. Settings and the
# autostart marker use the conventional "<module_key>_settings/_running" forms.
SETTINGS_KEY = "personal_music_core_settings"
RUNTIME_KEY = "personal_music_core:runtime"
PERSON_LINKS_KEY = "personal_music_core:person_links"
# Last "Test Connection" outcome. The music tab UI (Tater v1.2.0+) toasts the
# action's success message, but a toast is gone on the next click — the record
# here keeps the outcome visible on the link card as a summary row.
PERSON_LINK_TEST_KEY = "personal_music_core:person_link_test"
# Compact sync outcome per catalog ("track/artist/album/genre counts +
# synced_at", or "syncing/error") keyed by person id ("" = the household
# catalog). Link cards and the search card read this instead of decoding
# multi-megabyte catalog payloads on every tab render.
CATALOG_STATS_KEY = "personal_music_core:catalog_stats"
CATALOG_KEY = "personal_music_core:catalog:v1"
PLAYER_KEY = "personal_music_core:player"
# Per-person queues live at "personal_music_core:player:<person_id>" while the
# shared household queue stays at "personal_music_core:player" ("" queue id), so
# existing installs keep their global player state untouched.
QUEUE_REGISTRY_KEY = "personal_music_core:queues"
ROOM_BINDINGS_KEY = "personal_music_core:room_bindings"
PENDING_CONFIRM_KEY_PREFIX = "personal_music_core:pending:"
PENDING_CONFIRM_TTL_SECONDS = 600.0
QUEUE_CONFLICT_MODES = ("ask", "auto_move")
DEFAULT_QUEUE_CONFLICT_MODE = "ask"
# Jarvis Screen browser playback destinations ("screen:<screen_key>"). The
# profiles hash belongs to the Jarvis Screen core and is read-only here: the
# screen core's uninstall sweep derives its Redis namespace generically, so
# nothing is ever written under "jarvis_screen:*".
SCREEN_TARGET_PREFIX = "screen:"
SCREEN_PROFILES_KEY = "jarvis_screen:profiles"
# Follow-Me presence: each linked Person can carry a Home Assistant person
# entity (e.g. one a BLE tracker updates as they move between rooms). When the
# entity reports a new room, that Person's queue hands off to it. Per-Person
# tracking state lives at "personal_music_core:follow_me:<person_id>"; the HA
# base URL and token are reused from Tater's built-in Home Assistant
# integration (host-owned key "homeassistant_settings" — read only).
FOLLOW_ME_KEY = "personal_music_core:follow_me"
HA_SETTINGS_KEY = "homeassistant_settings"
HA_DEFAULT_BASE_URL = "http://homeassistant.local:8123"
FOLLOW_ME_TAKEOVER_MODES = ("auto", "ask")
DEFAULT_FOLLOW_ME_TAKEOVER_MODE = "auto"
FOLLOW_ME_AWAY_ACTIONS = ("keep_pause", "pause", "keep")
DEFAULT_FOLLOW_ME_AWAY_ACTION = "keep_pause"
# Voice resume behavior when the paused queue sits somewhere other than the
# room the speaker is asking from ("resume my music" said from another room).
RESUME_ROOM_MODES = ("stay", "follow", "ask")
DEFAULT_RESUME_ROOM_MODE = "stay"
FOLLOW_ME_DEFAULT_POLL_SECONDS = 15
FOLLOW_ME_DEFAULT_MOVE_DELAY_SECONDS = 20.0
# (connect, read) timeout for one HA REST poll; keeps a dead HA from stalling
# the background loop.
HA_STATE_TIMEOUT_SECONDS = (3.0, 8.0)
HISTORY_KEY = "personal_music_core:history:v1"
RECOMMENDATIONS_KEY = "personal_music_core:recommendations:v1"
PROMPT_PROFILE_KEY = "personal_music_core:profile:v1"
ACTIVITY_KEY = "personal_music_core:activity_feed"
# Legacy pre-3.5.0 shared Emby auth cache (household and person tokens used to
# overwrite each other here). Auth caches are now keyed per account identity
# (see _provider_auth_cache_key); this key is only cleared, never read.
EMBY_AUTH_CACHE_KEY = "personal_music_core:emby:auth"
MAX_ACTIVITY_EVENTS = 200
REQUEST_TIMEOUT_SECONDS = 30
ARTWORK_CONNECT_TIMEOUT_SECONDS = 2.0
ARTWORK_READ_TIMEOUT_SECONDS = 5.0
ARTWORK_INFLIGHT_WAIT_TIMEOUT_SECONDS = 6.0
ARTWORK_FAILURE_CACHE_SECONDS = 15.0
ARTWORK_MAX_CONCURRENT_FETCHES = 4
DEFAULT_SYNC_INTERVAL_SECONDS = 900
MAX_CATALOG_TRACKS = 20000
MAX_SEARCH_RESULTS = 100
MAX_HISTORY_EVENTS = 300
MAX_RECOMMENDATION_CANDIDATES = 200
MAX_PROMPT_CONTEXT_CHARS = 1400
CATALOG_ARTWORK_SCHEMA = 4
CATALOG_MEMORY_CACHE_TTL_SECONDS = 15.0
CONTINUATION_TRIGGER_REMAINING_TRACKS = 2
CONTINUATION_BATCH_TRACKS = 12
MAX_CONTINUATION_CANDIDATES = 200
PROVIDER_LABELS = {
    "emby": "Emby",
    "jellyfin": "Jellyfin",
    "subsonic": "Subsonic",
    "plex": "Plex",
    "network_share": "Network Share",
}
CATALOG_PROVIDER_IDS = {"emby", "jellyfin", "subsonic", "plex", "network_share"}
# Display/registration order for provider pickers and source cards (Emby first,
# preserving today's card order; the share stays last as the offline option).
CATALOG_PROVIDER_ORDER = ("emby", "jellyfin", "subsonic", "plex", "network_share")
# Containers the Tater satellites decode natively (firmware playback.c); every
# known provider is wired to send only these (or ask its server to transcode).
SAT_SAFE_AUDIO_CONTAINERS = {"wav", "mp3", "flac"}
EMBY_PAGE_SIZE = 500
EMBY_ARTWORK_MAX_WIDTH = 1000
SUBSONIC_API_VERSION = "1.16.1"
SUBSONIC_CLIENT_NAME = "PersonalMusicCore"
SUBSONIC_PAGE_SIZE = 500
PLEX_PAGE_SIZE = 100
PLEX_TV_API_BASE = "https://plex.tv/api/v2"
PLEX_AUTH_MODES = ("home_user", "own_account", "manual_token")
DEFAULT_PLEX_AUTH_MODE = "home_user"
STREAM_DEFAULT_PORT = 8621
STREAM_CHUNK_SIZE = 128 * 1024
SHARE_AUDIO_EXTENSIONS = {".mp3", ".flac", ".ogg", ".oga", ".opus", ".m4a", ".mp4", ".wav", ".wma"}
SHARE_PLAYLIST_EXTENSIONS = {".m3u", ".m3u8"}
# Case-insensitive folder-image names, checked in this order.
SHARE_FOLDER_ARTWORK_NAMES = (
    "cover.jpg",
    "folder.jpg",
    "cover.png",
    "folder.png",
    "album.jpg",
    "album.png",
    "albumart.jpg",
    "front.jpg",
    "front.png",
)
# Redis hash mapping artwork id -> {"path": ..., "version": ...} for share files.
SHARE_ART_INDEX_KEY = "personal_music_core:share:art"
SHARE_ART_CACHE_DIRNAME = "personal_music_artwork"
SHARE_MIME_TYPES = {
    ".mp3": "audio/mpeg",
    ".flac": "audio/flac",
    ".ogg": "audio/ogg",
    ".oga": "audio/ogg",
    ".opus": "audio/ogg",
    ".m4a": "audio/mp4",
    ".mp4": "audio/mp4",
    ".wav": "audio/wav",
    ".wma": "audio/x-ms-wma",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
}
GENERIC_SEARCH_WORDS = {
    "a",
    "an",
    "and",
    "for",
    "from",
    "me",
    "music",
    "of",
    "on",
    "play",
    "please",
    "some",
    "song",
    "songs",
    "the",
}

# Broad browse/search families. Specific source tags remain visible, while a
# track such as "Roots Reggae" is also discoverable through "Reggae".
GENRE_FAMILIES = (
    ("Alternative", ("alternative", "indie")),
    ("Blues", ("blues",)),
    ("Children's", ("children s music", "kids music", "nursery rhyme")),
    ("Classical", ("classical", "baroque", "opera", "orchestral", "chamber music")),
    ("Country", ("country", "bluegrass", "americana", "honky tonk")),
    ("Dance", ("dance", "disco")),
    (
        "Electronic",
        (
            "electronic",
            "electronica",
            "edm",
            "house",
            "techno",
            "trance",
            "ambient",
            "dubstep",
            "drum and bass",
            "dnb",
        ),
    ),
    ("Folk", ("folk", "singer songwriter")),
    ("Gospel", ("gospel", "worship", "christian music")),
    ("Hip-Hop/Rap", ("hip hop", "hiphop", "rap", "trap", "boom bap")),
    ("Holiday", ("holiday", "christmas music")),
    ("Jazz", ("jazz", "bebop", "swing")),
    ("Latin", ("latin", "salsa", "reggaeton", "bachata", "merengue", "bossa nova")),
    ("Metal", ("metal",)),
    ("New Age", ("new age",)),
    ("Pop", ("pop", "kpop")),
    ("Punk", ("punk",)),
    ("R&B/Soul", ("r b", "rnb", "rhythm and blues", "soul", "funk", "motown")),
    ("Reggae", ("reggae", "dub", "dancehall", "ska", "rocksteady")),
    ("Rock", ("rock", "grunge")),
    ("Soundtrack", ("soundtrack", "film score", "video game music")),
    ("Spoken Word", ("spoken word", "audiobook")),
    ("World", ("world music", "afrobeat", "highlife")),
)
GENRE_CANONICAL_NAMES = {
    "alternative": "Alternative",
    "blues": "Blues",
    "children s": "Children's",
    "children s music": "Children's",
    "kids music": "Children's",
    "classical": "Classical",
    "country": "Country",
    "dance": "Dance",
    "electronic": "Electronic",
    "folk": "Folk",
    "gospel": "Gospel",
    "hip hop": "Hip-Hop/Rap",
    "hiphop": "Hip-Hop/Rap",
    "hip hop rap": "Hip-Hop/Rap",
    "holiday": "Holiday",
    "jazz": "Jazz",
    "latin": "Latin",
    "metal": "Metal",
    "new age": "New Age",
    "pop": "Pop",
    "punk": "Punk",
    "r b": "R&B/Soul",
    "r b soul": "R&B/Soul",
    "rnb": "R&B/Soul",
    "rhythm and blues": "R&B/Soul",
    "reggae": "Reggae",
    "rock": "Rock",
    "soundtrack": "Soundtrack",
    "spoken word": "Spoken Word",
    "world": "World",
    "world music": "World",
}

_state_lock = threading.RLock()
# Serializes follow-me presence passes (background loop + manual task runs)
# so two ticks can't double-move or double-speak for the same Person.
_follow_me_lock = threading.Lock()
_artwork_cache_lock = threading.RLock()
_artwork_cache: Dict[str, Dict[str, Any]] = {}
_artwork_inflight: Dict[str, threading.Event] = {}
_artwork_failure_until: Dict[str, float] = {}
_artwork_fetch_slots = threading.BoundedSemaphore(ARTWORK_MAX_CONCURRENT_FETCHES)
_catalog_memory_cache_lock = threading.RLock()
_catalog_memory_cache: Dict[str, Any] = {
    "store": None,
    "loaded_at": 0.0,
    "payload": {},
}
_catalog_sync_lock = threading.Lock()
_catalog_sync_started_at = 0.0
# Worker thread for tab-triggered catalog syncs (View Their Music on a Person
# whose library was never loaded); "" scope = the household catalog.
_catalog_sync_thread: Optional[threading.Thread] = None
_recommendation_lock = threading.Lock()
_recommendation_started_at = 0.0
_recommendation_thread: Optional[threading.Thread] = None
_profile_lock = threading.Lock()
_profile_started_at = 0.0
_profile_thread: Optional[threading.Thread] = None
_continuation_lock = threading.Lock()
_continuation_started_at = 0.0
# One radio-continuation worker per queue slot ("" = shared household queue) so
# two Person queues can extend their queues at the same time.
_continuation_threads: Dict[str, Optional[threading.Thread]] = {}
_client_continuation_lock = threading.Lock()


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace").strip()
    return str(value).strip()


def _assistant_first_name(client: Any = None) -> str:
    store = client or globals().get("redis_client")
    try:
        value = _text(store.get("tater:first_name")) if store is not None else ""
    except Exception:
        value = ""
    first_name = value.split()[0] if value else ""
    return first_name[:48] or "Tater"


def _assistant_possessive(client: Any = None) -> str:
    name = _assistant_first_name(client)
    return f"{name}'" if name.casefold().endswith("s") else f"{name}'s"


def _recommendations_label(client: Any = None) -> str:
    return f"{_assistant_possessive(client)} Recommendations"


def _list(value: Any) -> List[str]:
    raw: List[Any]
    if isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        token = _text(value)
        parsed: Any = None
        if token.startswith("[") and token.endswith("]"):
            try:
                parsed = json.loads(token)
            except Exception:
                parsed = None
        if isinstance(parsed, list):
            raw = parsed
        else:
            raw = token.replace("\n", ",").split(",") if token else []
    result: List[str] = []
    seen = set()
    for item in raw:
        token = _text(item)
        key = token.casefold()
        if not token or key in seen:
            continue
        seen.add(key)
        result.append(token)
    return result


def _as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).lower()
    if token in {"1", "true", "yes", "on", "enabled"}:
        return True
    if token in {"0", "false", "no", "off", "disabled"}:
        return False
    return bool(default)


def _as_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        parsed = int(default)
    return max(minimum, min(maximum, parsed))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


_PEOPLE_API_MODULE: Any = None
_PEOPLE_API_UNAVAILABLE = False


def _people_api_module() -> Any:
    global _PEOPLE_API_MODULE, _PEOPLE_API_UNAVAILABLE
    if _PEOPLE_API_MODULE is not None:
        return _PEOPLE_API_MODULE
    if _PEOPLE_API_UNAVAILABLE:
        return None
    try:
        import people as people_module  # type: ignore

        _PEOPLE_API_MODULE = people_module
        return _PEOPLE_API_MODULE
    except Exception:
        pass
    try:
        candidate = Path(__file__).resolve().parents[2] / "Tater" / "people.py"
        if candidate.exists():
            spec = importlib.util.spec_from_file_location("tater_people_api", candidate)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                _PEOPLE_API_MODULE = module
                return _PEOPLE_API_MODULE
    except Exception:
        pass
    _PEOPLE_API_UNAVAILABLE = True
    return None


def _people_person_rows(client: Any = None) -> List[Dict[str, Any]]:
    module = _people_api_module()
    load_store = getattr(module, "load_store", None) if module is not None else None
    if not callable(load_store):
        return []
    try:
        store = load_store(client or globals().get("redis_client"))
    except Exception:
        return []
    people = list(store.get("people") or []) if isinstance(store, dict) else []
    rows = [dict(row) for row in people if isinstance(row, dict)]
    rows.sort(key=lambda row: (_text(row.get("display_name")).casefold(), _text(row.get("id"))))
    return rows


def _people_person_name(person_id: Any, client: Any = None) -> str:
    wanted = _text(person_id)
    if not wanted:
        return ""
    for person in _people_person_rows(client):
        if _text(person.get("id")) == wanted:
            return _text(person.get("display_name")) or wanted
    return ""


def _people_person_options(client: Any = None) -> List[Dict[str, str]]:
    options = [{"value": "", "label": "Choose a person"}]
    for person in _people_person_rows(client):
        person_id = _text(person.get("id"))
        display_name = _text(person.get("display_name"))
        if person_id and display_name:
            options.append({"value": person_id, "label": display_name})
    return options


def _context_person_id(*sources: Any) -> str:
    for source in sources:
        if not isinstance(source, dict):
            continue
        for candidate in (source, source.get("people_resolution")):
            if not isinstance(candidate, dict):
                continue
            person_id = _text(candidate.get("master_user_id") or candidate.get("person_id"))
            if person_id:
                return person_id
    return ""


# --------------------------------------------------------------------------
# Per-person linkage. people.py exposes fixed person fields only (unknown
# fields are stripped on save), so each Person's music setup lives in this
# core-owned hash: person_id -> {"music_source": ..., "emby": {...}, ...}.
# --------------------------------------------------------------------------

def _person_links(client: Any = None) -> Dict[str, Dict[str, Any]]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    links: Dict[str, Dict[str, Any]] = {}
    try:
        raw = store.hgetall(PERSON_LINKS_KEY) or {}
    except Exception:
        return {}
    for person_id, value in raw.items():
        try:
            parsed = value if isinstance(value, dict) else json.loads(_text(value))
        except Exception:
            parsed = None
        if isinstance(parsed, dict) and _text(person_id):
            links[_text(person_id)] = parsed
    return links


def _person_link(person_id: Any, client: Any = None) -> Dict[str, Any]:
    wanted = _text(person_id)
    if not wanted:
        return {}
    return _person_links(client).get(wanted) or {}


def _save_person_link(person_id: Any, link: Dict[str, Any], client: Any = None) -> None:
    store = client or globals().get("redis_client")
    wanted = _text(person_id)
    if store is None or not wanted:
        return
    _save_hash(store, PERSON_LINKS_KEY, {wanted: json.dumps(link, sort_keys=True)})


def _delete_person_link(person_id: Any, client: Any = None) -> None:
    store = client or globals().get("redis_client")
    wanted = _text(person_id)
    if store is None or not wanted:
        return
    try:
        store.hdel(PERSON_LINKS_KEY, wanted)
    except Exception:
        pass


# Form fields the link test remembers between the test click and the tab
# refetch that follows it (see PERSON_LINK_TEST_KEY). The provider form fields
# are appended from the registry after PROVIDER_FIELD_SPECS is built.
_PERSON_LINK_TEST_CORE_FIELD_KEYS = (
    "person_link_person_id",
    "person_link_source",
    "person_link_extra_source",
    "person_link_queue_conflict_mode",
    "person_link_recommendations_enabled",
    "person_link_recommendation_interval_hours",
    "person_link_recommendation_playlist_count",
    "person_link_recommendation_items_per_playlist",
    "person_link_prompt_context_enabled",
    "person_link_endless_playback_mode",
    "person_link_endless_playback_playlist",
    "person_link_folder_playlists",
    "person_link_smart_shuffle_enabled",
    "person_link_follow_me_entity",
    "person_link_follow_me_room_overrides",
    "person_link_follow_me_takeover_mode",
    "person_link_follow_me_away_action",
    "person_link_transfer_resume_delay_seconds",
    "person_link_follow_me_move_resume_delay_seconds",
    "person_link_follow_me_resume_delay_seconds",
    "person_link_resume_room_mode",
)


def _person_link_test_state(client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        raw = _decode_hash(store.hgetall(PERSON_LINK_TEST_KEY) or {})
    except Exception:
        return {}
    if not raw:
        return {}
    try:
        values = json.loads(raw.get("values") or "{}")
    except Exception:
        values = {}
    return {
        "person_id": raw.get("person_id", ""),
        "status": raw.get("status", ""),
        "message": raw.get("message", ""),
        "values": values if isinstance(values, dict) else {},
    }


def _save_person_link_test_state(
    person_id: str,
    values: Dict[str, Any],
    status: str,
    message: str,
    client: Any = None,
) -> None:
    store = client or globals().get("redis_client")
    if store is None:
        return
    _save_hash(
        store,
        PERSON_LINK_TEST_KEY,
        {
            "person_id": person_id,
            "status": status,
            "message": message,
            "tested_at": time.time(),
            "values": json.dumps(values, sort_keys=True),
        },
    )


def _clear_person_link_test_state(person_id: Any, client: Any = None) -> None:
    store = client or globals().get("redis_client")
    wanted = _text(person_id)
    if store is None or not wanted:
        return
    if _person_link_test_state(store).get("person_id") != wanted:
        return
    try:
        store.delete(PERSON_LINK_TEST_KEY)
    except Exception:
        pass


def _linked_person_ids(client: Any = None) -> List[str]:
    return sorted(
        person_id
        for person_id, link in _person_links(client).items()
        if _provider_id(link.get("music_source"), "") in CATALOG_PROVIDER_IDS
    )


def _person_link_source(link: Dict[str, Any]) -> str:
    """Effective provider id for one person's link ("" = follow the global source)."""
    return _provider_id(link.get("music_source"), "")


def _person_tri_state(link: Dict[str, Any], key: str) -> str:
    """A person link toggle's "" / "on" / "off" state ("" = inherit global)."""
    value = _text(link.get(key)).casefold()
    return value if value in {"on", "off"} else ""


def _person_recommendations_enabled(
    person_id: Any,
    cfg: Dict[str, Any],
    client: Any = None,
) -> bool:
    """One Person's recommendation toggle; unlinked or "inherit" follows global."""
    state = _person_tri_state(_person_link(person_id, client), "recommendations_enabled")
    if state:
        return state == "on"
    return _as_bool(cfg.get("recommendations_enabled"), True)


def _person_prompt_context_enabled(
    person_id: Any,
    cfg: Dict[str, Any],
    client: Any = None,
) -> bool:
    """One Person's prompt-context toggle; unlinked or "inherit" follows global."""
    state = _person_tri_state(_person_link(person_id, client), "prompt_context_enabled")
    if state:
        return state == "on"
    return _as_bool(cfg.get("prompt_context_enabled"), True)


def _person_recommendation_int(
    person_id: Any,
    key: str,
    cfg: Dict[str, Any],
    *,
    default: int,
    minimum: int,
    maximum: int,
    client: Any = None,
) -> int:
    """One Person's numeric Personalization override; blank or invalid inherits global."""
    global_value = _as_int(cfg.get(key), default, minimum, maximum)
    return _as_int(_person_link(person_id, client).get(key), global_value, minimum, maximum)


def _person_select_choice(
    person_id: Any,
    key: str,
    cfg: Dict[str, Any],
    allowed: "tuple[str, ...] | list[str]",
    default: str,
    client: Any = None,
) -> str:
    """One Person's select override; blank or invalid inherits the global value."""
    global_value = _text(cfg.get(key)) if _text(cfg.get(key)) in allowed else default
    chosen = _text(_person_link(person_id, client).get(key))
    return chosen if chosen in allowed else global_value


def _person_endless_mode(person_id: Any, cfg: Dict[str, Any], client: Any = None) -> str:
    """One Person's Endless Playback mode; unlinked or blank follows the global setting."""
    return _person_select_choice(
        person_id,
        "endless_playback_mode",
        cfg,
        ENDLESS_PLAYBACK_MODES,
        DEFAULT_ENDLESS_PLAYBACK_MODE,
        client,
    )


def _person_endless_playlist(person_id: Any, cfg: Dict[str, Any], client: Any = None) -> str:
    """One Person's "Tracks from a playlist" pick; blank falls back to the global pick."""
    return _text(_person_link(person_id, client).get("endless_playback_playlist")) or _text(
        cfg.get("endless_playback_playlist")
    )


def _person_smart_shuffle_enabled(
    person_id: Any,
    cfg: Dict[str, Any],
    client: Any = None,
) -> bool:
    """One Person's Smart Shuffle toggle; unlinked or "inherit" follows global."""
    state = _person_tri_state(_person_link(person_id, client), "smart_shuffle_enabled")
    if state:
        return state == "on"
    return _as_bool(cfg.get("smart_shuffle_enabled"), False)


def _recommendations_possible(cfg: Dict[str, Any], client: Any = None) -> bool:
    """True when recommendation refresh should run for the household or any linked Person."""
    if _as_bool(cfg.get("recommendations_enabled"), True):
        return True
    return any(
        _person_recommendations_enabled(linked_id, cfg, client)
        for linked_id in _linked_person_ids(client)
    )


def _prompt_context_possible(cfg: Dict[str, Any], client: Any = None) -> bool:
    """True when prompt-profile refresh should run for the household or any linked Person."""
    if _as_bool(cfg.get("prompt_context_enabled"), True):
        return True
    return any(
        _person_prompt_context_enabled(linked_id, cfg, client)
        for linked_id in _linked_person_ids(client)
    )


def _scoped_key(base: str, person_id: Any) -> str:
    """Per-person data keys; "" keeps the shared global key."""
    wanted = _text(person_id)
    return f"{base}:{wanted}" if wanted else base


def _catalog_key(person_id: Any = "") -> str:
    return _scoped_key(CATALOG_KEY, person_id)


def _history_key(person_id: Any = "") -> str:
    return _scoped_key(HISTORY_KEY, person_id)


def _catalog_stats_field(person_id: Any = "") -> str:
    """Hash field one catalog's stats live under (person id, "" = household)."""
    return _text(person_id)


def _record_catalog_stats(store: Any, person_id: Any, stats: Dict[str, Any]) -> None:
    if store is None:
        return
    _save_hash(
        store,
        CATALOG_STATS_KEY,
        {
            _catalog_stats_field(person_id): json.dumps(
                stats, ensure_ascii=False, separators=(",", ":")
            )
        },
    )


def _catalog_stats(person_id: Any, store: Any = None) -> Dict[str, Any]:
    store = store or globals().get("redis_client")
    try:
        # Read the field straight from the hash: the household field is "",
        # which _decode_hash's empty-key filter would drop.
        raw = _text((store.hgetall(CATALOG_STATS_KEY) or {}).get(_catalog_stats_field(person_id), ""))
        value = json.loads(raw) if raw else {}
    except Exception:
        value = {}
    return value if isinstance(value, dict) else {}


def _recommendations_key(person_id: Any = "") -> str:
    return _scoped_key(RECOMMENDATIONS_KEY, person_id)


def _profile_key(person_id: Any = "") -> str:
    return _scoped_key(PROMPT_PROFILE_KEY, person_id)


def _person_link_extra_source(link: Dict[str, Any]) -> str:
    """A Person's second linked source id ("" = none). The same source type may
    appear twice (e.g. their own Emby account plus a household Emby user)."""
    return _provider_id(link.get("extra_source"), "")


def _person_link_sources(link: Dict[str, Any]) -> List[str]:
    """Every source id configured on one Person's link, in play order."""
    sources: List[str] = []
    primary = _person_link_source(link)
    if primary in CATALOG_PROVIDER_IDS:
        sources.append(primary)
    extra = _person_link_extra_source(link)
    if extra in CATALOG_PROVIDER_IDS:
        sources.append(extra)
    return sources


def _person_extra_slot(person_id: Any, source: Any) -> str:
    """Catalog/history slot id for one Person's second linked source."""
    return f"{_text(person_id)}+{_text(source)}"


def _build_person_provider(
    source: str,
    values: Dict[str, Any],
    stream_scope: str,
) -> Any:
    """Instantiate one catalog provider from a Person link's stored values."""
    spec = PROVIDER_FIELD_SPECS.get(_provider_id(source, ""))
    if spec is None:
        raise ValueError(
            f"{PROVIDER_LABELS.get(source, source)} support is not enabled in this build."
        )
    return spec.build_provider(values if isinstance(values, dict) else {}, stream_scope)


def _person_link_provider(
    person_id: Any,
    provider_id: Any,
    client: Any = None,
) -> Optional[Any]:
    """Build a provider from one Person's link settings, if they override the source.

    Answers the Person's primary source first, then their second linked source;
    a scoped person id ("<person_id>+<source>") addresses the second source's
    own credentials directly (the stream server uses that for per-Person Emby
    tokens).
    """
    link = _person_link(person_id, client)
    scope = _text(person_id)
    base_person, sep, scope_suffix = scope.partition("+")
    extra_source = _person_link_extra_source(link)
    requested = _provider_id(provider_id, "") if _text(provider_id) else ""

    # A scope like "<person>+emby" names the second source's slot explicitly.
    if (
        sep
        and extra_source
        and _provider_id(scope_suffix, "") == extra_source
        and _person_extra_slot(base_person, extra_source) == scope
    ):
        values = link.get("extra") if isinstance(link.get("extra"), dict) else {}
        return _build_person_provider(extra_source, values, scope)

    source = _person_link_source(link)
    if not source:
        return None
    values = link.get(source) if isinstance(link.get(source), dict) else {}
    if not requested or requested == source:
        return _build_person_provider(source, values, base_person or scope)
    if requested == extra_source:
        values = link.get("extra") if isinstance(link.get("extra"), dict) else {}
        return _build_person_provider(
            extra_source, values, _person_extra_slot(base_person, extra_source)
        )
    return None


def _person_catalog_source_ids(person_id: Any, client: Any = None) -> List[str]:
    """Every provider id one Person listens from (their link, or the global source)."""
    wanted = _text(person_id)
    if not wanted:
        return []
    sources = _person_link_sources(_person_link(wanted, client))
    if sources:
        return sources
    return [_person_source_id(wanted, client)]


def _provider_id(value: Any, default: str = "emby") -> str:
    token = _text(value).lower().replace("-", "_").replace(" ", "_")
    if token in {"emby"}:
        return "emby"
    if token in {"jellyfin"}:
        return "jellyfin"
    # Navidrome / Airsonic / Gonic all speak the Subsonic REST API; the stored
    # id is always the protocol name.
    if token in {"subsonic", "navidrome", "airsonic", "gonic"}:
        return "subsonic"
    if token in {"plex"}:
        return "plex"
    if token in {"network_share", "share", "network", "smb", "nfs"}:
        return "network_share"
    # An explicit empty default means "no fallback" for person-scoped lookups.
    allowed = {"", "emby", "jellyfin", "subsonic", "plex", "network_share"}
    return _text(default) if _text(default) in allowed else "emby"


def _provider_auth_cache_key(provider_id: Any, server_url: Any, identity: Any) -> str:
    """Per-account-identity auth cache key.

    One shared key per provider used to let a Person's sign-in overwrite the
    household's cached token (and, with an unset person user id, even sync the
    household's library under the Person's name). Keying on the server URL plus
    the account identity keeps every account's token separate.
    """
    digest = hashlib.sha1(
        f"{_text(server_url).strip()}\x00{_text(identity).strip()}".encode("utf-8")
    ).hexdigest()[:16]
    return f"personal_music_core:{_provider_id(provider_id, '')}:auth:{digest}"


def _decode_hash(raw: Any) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    return {_text(key): _text(value) for key, value in raw.items() if _text(key)}


def _settings(client: Any = None) -> Dict[str, str]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        return _decode_hash(store.hgetall(SETTINGS_KEY) or {})
    except Exception:
        return {}


def _external_audio_module() -> Any:
    try:
        import external_audio

        return external_audio
    except Exception:
        return None


def _airplay_receiver_targets(
    cfg: Dict[str, Any],
    player: Optional[Dict[str, Any]] = None,
) -> List[str]:
    configured = _normalize_stereo_targets(cfg.get("airplay_receiver_targets"))
    if not configured and isinstance(player, dict):
        configured = _normalize_stereo_targets(player.get("targets") or player.get("target"))
    return [target for target in configured if _is_external_audio_target(target)]


def _external_audio_config(
    cfg: Dict[str, Any],
    player: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    current_player = player if isinstance(player, dict) else _player()
    targets = _airplay_receiver_targets(cfg, current_player)
    # Incoming AirPlay owns the group-volume slider. Keep every selected
    # destination at unity here so sender 100% can reach the player's real
    # maximum and one saved Music Core volume does not attenuate it again.
    default_volume = 100
    settings = _selected_player_settings(
        targets,
        cfg,
        default_volume=default_volume,
    )
    return {
        "enabled": _as_bool(cfg.get("airplay_receiver_enabled"), False),
        "receiver_name": _text(cfg.get("airplay_receiver_name")) or "Tater Music",
        "receiver_pin": _text(cfg.get("airplay_receiver_pin")),
        "targets": targets,
        "volume_percent": default_volume,
        "target_volume_percent": {
            target: 100
            for target in settings
        },
        "target_sync_offset_ms": {
            target: _as_int(values.get("sync_offset_ms"), 0, -1000, 1000)
            for target, values in settings.items()
        },
        "target_transport_mode": {
            target: "airplay"
            for target in targets
            if _is_sonos_target(target)
        },
    }


def _configure_external_audio(
    cfg: Optional[Dict[str, Any]] = None,
    player: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    settings = cfg if isinstance(cfg, dict) else _settings()
    module = _external_audio_module()
    if module is None:
        return {
            "enabled": _as_bool(settings.get("airplay_receiver_enabled"), False),
            "status": "runtime_unavailable",
            "receiver_error": "Update Tater to a build that includes External Audio Input.",
            "targets": _airplay_receiver_targets(settings, player),
            "input_active": False,
        }
    try:
        result = module.configure_external_audio_runtime(
            _external_audio_config(settings, player)
        )
        return result if isinstance(result, dict) else {}
    except Exception as exc:
        return {
            "enabled": _as_bool(settings.get("airplay_receiver_enabled"), False),
            "status": "error",
            "receiver_error": _text(exc),
            "targets": _airplay_receiver_targets(settings, player),
            "input_active": False,
        }


def _external_audio_status(cfg: Dict[str, Any], player: Dict[str, Any]) -> Dict[str, Any]:
    module = _external_audio_module()
    if module is None:
        return {
            "enabled": _as_bool(cfg.get("airplay_receiver_enabled"), False),
            "status": "runtime_unavailable",
            "receiver_error": "Update Tater to a build that includes External Audio Input.",
            "targets": _airplay_receiver_targets(cfg, player),
            "input_active": False,
        }
    try:
        result = module.get_external_audio_status()
        return result if isinstance(result, dict) else {}
    except Exception as exc:
        return {
            "enabled": _as_bool(cfg.get("airplay_receiver_enabled"), False),
            "status": "error",
            "receiver_error": _text(exc),
            "targets": _airplay_receiver_targets(cfg, player),
            "input_active": False,
        }


def _target_group_signature(targets: Any) -> str:
    values = sorted({_text(value) for value in _list(targets) if _text(value)})
    if not values:
        return ""
    return hashlib.sha256("\n".join(values).encode("utf-8")).hexdigest()[:20]


def _mixed_sync_calibrations(cfg: Dict[str, Any]) -> Dict[str, int]:
    raw = cfg.get("mixed_sync_calibrations")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        return {}
    return {
        _text(key): _as_int(value, 0, -750, 3000)
        for key, value in raw.items()
        if _text(key)
    }


def _player_transport_mode(value: Any) -> str:
    mode = _text(value).casefold()
    return mode if mode in {"auto", "native", "airplay"} else "auto"


def _player_calibrations(cfg: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return per-destination volume and timeline calibration settings."""
    raw = cfg.get("player_calibrations")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    if not isinstance(raw, dict):
        return {}
    calibrations: Dict[str, Dict[str, Any]] = {}
    for raw_target, raw_values in raw.items():
        target = _text(raw_target)
        if not target or not isinstance(raw_values, dict):
            continue
        calibration: Dict[str, Any] = {
            "volume_percent": _as_int(raw_values.get("volume_percent"), 75, 0, 100),
            "sync_offset_ms": _as_int(raw_values.get("sync_offset_ms"), 0, -1000, 1000),
        }
        if target.casefold().startswith(("sonos:", "integration:sonos:")):
            calibration["transport_mode"] = _player_transport_mode(
                raw_values.get("transport_mode")
            )
        calibrations[target] = calibration
    return calibrations


def _target_calibration(
    target: Any,
    cfg: Dict[str, Any],
    *,
    default_volume: int = 75,
) -> Dict[str, Any]:
    target_id = _text(target)
    saved = _player_calibrations(cfg).get(target_id, {})
    calibration: Dict[str, Any] = {
        "volume_percent": _as_int(saved.get("volume_percent"), default_volume, 0, 100),
        "sync_offset_ms": _as_int(saved.get("sync_offset_ms"), 0, -1000, 1000),
    }
    if target_id.casefold().startswith(("sonos:", "integration:sonos:")):
        calibration["transport_mode"] = _player_transport_mode(saved.get("transport_mode"))
    return calibration


def _normalize_player_settings(
    raw: Any,
    *,
    targets: Any = None,
    cfg: Optional[Dict[str, Any]] = None,
    default_volume: int = 75,
) -> Dict[str, Dict[str, Any]]:
    source = raw
    if isinstance(source, str):
        try:
            source = json.loads(source)
        except Exception:
            source = {}
    source = source if isinstance(source, dict) else {}
    target_ids = _list(targets) if targets is not None else _list(list(source.keys()))
    settings: Dict[str, Dict[str, Any]] = {}
    current_cfg = cfg if isinstance(cfg, dict) else {}
    for target in target_ids:
        fallback = _target_calibration(target, current_cfg, default_volume=default_volume)
        values = source.get(target) if isinstance(source.get(target), dict) else {}
        setting: Dict[str, Any] = {
            "volume_percent": _as_int(
                values.get("volume_percent"),
                fallback["volume_percent"],
                0,
                100,
            ),
            "sync_offset_ms": _as_int(
                values.get("sync_offset_ms"),
                fallback["sync_offset_ms"],
                -1000,
                1000,
            ),
        }
        if target.casefold().startswith(("sonos:", "integration:sonos:")):
            setting["transport_mode"] = _player_transport_mode(
                values.get("transport_mode", fallback.get("transport_mode"))
            )
        settings[target] = setting
    return settings


def _save_player_calibrations(client: Any, raw: Any) -> Dict[str, Dict[str, Any]]:
    cfg = _settings(client)
    calibrations = _player_calibrations(cfg)
    updates = _normalize_player_settings(raw, cfg=cfg)
    calibrations.update(updates)
    _save_hash(
        client,
        SETTINGS_KEY,
        {"player_calibrations": json.dumps(calibrations, sort_keys=True)},
    )
    return calibrations


def _selected_player_settings(
    targets: Any,
    cfg: Dict[str, Any],
    *,
    default_volume: int = 75,
) -> Dict[str, Dict[str, Any]]:
    return {
        target: _target_calibration(target, cfg, default_volume=default_volume)
        for target in _list(targets)
    }


def _is_native_target(value: Any) -> bool:
    target = _text(value).casefold()
    return target.startswith(("voice_core:native:", "voice_core:stereo:", "native:", "stereo:"))


def _is_airplay_target(value: Any) -> bool:
    return _text(value).casefold().startswith("airplay:")


def _is_sonos_target(value: Any) -> bool:
    return _text(value).casefold().startswith("sonos:")


def _is_external_audio_target(value: Any) -> bool:
    return _is_native_target(value) or _is_airplay_target(value) or _is_sonos_target(value)


def _is_external_audio_option(row: Any) -> bool:
    option = row if isinstance(row, dict) else {}
    target = _text(option.get("value"))
    if not _is_external_audio_target(target):
        return False
    if _is_sonos_target(target):
        return _is_airplay_target(option.get("airplay_bridge_target"))
    return True


def _sonos_airplay_target(value: Any) -> str:
    if not _is_sonos_target(value):
        return ""
    try:
        from announcement_targets import resolve_sonos_airplay_target

        target = _text(resolve_sonos_airplay_target(value))
        return target if _is_airplay_target(target) else ""
    except Exception:
        return ""


def _uses_audio_sync_transcode(targets: Any) -> bool:
    """Use one normalized PCM source for every hardware Music Core target.

    ``screen:`` destinations (Jarvis Screen browser playback) decode the
    original containers natively, so they never force the WAV transcode —
    a screen-only target list plays the plain source URL.
    """
    return any(
        not _text(target).casefold().startswith(SCREEN_TARGET_PREFIX)
        for target in _list(targets)
    )


def _mixed_sync_from_player_settings(
    targets: Any,
    settings: Dict[str, Dict[str, Any]],
    fallback: int,
) -> int:
    native_offsets = [
        _as_int(settings.get(target, {}).get("sync_offset_ms"), 0, -1000, 1000)
        for target in _list(targets)
        if _is_native_target(target)
    ]
    external_offsets = [
        _as_int(settings.get(target, {}).get("sync_offset_ms"), 0, -1000, 1000)
        for target in _list(targets)
        if not _is_native_target(target)
    ]
    if not native_offsets or not external_offsets:
        return _as_int(fallback, 0, -750, 3000)
    native_average = round(sum(native_offsets) / len(native_offsets))
    external_average = round(sum(external_offsets) / len(external_offsets))
    return _as_int(
        _as_int(fallback, 0, -750, 3000) + native_average - external_average,
        fallback,
        -750,
        3000,
    )


def _sync_test_wav(*, duration_seconds: float = 6.0) -> bytes:
    sample_rate = 16000
    frame_count = int(sample_rate * max(2.0, min(10.0, duration_seconds)))
    click_frames = int(sample_rate * 0.025)
    interval_frames = int(sample_rate * 0.5)
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(2)
        wav_file.setframerate(sample_rate)
        frames = bytearray()
        for frame_index in range(frame_count):
            click_index = frame_index % interval_frames
            if click_index < click_frames:
                envelope = math.exp(-8.0 * click_index / max(1, click_frames))
                sample = int(26000 * envelope * math.sin(2.0 * math.pi * 1400.0 * click_index / sample_rate))
            else:
                sample = 0
            frames.extend(struct.pack("<h", sample))
        wav_file.writeframes(bytes(frames))
    return output.getvalue()


def _mixed_sync_adjustment(targets: Any, cfg: Dict[str, Any]) -> int:
    default = _as_int(cfg.get("mixed_sync_default_adjustment_ms"), 0, -750, 3000)
    signature = _target_group_signature(targets)
    return _mixed_sync_calibrations(cfg).get(signature, default) if signature else default


def _save_mixed_sync_adjustment(client: Any, targets: Any, value: Any) -> int:
    cfg = _settings(client)
    adjustment = _as_int(value, _mixed_sync_adjustment(targets, cfg), -750, 3000)
    signature = _target_group_signature(targets)
    if not signature:
        return adjustment
    calibrations = _mixed_sync_calibrations(cfg)
    calibrations[signature] = adjustment
    _save_hash(client, SETTINGS_KEY, {"mixed_sync_calibrations": json.dumps(calibrations, sort_keys=True)})
    return adjustment


def _runtime(client: Any = None) -> Dict[str, str]:
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        return _decode_hash(store.hgetall(RUNTIME_KEY) or {})
    except Exception:
        return {}


def _save_hash(client: Any, key: str, values: Dict[str, Any]) -> None:
    if client is None:
        return
    cleaned = {str(key): str(value) for key, value in values.items() if value is not None}
    if cleaned:
        client.hset(key, mapping=cleaned)


def _load_json(client: Any, key: str, default: Any) -> Any:
    if client is None:
        return default
    try:
        raw = client.get(key)
        if raw in (None, ""):
            return default
        return json.loads(_text(raw))
    except Exception:
        return default


def _save_json(client: Any, key: str, value: Any) -> None:
    if client is not None:
        client.set(key, json.dumps(value, ensure_ascii=False, separators=(",", ":")))


def _normalize_server_url(value: Any) -> str:
    raw = _text(value).rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Server URL must begin with http:// or https://")
    return raw.rstrip("/")


def _normalize_cached_artwork(track: Dict[str, Any]) -> None:
    """Recompute provider artwork flags on a catalog track loaded from Redis."""
    track["provider"] = _provider_id(track.get("provider"))
    if (
        _text(track.get("artwork_path"))
        or _text(track.get("artwork_item_id"))
        or _text(track.get("album_id"))
    ):
        track["has_artwork"] = True
    else:
        track["has_artwork"] = False
        track["artwork_version"] = ""


def _unwrap_response(response: Any) -> Any:
    try:
        body = response.json()
    except Exception:
        body = {}
    if not bool(getattr(response, "ok", False)):
        error = body.get("error") if isinstance(body, dict) else {}
        message = error.get("message") if isinstance(error, dict) else ""
        detail = _text(message) or f"Music provider returned HTTP {getattr(response, 'status_code', 0)}."
        if int(getattr(response, "status_code", 0) or 0) == 401:
            raise PermissionError(detail)
        raise RuntimeError(detail)
    if isinstance(body, dict) and body.get("success") is False:
        error = body.get("error")
        message = error.get("message") if isinstance(error, dict) else error
        raise RuntimeError(_text(message) or "The music provider rejected the request.")
    if isinstance(body, dict) and "data" in body:
        return body.get("data")
    return body


@dataclass
class EmbyMusicProvider:
    """One Emby server music view, addressed with either a server API key or a per-user token.

    Server API keys stream directly from Emby (``?api_key=`` query auth travels in the
    URL a playback target fetches). Per-user access tokens can only travel in headers,
    so those streams and artwork requests are proxied through this core's own stream
    server, which attaches the bearer header server-side.
    """

    server_url: str
    auth_mode: str = "user_token"
    username: str = ""
    password: str = ""
    api_key: str = ""
    user_id: str = ""
    library_name: str = ""
    # Subfolder of the library to sync (name like "Music", or an absolute
    # path). Lets one mixed-content library (music + TV + movies) serve as a
    # Person's source while only its music folder is indexed.
    library_folder: str = ""
    # Person id when this provider instance serves a linked Person's own Emby
    # account; proxied stream routes carry the scope so the proxy attaches the
    # right person's credentials server-side.
    stream_scope: str = ""
    provider_id = "emby"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "EmbyMusicProvider":
        return cls(
            server_url=_normalize_server_url(
                settings.get("emby_server_url") or settings.get("server_url")
            ),
            auth_mode="api_key"
            if _text(settings.get("emby_auth_mode")).casefold() == "api_key"
            else "user_token",
            username=_text(settings.get("emby_username")),
            password=_text(settings.get("emby_password")),
            api_key=_text(settings.get("emby_api_key")),
            user_id=_text(settings.get("emby_user_id")),
            library_name=_text(settings.get("emby_library_name")),
            library_folder=_text(settings.get("emby_library_folder")),
        )

    @property
    def connected(self) -> bool:
        if not self.server_url:
            return False
        if self.auth_mode == "api_key":
            return bool(self.api_key)
        return bool(self.username and self.password)

    @property
    def device_id(self) -> str:
        digest = hashlib.sha256(
            f"personal_music_core\x00{self.server_url}\x00{self.username}".encode("utf-8")
        ).hexdigest()[:32]
        return f"personal-music-{digest[:16]}"

    def _emby_authorization_header(self, token: str = "") -> str:
        parts = [
            'MediaBrowser Client="Personal Music Core"',
            'Device="Tater"',
            f'DeviceId="{self.device_id}"',
            f'Version="{__version__}"',
        ]
        if token:
            parts.append(f'Token="{token}"')
        return ", ".join(parts)

    def _auth_cache_key(self) -> str:
        return _provider_auth_cache_key(self.provider_id, self.server_url, self.username)

    def _cached_auth(self, client: Any = None) -> Dict[str, str]:
        store = client or globals().get("redis_client")
        if store is None:
            return {}
        try:
            return _decode_hash(store.hgetall(self._auth_cache_key()) or {})
        except Exception:
            return {}

    def _save_cached_auth(self, token: str, user_id: str, client: Any = None) -> None:
        store = client or globals().get("redis_client")
        if store is None:
            return
        _save_hash(
            store,
            self._auth_cache_key(),
            {
                "access_token": token,
                "user_id": user_id,
                "authenticated_at": time.time(),
            },
        )

    def clear_cached_auth(self, client: Any = None) -> None:
        store = client or globals().get("redis_client")
        if store is not None:
            try:
                store.delete(self._auth_cache_key())
            except Exception:
                pass

    def authenticate(
        self,
        *,
        force: bool = False,
        client: Any = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> tuple[str, str]:
        """Return (access_token, user_id), authenticating against Emby when needed."""
        cached = {} if force else self._cached_auth(client)
        token = _text(cached.get("access_token"))
        user_id = _text(cached.get("user_id")) or self.user_id
        if token and (not self.user_id or user_id == self.user_id):
            return token, user_id
        if not self.username or not self.password:
            raise ValueError("Emby username and password are required for user sign-in.")
        response = requests.post(
            f"{self.server_url}/Users/AuthenticateByName",
            headers={
                "Content-Type": "application/json",
                "X-Emby-Authorization": self._emby_authorization_header(),
                "Accept": "application/json",
            },
            json={"Username": self.username, "Pw": self.password},
            timeout=max(5, int(timeout)),
        )
        if response.status_code in (401, 403):
            raise PermissionError("Emby rejected the username or password.")
        if not response.ok:
            raise RuntimeError(f"Emby sign-in failed with HTTP {response.status_code}.")
        body = response.json() if response.content else {}
        token = _text(body.get("AccessToken"))
        user = body.get("User") if isinstance(body.get("User"), dict) else {}
        user_id = _text(user.get("Id")) or _text(body.get("UserId"))
        if not token or not user_id:
            raise RuntimeError("Emby sign-in did not return an access token.")
        self._save_cached_auth(token, user_id, client)
        return token, user_id

    def _access_token(self, client: Any = None, *, force_refresh: bool = False) -> str:
        token, _user_id = self.authenticate(force=force_refresh, client=client)
        return token

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        authenticated: bool = True,
        client: Any = None,
    ) -> Any:
        if not self.server_url:
            raise ValueError("Emby server URL is not configured.")
        query: Dict[str, Any] = dict(params or {})
        headers = {"Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if authenticated:
            if self.auth_mode == "api_key":
                query.setdefault("api_key", self.api_key)
            else:
                headers["X-Emby-Token"] = self._access_token(client=client)
        url = f"{self.server_url}/{path.lstrip('/')}"
        response = requests.request(
            method.upper(),
            url,
            params=query,
            headers=headers,
            json=payload,
            timeout=max(5, int(timeout)),
        )
        if response.status_code == 401:
            self.clear_cached_auth(client)
            raise PermissionError("Emby rejected this core's credentials.")
        if not response.ok:
            raise RuntimeError(f"Emby returned HTTP {response.status_code} for {path}.")
        try:
            return response.json()
        except Exception:
            return {}

    def resolve_user_id(self, client: Any = None) -> str:
        """Best-effort Emby user id for this configuration."""
        if self.user_id:
            return self.user_id
        if self.auth_mode == "user_token":
            _token, user_id = self.authenticate(client=client)
            return user_id
        users = self.request("GET", "Users", client=client) or []
        rows = users.get("Items") if isinstance(users, dict) else users
        if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict):
            return _text(rows[0].get("Id"))
        if self.username and isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and _text(row.get("Name")).casefold() == self.username.casefold():
                    return _text(row.get("Id"))
        raise ValueError(
            "Set the Emby User ID in settings, or use one Emby user account with username sign-in."
        )

    def music_view(self, client: Any = None) -> Dict[str, Any]:
        user_id = self.resolve_user_id(client)
        views = self.request("GET", f"Users/{quote(str(user_id), safe='')}/Views", client=client) or {}
        rows = views.get("Items") if isinstance(views, dict) else views
        candidates = [row for row in (rows or []) if isinstance(row, dict)]
        wanted = self.library_name.casefold()
        for row in candidates:
            if wanted and _text(row.get("Name")).casefold() == wanted:
                return row
        for row in candidates:
            if _text(row.get("CollectionType")).casefold() == "music":
                return row
        if wanted:
            raise ValueError(f"No Emby music library named '{self.library_name}' is visible to this Emby user.")
        if candidates:
            raise ValueError(
                "No Emby library with a music collection type is visible to this Emby user. "
                "Set a Library Name in the Emby source settings."
            )
        raise ValueError("This Emby user cannot see any libraries.")

    def music_folder(
        self, view: Dict[str, Any], client: Any = None
    ) -> Dict[str, Any]:
        """Resolve the configured subfolder inside `view` (the view itself when unset).

        Accepts a folder name ("Music") or an absolute server path
        ("/mnt/media/zoe/Music"); Emby reports each item's filesystem path, so
        both resolve. Raises when nothing matches so settings mistakes surface
        at test/save time instead of syncing an empty catalog.
        """
        wanted = _text(self.library_folder)
        if not wanted:
            return view
        user_id = self.resolve_user_id(client)
        page = self.request(
            "GET",
            f"Users/{quote(str(user_id), safe='')}/Items",
            params={
                "ParentId": _text(view.get("Id")),
                "Recursive": "true",
                "IncludeItemTypes": "Folder",
                "Fields": "Path",
                "Limit": 1000,
            },
            client=client,
        ) or {}
        rows = page.get("Items") if isinstance(page, dict) else page
        candidates = [row for row in (rows or []) if isinstance(row, dict)]
        wanted_path = wanted.rstrip("/").casefold()
        for row in candidates:
            if wanted.startswith("/") and _text(row.get("Path")).rstrip("/").casefold() == wanted_path:
                return row
            if _text(row.get("Name")).casefold() == wanted.casefold():
                return row
        suffix = "/" + wanted.strip("/")
        if not wanted.startswith("/"):
            for row in candidates:
                if _text(row.get("Path")).rstrip("/").casefold().endswith(suffix.casefold()):
                    return row
        raise ValueError(
            f"No folder matching '{wanted}' exists inside the "
            f"{_text(view.get('Name')) or 'music'} library visible to this Emby user."
        )

    def stream_url(self, track: Dict[str, Any], *, audio_sync: bool = False) -> str:
        item_id = _text(track.get("provider_track_id")) or _text(track.get("id"))
        if not item_id:
            return _text(track.get("stream_url"))
        if self.auth_mode == "api_key":
            values: Dict[str, Any] = {"api_key": self.api_key}
            if audio_sync:
                # Mixed Tater satellite + Sonos/AirPlay groups share one normalized
                # PCM source so every target can stay clock-aligned.
                values.update(
                    {
                        "AudioCodec": "wav",
                        "AudioSampleRate": 44100,
                        "AudioChannels": 2,
                    }
                )
            else:
                values["Static"] = "true"
            query = urlencode(values)
            return f"{self.server_url}/Audio/{quote(str(item_id), safe='')}/stream?{query}"
        kind = f"emby:{self.stream_scope}" if self.stream_scope else "emby"
        return _stream_proxy_url(kind, item_id, sync=audio_sync)

    def artwork_url(self, track: Dict[str, Any]) -> str:
        item_id = _text(track.get("artwork_item_id")) or _text(track.get("provider_track_id")) or _text(track.get("id"))
        if not item_id:
            return ""
        if not _text(track.get("artwork_version")):
            # The song item carries no Primary image (cover art usually lives
            # on the album item in Emby) — request the album's art instead.
            album_id = _text(track.get("album_id"))
            if album_id:
                item_id = album_id
        if self.auth_mode == "api_key":
            values = {
                "api_key": self.api_key,
                "MaxWidth": EMBY_ARTWORK_MAX_WIDTH,
                "Quality": 90,
            }
            tag = _text(track.get("artwork_version"))
            if tag:
                values["tag"] = tag
            query = urlencode(values)
            return f"{self.server_url}/Items/{quote(str(item_id), safe='')}/Images/Primary?{query}"
        kind = f"emby_art:{self.stream_scope}" if self.stream_scope else "emby_art"
        return _stream_proxy_url(kind, item_id)

    def proxy_request(
        self,
        item_id: str,
        *,
        art: bool = False,
        sync: bool = False,
    ) -> "tuple[str, Dict[str, str]]":
        """Build (url, headers) for one proxied Emby stream or artwork request.

        The core stream server calls this server-side, so per-user tokens and
        API keys never reach a playback target inside a stream URL.
        """
        item_id = _text(item_id)
        if art:
            params: Dict[str, Any] = {"MaxWidth": EMBY_ARTWORK_MAX_WIDTH, "Quality": 90}
            path = f"Items/{quote(item_id, safe='')}/Images/Primary"
        else:
            params = {}
            if sync:
                # Mixed Tater satellite + Sonos/AirPlay groups share one
                # normalized PCM source so every target can stay clock-aligned.
                params.update({"AudioCodec": "wav", "AudioSampleRate": 44100, "AudioChannels": 2})
            else:
                params["Static"] = "true"
            path = f"Audio/{quote(item_id, safe='')}/stream"
        headers = {"Accept": "*/*"}
        if self.auth_mode == "api_key":
            params["api_key"] = self.api_key
        else:
            headers["X-Emby-Token"] = self._access_token()
        return f"{self.server_url}/{path}?{urlencode(params)}", headers

    def catalog(self) -> Dict[str, Any]:
        view = self.music_view()
        view_id = _text(view.get("Id"))
        view_name = _text(view.get("Name")) or "Music"
        folder = self.music_folder(view)
        folder_id = _text(folder.get("Id")) or view_id
        # When no subfolder is configured the resolved folder IS the view; only
        # label the scope when a real subfolder was matched.
        folder_name = (
            _text(folder.get("Name"))
            if folder_id != view_id and _text(folder.get("Name"))
            else ""
        )
        user_id = self.resolve_user_id()
        tracks: List[Dict[str, Any]] = []
        start_index = 0
        while len(tracks) < MAX_CATALOG_TRACKS:
            page = self.request(
                "GET",
                f"Users/{quote(str(user_id), safe='')}/Items",
                params={
                    "ParentId": folder_id,
                    "Recursive": "true",
                    "IncludeItemTypes": "Song",
                    "Fields": "Genres,MediaSources,Path",
                    "SortBy": "Album,SortName",
                    "SortOrder": "Ascending",
                    "StartIndex": start_index,
                    "Limit": EMBY_PAGE_SIZE,
                },
                timeout=180,
            ) or {}
            rows = page.get("Items") if isinstance(page, dict) else page
            if not isinstance(rows, list):
                break
            for row in rows:
                if isinstance(row, dict):
                    tracks.append(row)
            total = _as_int(page.get("TotalRecordCount") if isinstance(page, dict) else 0, 0, 0, 10**9)
            start_index += len(rows)
            if not rows or (total and start_index >= total) or start_index >= MAX_CATALOG_TRACKS:
                break
        try:
            user_playlists = self.user_playlists()
        except Exception as exc:
            # Playlists are a bonus on top of the song sync — a failure listing
            # them (permissions, a playlist item that errors) must not break it.
            logger.warning("[Music] Emby playlist listing failed: %s", exc)
            user_playlists = []
        return {
            "catalog_id": folder_id,
            "tracks": tracks[:MAX_CATALOG_TRACKS],
            "total": len(tracks),
            "playlists": user_playlists,
            "libraries": {
                folder_id: f"{view_name} · {folder_name}" if folder_name else view_name,
            },
        }

    def user_playlists(self) -> List[Dict[str, Any]]:
        """The signed-in Emby user's own playlists, with their song ids.

        Used by the "Tracks from a playlist" Endless Playback mode and by voice
        "play playlist" requests when no AI-named mix matches the name. Track ids
        are Emby item ids, resolved against the synced catalog like mix tracks.
        """
        user_id = self.resolve_user_id()
        playlists: List[Dict[str, Any]] = []
        start_index = 0
        while start_index < 2000 and len(playlists) < 200:
            page = self.request(
                "GET",
                f"Users/{quote(str(user_id), safe='')}/Items",
                params={
                    "IncludeItemTypes": "Playlist",
                    "Recursive": "true",
                    "SortBy": "SortName",
                    "SortOrder": "Ascending",
                    "StartIndex": start_index,
                    "Limit": EMBY_PAGE_SIZE,
                },
                timeout=60,
            ) or {}
            rows = page.get("Items") if isinstance(page, dict) else page
            if not isinstance(rows, list):
                break
            for row in rows:
                if not isinstance(row, dict):
                    continue
                playlist_id = _text(row.get("Id"))
                name = _text(row.get("Name"))
                if not playlist_id or not name:
                    continue
                playlists.append(
                    {
                        "id": f"emby_playlist:{playlist_id}",
                        "name": name,
                        "description": "",
                        "track_ids": self._playlist_track_ids(user_id, playlist_id),
                    }
                )
                if len(playlists) >= 200:
                    break
            total = _as_int(
                page.get("TotalRecordCount") if isinstance(page, dict) else 0, 0, 0, 10**9
            )
            start_index += len(rows)
            if not rows or (total and start_index >= total):
                break
        return playlists

    def _playlist_track_ids(self, user_id: str, playlist_id: str) -> List[str]:
        track_ids: List[str] = []
        start_index = 0
        while start_index < MAX_CATALOG_TRACKS:
            page = self.request(
                "GET",
                f"Users/{quote(str(user_id), safe='')}/Items",
                params={
                    "ParentId": playlist_id,
                    "IncludeItemTypes": "Audio",
                    "Recursive": "true",
                    "SortBy": "ParentIndexNumber,IndexNumber,SortName",
                    "SortOrder": "Ascending",
                    "StartIndex": start_index,
                    "Limit": EMBY_PAGE_SIZE,
                },
                timeout=60,
            ) or {}
            rows = page.get("Items") if isinstance(page, dict) else page
            if not isinstance(rows, list):
                break
            for row in rows:
                if isinstance(row, dict) and _text(row.get("Id")):
                    track_ids.append(_text(row.get("Id")))
            total = _as_int(
                page.get("TotalRecordCount") if isinstance(page, dict) else 0, 0, 0, 10**9
            )
            start_index += len(rows)
            if not rows or (total and start_index >= total):
                break
        return track_ids

# --------------------------------------------------------------------------
@dataclass
class JellyfinMusicProvider(EmbyMusicProvider):
    """One Jellyfin server music view.

    Jellyfin forked Emby and kept the music surface path-identical, so this is a
    near-straight subclass: same ``/Users/AuthenticateByName`` flow, same
    ``Users/{uid}/Views`` + ``Users/{uid}/Items`` paging, same
    ``Audio/{id}/stream`` and ``Items/{id}/Images/Primary`` routes. Differences:
    Jellyfin 10.9+ wants the modern ``Authorization:`` MediaBrowser header (it
    still accepts the legacy ``X-Emby-*`` forms, which this core sends too for
    older servers), and it carries its own settings keys and auth cache.
    """

    provider_id = "jellyfin"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "JellyfinMusicProvider":
        return cls(
            server_url=_normalize_server_url(settings.get("jellyfin_server_url")),
            auth_mode="api_key"
            if _text(settings.get("jellyfin_auth_mode")).casefold() == "api_key"
            else "user_token",
            username=_text(settings.get("jellyfin_username")),
            password=_text(settings.get("jellyfin_password")),
            api_key=_text(settings.get("jellyfin_api_key")),
            user_id=_text(settings.get("jellyfin_user_id")),
            library_name=_text(settings.get("jellyfin_library_name")),
            library_folder=_text(settings.get("jellyfin_library_folder")),
        )

    def authenticate(
        self,
        *,
        force: bool = False,
        client: Any = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> "tuple[str, str]":
        """Return (access_token, user_id), authenticating against Jellyfin when needed."""
        cached = {} if force else self._cached_auth(client)
        token = _text(cached.get("access_token"))
        user_id = _text(cached.get("user_id")) or self.user_id
        if token and (not self.user_id or user_id == self.user_id):
            return token, user_id
        if not self.username or not self.password:
            raise ValueError("Jellyfin username and password are required for user sign-in.")
        header = self._emby_authorization_header()
        response = requests.post(
            f"{self.server_url}/Users/AuthenticateByName",
            headers={
                "Content-Type": "application/json",
                # Modern servers want `Authorization:`, older ones the legacy
                # X-Emby form; Jellyfin accepts either, so send both.
                "Authorization": header,
                "X-Emby-Authorization": header,
                "Accept": "application/json",
            },
            json={"Username": self.username, "Pw": self.password},
            timeout=max(5, int(timeout)),
        )
        if response.status_code in (401, 403):
            raise PermissionError("Jellyfin rejected the username or password.")
        if not response.ok:
            raise RuntimeError(f"Jellyfin sign-in failed with HTTP {response.status_code}.")
        body = response.json() if response.content else {}
        token = _text(body.get("AccessToken"))
        user = body.get("User") if isinstance(body.get("User"), dict) else {}
        user_id = _text(user.get("Id")) or _text(body.get("UserId"))
        if not token or not user_id:
            raise RuntimeError("Jellyfin sign-in did not return an access token.")
        self._save_cached_auth(token, user_id, client)
        return token, user_id

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
        authenticated: bool = True,
        client: Any = None,
    ) -> Any:
        if not self.server_url:
            raise ValueError("Jellyfin server URL is not configured.")
        query: Dict[str, Any] = dict(params or {})
        headers = {"Accept": "application/json"}
        if payload is not None:
            headers["Content-Type"] = "application/json"
        if authenticated:
            if self.auth_mode == "api_key":
                query.setdefault("api_key", self.api_key)
            else:
                token = self._access_token(client=client)
                # X-Emby-Token keeps older Jellyfin releases working; modern
                # releases also accept the Authorization header forms.
                headers["X-Emby-Token"] = token
                auth_header = self._emby_authorization_header(token)
                headers["Authorization"] = auth_header
                headers["X-Emby-Authorization"] = auth_header
        url = f"{self.server_url}/{path.lstrip('/')}"
        response = requests.request(
            method.upper(),
            url,
            params=query,
            headers=headers,
            json=payload,
            timeout=max(5, int(timeout)),
        )
        if response.status_code == 401:
            self.clear_cached_auth(client)
            raise PermissionError("Jellyfin rejected this core's credentials.")
        if not response.ok:
            raise RuntimeError(f"Jellyfin returned HTTP {response.status_code} for {path}.")
        try:
            return response.json()
        except Exception:
            return {}

    def stream_url(self, track: Dict[str, Any], *, audio_sync: bool = False) -> str:
        # The api_key direct-stream URL is identical to Emby's; only the proxied
        # proxy kinds differ (jellyfin / jellyfin_art instead of emby / emby_art).
        if self.auth_mode != "api_key":
            item_id = _text(track.get("provider_track_id")) or _text(track.get("id"))
            if item_id:
                kind = f"jellyfin:{self.stream_scope}" if self.stream_scope else "jellyfin"
                return _stream_proxy_url(kind, item_id, sync=audio_sync)
        return super().stream_url(track, audio_sync=audio_sync)

    def artwork_url(self, track: Dict[str, Any]) -> str:
        item_id = _text(track.get("artwork_item_id")) or _text(track.get("provider_track_id")) or _text(track.get("id"))
        if item_id and self.auth_mode != "api_key":
            # Cover art usually hangs off the album item; prefer it when the
            # song carries no Primary image (mirrors the Emby fallback).
            if not _text(track.get("artwork_version")):
                album_id = _text(track.get("album_id"))
                if album_id:
                    item_id = album_id
            kind = f"jellyfin_art:{self.stream_scope}" if self.stream_scope else "jellyfin_art"
            return _stream_proxy_url(kind, item_id)
        return super().artwork_url(track)

    def proxy_request(
        self,
        item_id: str,
        *,
        art: bool = False,
        sync: bool = False,
    ) -> "tuple[str, Dict[str, str]]":
        """Build (url, headers) for one proxied Jellyfin stream or artwork request."""
        url, headers = super().proxy_request(item_id, art=art, sync=sync)
        if self.auth_mode != "api_key":
            token = self._access_token()
            auth_header = self._emby_authorization_header(token)
            headers["Authorization"] = auth_header
            headers["X-Emby-Authorization"] = auth_header
        return url, headers

    def resolve_user_id(self, client: Any = None) -> str:
        """Best-effort Jellyfin user id for this configuration."""
        if self.user_id:
            return self.user_id
        if self.auth_mode == "user_token":
            _token, user_id = self.authenticate(client=client)
            return user_id
        users = self.request("GET", "Users", client=client) or []
        rows = users.get("Items") if isinstance(users, dict) else users
        if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict):
            return _text(rows[0].get("Id"))
        if self.username and isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict) and _text(row.get("Name")).casefold() == self.username.casefold():
                    return _text(row.get("Id"))
        raise ValueError(
            "Set the Jellyfin User ID in settings, or use one Jellyfin user account with username sign-in."
        )

    def music_view(self, client: Any = None) -> Dict[str, Any]:
        try:
            return super().music_view(client)
        except ValueError as exc:
            raise ValueError(_text(exc).replace("Emby", "Jellyfin")) from exc

    def music_folder(self, view: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
        try:
            return super().music_folder(view, client)
        except ValueError as exc:
            raise ValueError(_text(exc).replace("Emby", "Jellyfin")) from exc


# --------------------------------------------------------------------------
# Network Share tag reading (stdlib only — the Tater image has no mutagen).
# --------------------------------------------------------------------------

_ID3_V22_TEXT_FRAMES = {
    "TT2": "title",
    "TP1": "artist",
    "TAL": "album",
    "TP2": "album_artist",
    "TCO": "genre",
    "TRK": "track",
    "TPA": "disc",
    "TYE": "date",
}
_ID3_V3_TEXT_FRAMES = {
    "TIT2": "title",
    "TPE1": "artist",
    "TALB": "album",
    "TPE2": "album_artist",
    "TCON": "genre",
    "TRCK": "track",
    "TPOS": "disc",
    "TDRC": "date",
    "TYER": "date",
    "TDAT": "date",
}


def _share_id3_text(data: bytes) -> str:
    """Decode one ID3v2 text frame body (first value only)."""
    if not data:
        return ""
    encoding, body = data[0], data[1:]
    try:
        if encoding == 1:
            text = body.decode("utf-16", "replace")
        elif encoding == 2:
            text = body.decode("utf-16-be", "replace")
        elif encoding == 3:
            text = body.decode("utf-8", "replace").lstrip("﻿")
        else:
            text = body.decode("latin-1", "replace")
    except Exception:
        return ""
    return text.split("\x00")[0].strip()


def _share_id3_syncsafe(raw: bytes) -> int:
    return (
        ((raw[0] & 0x7F) << 21)
        | ((raw[1] & 0x7F) << 14)
        | ((raw[2] & 0x7F) << 7)
        | (raw[3] & 0x7F)
    )


def _share_image_kind(mime: str, data: bytes) -> str:
    mime = mime.casefold()
    if "png" in mime or data.startswith(b"\x89PNG"):
        return "png"
    return "jpg"


def _share_read_id3(path: str) -> Dict[str, Any]:
    tags: Dict[str, Any] = {}
    try:
        with open(path, "rb") as handle:
            header = handle.read(10)
            if len(header) < 10 or not header.startswith(b"ID3"):
                return tags
            major = header[3]
            flags = header[5]
            blob = handle.read(_share_id3_syncsafe(header[6:10]))
    except OSError:
        return tags
    if not blob or major not in (2, 3, 4):
        return tags
    if flags & 0x80 and major < 4:  # whole-tag unsynchronisation
        blob = blob.replace(b"\xff\x00", b"\xff")
    pos = 0
    if flags & 0x40 and len(blob) >= 4:  # extended header
        if major == 4:
            pos += max(4, _share_id3_syncsafe(blob[0:4]))
        else:
            pos += struct.unpack(">I", blob[0:4])[0] + 4
    id_len = 3 if major == 2 else 4
    size_len = 3 if major == 2 else 4
    flags_len = 0 if major == 2 else 2
    head_len = id_len + size_len + flags_len
    picture: Dict[str, Any] = {}
    while pos + head_len <= len(blob):
        frame_id = blob[pos : pos + id_len].decode("latin-1", "replace")
        raw_size = blob[pos + id_len : pos + id_len + size_len]
        frame_flags = blob[pos + id_len + size_len : pos + head_len]
        if major == 2:
            size = int.from_bytes(raw_size, "big")
        elif major == 4:
            size = _share_id3_syncsafe(raw_size)
        else:
            size = struct.unpack(">I", raw_size)[0]
        pos += head_len
        if not frame_id.isalnum() or size <= 0 or pos + size > len(blob):
            break  # padding or corrupt tail
        data = blob[pos : pos + size]
        pos += size
        if major == 4 and len(frame_flags) == 2:
            if frame_flags[1] & 0x02:  # per-frame unsynchronisation
                data = data.replace(b"\xff\x00", b"\xff")
            if frame_flags[1] & 0x01 and len(data) >= 4:  # data length indicator
                data = data[4:]
        field = (_ID3_V22_TEXT_FRAMES if major == 2 else _ID3_V3_TEXT_FRAMES).get(frame_id)
        if field and field != "date":
            value = _share_id3_text(data)
            if value and not tags.get(field):
                tags[field] = value
        elif field == "date":
            value = _share_id3_text(data)
            if value and not tags.get("year"):
                tags["year"] = value[:4]
        elif (major == 2 and frame_id == "PIC") or frame_id == "APIC":
            parsed = _share_id3_apic(data, legacy=major == 2)
            if parsed and not picture:
                picture = parsed
    if picture and not tags.get("picture"):
        tags["picture"] = picture
    return tags


def _share_id3_apic(data: bytes, *, legacy: bool = False) -> Dict[str, Any]:
    """Parse one APIC/PIC frame into {"mime", "data"}."""
    if not data:
        return {}
    encoding = data[0]
    pos = 1
    if legacy:
        mime = data[pos : pos + 3].decode("latin-1", "replace").strip().casefold()
        pos += 3
    else:
        end = data.find(b"\x00", pos)
        if end < 0:
            return {}
        mime = data[pos:end].decode("latin-1", "replace").casefold()
        pos = end + 1
    pos += 1  # picture type byte
    # Description terminates with the text encoding's terminator.
    if encoding in (1, 2):
        terminator = b"\x00\x00"
    else:
        terminator = b"\x00"
    end = data.find(terminator, pos)
    if end < 0:
        return {}
    pos = end + len(terminator)
    image = data[pos:]
    if len(image) < 8:
        return {}
    return {"mime": _share_image_kind(mime, image), "data": image}


def _share_parse_vorbis_comments(data: bytes) -> Dict[str, List[str]]:
    pos = 0
    if len(data) < 8:
        return {}
    vendor_len = struct.unpack("<I", data[pos : pos + 4])[0]
    pos += 4 + vendor_len
    if pos + 4 > len(data):
        return {}
    count = struct.unpack("<I", data[pos : pos + 4])[0]
    pos += 4
    comments: Dict[str, List[str]] = {}
    for _ in range(min(count, 512)):
        if pos + 4 > len(data):
            break
        length = struct.unpack("<I", data[pos : pos + 4])[0]
        pos += 4
        raw = data[pos : pos + length]
        pos += length
        if b"=" not in raw:
            continue
        key, value = raw.split(b"=", 1)
        name = key.decode("ascii", "replace").strip().upper()
        text = value.decode("utf-8", "replace").strip()
        if name and text:
            comments.setdefault(name, []).append(text)
    return comments


def _share_vorbis_tags(comments: Dict[str, List[str]]) -> Dict[str, Any]:
    tags: Dict[str, Any] = {}
    field_map = {
        "TITLE": "title",
        "ARTIST": "artist",
        "ALBUM": "album",
        "ALBUMARTIST": "album_artist",
        "ALBUM ARTIST": "album_artist",
        "GENRE": "genre",
        "TRACKNUMBER": "track",
        "DISCNUMBER": "disc",
    }
    for key, field in field_map.items():
        values = [value for value in comments.get(key) or [] if value]
        if not values:
            continue
        value = ", ".join(values) if field in ("artist", "genre") else values[0]
        if not tags.get(field):
            tags[field] = value
    for key in ("DATE", "ORIGINALDATE", "YEAR"):
        values = comments.get(key) or []
        if values and not tags.get("year"):
            tags["year"] = _text(values[0])[:4]
            break
    return tags


def _share_read_flac(path: str) -> Dict[str, Any]:
    tags: Dict[str, Any] = {}
    try:
        with open(path, "rb") as handle:
            if handle.read(4) != b"fLaC":
                return tags
            while True:
                head = handle.read(4)
                if len(head) < 4:
                    break
                block_type = head[0] & 0x7F
                length = int.from_bytes(head[1:4], "big")
                if block_type not in (0, 4, 6):
                    handle.seek(length, 1)
                    if head[0] & 0x80:
                        break
                    continue
                payload = handle.read(length)
                if block_type == 0 and len(payload) >= 18:  # STREAMINFO
                    sample_rate = int.from_bytes(payload[10:13], "big") >> 2
                    total_samples = ((payload[13] & 0x0F) << 32) | int.from_bytes(
                        payload[14:18], "big"
                    )
                    if sample_rate and total_samples:
                        tags["duration"] = total_samples / sample_rate
                elif block_type == 4:  # VORBIS_COMMENT
                    tags.update(_share_vorbis_tags(_share_parse_vorbis_comments(payload)))
                elif block_type == 6 and len(payload) > 32:  # PICTURE
                    pos = 4
                    mime_len = struct.unpack(">I", payload[pos : pos + 4])[0]
                    pos += 4
                    mime = payload[pos : pos + mime_len].decode("latin-1", "replace")
                    pos += mime_len
                    desc_len = struct.unpack(">I", payload[pos : pos + 4])[0]
                    pos += 4 + desc_len + 16
                    data_len = struct.unpack(">I", payload[pos : pos + 4])[0]
                    pos += 4
                    image = payload[pos : pos + data_len]
                    if len(image) > 8 and not tags.get("picture"):
                        tags["picture"] = {"mime": _share_image_kind(mime, image), "data": image}
                if head[0] & 0x80:
                    break
    except OSError:
        return {}
    return tags


def _share_ogg_comment_and_rate(handle: Any) -> tuple[Dict[str, List[str]], int, int]:
    """Read (comments, sample_rate, pre_skip) from an Ogg/Opus stream's head."""
    comments: Dict[str, List[str]] = {}
    sample_rate = 0
    pre_skip = 0
    packets: List[bytes] = []
    current = bytearray()
    while True:
        page_head = handle.read(27)
        if len(page_head) < 27 or not page_head.startswith(b"OggS"):
            break
        seg_count = page_head[26]
        seg_table = handle.read(seg_count)
        if len(seg_table) < seg_count:
            break
        payload = handle.read(sum(seg_table))
        pos = 0
        for seg in seg_table:
            current.extend(payload[pos : pos + seg])
            pos += seg
            if seg < 255:
                packets.append(bytes(current))
                current = bytearray()
        if len(packets) >= 2:
            break
    for packet in packets:
        if packet.startswith(b"\x01vorbis") and len(packet) >= 16:
            sample_rate = struct.unpack("<I", packet[12:16])[0]
        elif packet.startswith(b"OpusHead") and len(packet) >= 16:
            pre_skip = struct.unpack("<H", packet[10:12])[0]
            sample_rate = struct.unpack("<I", packet[12:16])[0]
        elif packet.startswith(b"\x03vorbis"):
            comments = _share_parse_vorbis_comments(packet[7:])
        elif packet.startswith(b"OpusTags"):
            comments = _share_parse_vorbis_comments(packet[8:])
    return comments, sample_rate, pre_skip


def _share_ogg_duration(path: str) -> float:
    """Last-page granule position, read from the file tail."""
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as handle:
            handle.seek(max(0, size - 65536))
            tail = handle.read()
    except OSError:
        return 0.0
    pos = tail.rfind(b"OggS")
    if pos < 0 or len(tail) < pos + 14:
        return 0.0
    try:
        granule = struct.unpack("<q", tail[pos + 6 : pos + 14])[0]
    except struct.error:
        return 0.0
    return max(0.0, float(granule))


def _share_read_ogg(path: str) -> Dict[str, Any]:
    tags: Dict[str, Any] = {}
    is_opus = path.casefold().endswith(".opus")
    try:
        with open(path, "rb") as handle:
            # The page walker validates the OggS magic itself.
            comments, sample_rate, pre_skip = _share_ogg_comment_and_rate(handle)
    except OSError:
        return {}
    tags.update(_share_vorbis_tags(comments))
    if is_opus:
        if sample_rate:
            # Opus always decodes at 48 kHz; the header rate is the original.
            duration = (_share_ogg_duration(path) - pre_skip) / 48000.0
        else:
            duration = 0.0
    else:
        duration = _share_ogg_duration(path) / sample_rate if sample_rate else 0.0
    if duration > 0:
        tags["duration"] = duration
    return tags


def _share_read_mp4(path: str) -> Dict[str, Any]:
    text_fields = {
        "\xa9nam": "title",
        "\xa9ART": "artist",
        "\xa9alb": "album",
        "aART": "album_artist",
        "\xa9gen": "genre",
        "gnre": "genre",
        "\xa9day": "year",
    }

    def walk(handle: Any, start: int, end: int, path: List[bytes]) -> Optional[Dict[str, Any]]:
        """Return {start, end} of the first atom whose container path matches."""
        pos = start
        while pos + 8 <= end:
            handle.seek(pos)
            header = handle.read(8)
            if len(header) < 8:
                return None
            size = struct.unpack(">I", header[0:4])[0]
            kind = header[4:8]
            header_len = 8
            if size == 1:
                extended = handle.read(8)
                if len(extended) < 8:
                    return None
                size = struct.unpack(">Q", extended)[0]
                header_len = 16
            elif size == 0:
                size = end - pos
            if size < header_len or pos + size > end:
                return None
            if kind == path[0]:
                content = pos + header_len
                if kind == b"meta":
                    content += 4  # version/flags precede meta's child atoms
                if len(path) == 1:
                    return {"start": content, "end": pos + size}
                return walk(handle, content, pos + size, path[1:])
            pos += size
        return None

    def read(handle: Any, start: int, end: int) -> bytes:
        handle.seek(start)
        return handle.read(max(0, end - start))

    tags: Dict[str, Any] = {}
    try:
        with open(path, "rb") as handle:
            handle.seek(0, 2)
            file_end = handle.tell()
            mvhd = walk(handle, 0, file_end, [b"moov", b"mvhd"])
            if mvhd:
                blob = read(handle, mvhd["start"], mvhd["end"])
                if len(blob) >= 20 and blob[0] == 0:  # version 0
                    timescale = struct.unpack(">I", blob[12:16])[0]
                    duration = struct.unpack(">I", blob[16:20])[0]
                elif len(blob) >= 28:  # version 1
                    timescale = struct.unpack(">I", blob[20:24])[0]
                    duration = struct.unpack(">Q", blob[24:32])[0]
                else:
                    timescale = duration = 0
                if timescale and duration:
                    tags["duration"] = duration / timescale
            ilst = walk(handle, 0, file_end, [b"moov", b"udta", b"meta", b"ilst"])
            if ilst:
                blob = read(handle, ilst["start"], ilst["end"])
                pos = 0
                picture: Dict[str, Any] = {}
                while pos + 8 <= len(blob):
                    size = struct.unpack(">I", blob[pos : pos + 4])[0]
                    key = blob[pos + 4 : pos + 8].decode("latin-1", "replace")
                    if size < 8 or pos + size > len(blob):
                        break
                    item = blob[pos + 8 : pos + size]
                    pos += size
                    data_pos = 0
                    while data_pos + 16 <= len(item):
                        data_size = struct.unpack(">I", item[data_pos : data_pos + 4])[0]
                        if item[data_pos + 4 : data_pos + 8] != b"data" or data_size < 16:
                            break
                        flags = struct.unpack(">I", item[data_pos + 8 : data_pos + 12])[0] & 0xFFFFFF
                        payload = item[data_pos + 16 : data_pos + data_size]
                        data_pos += data_size
                        field = text_fields.get(key)
                        if field and flags in (0, 1, 2, 3) and not tags.get(field):
                            text = payload.decode("utf-8", "replace").strip()
                            if text:
                                tags[field] = text[:4] if field == "year" else text
                        elif key == "trkn" and len(payload) >= 4:
                            tags["track"] = str(struct.unpack(">H", payload[2:4])[0])
                        elif key == "disk" and len(payload) >= 4:
                            tags["disc"] = str(struct.unpack(">H", payload[2:4])[0])
                        elif key == "covr" and flags in (13, 14) and not picture:
                            picture = {
                                "mime": "jpg" if flags == 13 else "png",
                                "data": payload,
                            }
                if picture and not tags.get("picture"):
                    tags["picture"] = picture
    except OSError:
        return {}
    return tags


def _share_read_wav(path: str) -> Dict[str, Any]:
    try:
        with open(path, "rb") as handle:
            riff = handle.read(12)
            if len(riff) < 12 or riff[0:4] != b"RIFF" or riff[8:12] != b"WAVE":
                return {}
            byte_rate = 0
            data_size = 0
            while True:
                head = handle.read(8)
                if len(head) < 8:
                    break
                chunk_id = head[0:4]
                chunk_size = struct.unpack("<I", head[4:8])[0]
                if chunk_id == b"fmt " and chunk_size >= 16:
                    fmt = handle.read(min(chunk_size, 16))
                    byte_rate = struct.unpack("<I", fmt[8:12])[0]
                    handle.seek(chunk_size - len(fmt), 1)
                elif chunk_id == b"data":
                    data_size = chunk_size
                    break
                else:
                    handle.seek(chunk_size + (chunk_size & 1), 1)
    except OSError:
        return {}
    if byte_rate and data_size:
        return {"duration": data_size / byte_rate}
    return {}


def _share_tags_from_path(path: str) -> Dict[str, Any]:
    """Filename fallback: Artist/Album/NN - Title.ext or Artist - Title.ext."""
    rel = Path(path)
    stem = rel.stem
    tags: Dict[str, Any] = {}
    match = re.match(r"^\s*(\d{1,3})\s*[-.)]\s*(.+)$", stem)
    if match:
        tags["track"] = match.group(1)
        stem = match.group(2)
    parts = rel.parts
    if len(parts) >= 3:
        tags.setdefault("artist", parts[-3])
        tags.setdefault("album", parts[-2])
    elif len(parts) == 2:
        tags.setdefault("album", parts[-2])
    if " - " in stem and not tags.get("artist"):
        artist, _, title = stem.partition(" - ")
        tags["artist"] = artist.strip()
        stem = title
    tags.setdefault("title", stem.strip() or rel.name)
    return tags


def _share_nfo_album_metadata(text: str) -> Dict[str, str]:
    """Read album fields out of one Kodi-style NFO document."""
    stripped = _text(text).lstrip("﻿")
    if not stripped:
        return {}
    root: Any = None
    try:
        root = ElementTree.fromstring(stripped)
    except Exception:
        # Kodi NFOs may carry trailing junk after the XML document; retry with
        # just the <album>…</album> block when the full parse fails.
        start = stripped.casefold().find("<album")
        end = stripped.casefold().rfind("</album>")
        if start < 0 or end <= start:
            return {}
        try:
            root = ElementTree.fromstring(stripped[start : end + 8])
        except Exception:
            return {}
    if root is None:
        return {}

    def tag_name(element: Any) -> str:
        return _text(getattr(element, "tag", "")).rsplit("}", 1)[-1].casefold()

    album = root if tag_name(root) == "album" else next(
        (element for element in root.iter() if tag_name(element) == "album"), None
    )
    if album is None:
        return {}
    metadata: Dict[str, str] = {}
    for key in ("albumartist", "artist"):
        values = [
            _text(child.text)
            for child in album
            if tag_name(child) == key and _text(child.text)
        ]
        if values:
            metadata[key] = ", ".join(values)
    return metadata


def _share_album_nfo(dirpath: str, filenames: List[str]) -> Dict[str, str]:
    """The album.nfo sitting next to a share album's tracks, if any."""
    for name in filenames:
        if _text(name).casefold() != "album.nfo":
            continue
        try:
            with open(os.path.join(dirpath, name), "r", encoding="utf-8", errors="replace") as handle:
                return _share_nfo_album_metadata(handle.read())
        except OSError:
            return {}
    return {}


def _share_m3u_track_rel(entry: str, m3u_rel: str) -> Optional[str]:
    """Normalize one .m3u line to a share-relative path (forward slashes)."""
    line = _text(entry).strip().strip('"')
    if not line or line.startswith("#"):
        return None
    if line.casefold().startswith("file://"):
        line = unquote(line[7:])
    # Windows-authored playlists use backslashes; UNC hosts stay untouched.
    if line.startswith("\\\\"):
        return None
    line = line.replace("\\", "/")
    # Remote entries (http://…) and anything not on this share cannot resolve.
    if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", line):
        return None
    candidates = []
    if line.startswith("/"):
        candidates.append(line.lstrip("/"))
    else:
        # Relative entries resolve against the .m3u's own folder first, then
        # the share root (the common "flat" export).
        m3u_dir = "/".join(m3u_rel.split("/")[:-1])
        if m3u_dir:
            candidates.append(f"{m3u_dir}/{line}")
        candidates.append(line)
    for candidate in candidates:
        normalized = os.path.normpath(candidate).replace(os.sep, "/")
        if normalized and not normalized.startswith("../") and normalized != "..":
            return normalized
    return None


def _share_m3u_playlists(
    root: str,
    m3u_files: List[str],
    path_to_id: Dict[str, str],
) -> List[Dict[str, Any]]:
    """User-made .m3u/.m3u8 playlists on the share, as resolvable track id lists."""
    if not m3u_files:
        return []
    lookup = dict(path_to_id)
    casefolded = {key.casefold(): value for key, value in path_to_id.items()}
    playlists: List[Dict[str, Any]] = []
    for m3u_rel in sorted(m3u_files):
        try:
            with open(os.path.join(root, m3u_rel), "r", encoding="utf-8", errors="replace") as handle:
                lines = handle.read().splitlines()
        except OSError:
            continue
        track_ids: List[str] = []
        for line in lines:
            rel = _share_m3u_track_rel(line, m3u_rel)
            if not rel:
                continue
            track_id = lookup.get(rel) or casefolded.get(rel.casefold())
            if track_id and track_id not in track_ids:
                track_ids.append(track_id)
        if not track_ids:
            continue
        playlists.append(
            {
                "id": "share_m3u:" + hashlib.sha1(m3u_rel.encode("utf-8")).hexdigest()[:16],
                "name": Path(m3u_rel).stem,
                "description": "",
                "track_ids": track_ids[:MAX_CATALOG_TRACKS],
            }
        )
        if len(playlists) >= 200:
            break
    return sorted(playlists, key=lambda row: _text(row.get("name")).casefold())


def _share_read_tags(path: str) -> Dict[str, Any]:
    suffix = Path(path).suffix.casefold()
    if suffix == ".mp3":
        tags = _share_read_id3(path)
    elif suffix == ".flac":
        tags = _share_read_flac(path)
    elif suffix in (".ogg", ".oga", ".opus"):
        tags = _share_read_ogg(path)
    elif suffix in (".m4a", ".mp4"):
        tags = _share_read_mp4(path)
    elif suffix == ".wav":
        tags = _share_read_wav(path)
    else:
        tags = {}
    if "picture" in tags and not isinstance(tags.get("picture"), dict):
        tags["picture"] = {}
    fallback = _share_tags_from_path(path)
    for key, value in fallback.items():
        if not tags.get(key):
            tags[key] = value
    if not tags.get("album_artist") and tags.get("artist"):
        tags["album_artist"] = tags["artist"]
        # Marker for the catalog walk: album.nfo should outrank the track
        # artist when it carries a real album artist.
        tags["album_artist_fallback"] = True
    return tags


def _share_art_cache_dir() -> str:
    return os.path.join(tempfile.gettempdir(), SHARE_ART_CACHE_DIRNAME)


def _share_track_id(rel_path: str) -> str:
    return "track:" + hashlib.sha256(rel_path.encode("utf-8")).hexdigest()[:24]


def _share_stream_id(rel_path: str) -> str:
    """URL-safe, path-free id for one share file (token-gated at the proxy)."""
    return base64.urlsafe_b64encode(rel_path.encode("utf-8")).decode("ascii").rstrip("=")


def _share_root_scope(root: str) -> str:
    """Stable short token for one share root (per-Person roots get their own)."""
    return hashlib.sha1(_text(root).encode("utf-8")).hexdigest()[:12]


def _share_scoped_stream_id(rel_path: str, root: str) -> str:
    """Stream id that carries its share root, so per-Person roots stream correctly."""
    return f"{_share_stream_id(root)}~{_share_stream_id(rel_path)}"


def _share_root_and_relpath_from_id(
    stream_id: str,
    default_root: str,
) -> "tuple[Optional[str], Optional[str]]":
    """Decode a share stream id to (root, rel path).

    Scoped ids ("<root b64>~<rel b64>") resolve against their own root; legacy
    ids (no "~") keep resolving against the global share root.
    """
    scoped, sep, rel_token = _text(stream_id).partition("~")
    if sep:
        padding = "=" * (-len(scoped) % 4)
        try:
            root = base64.urlsafe_b64decode(scoped + padding).decode("utf-8")
            rel_path = base64.urlsafe_b64decode(
                rel_token + "=" * (-len(rel_token) % 4)
            ).decode("utf-8")
        except Exception:
            return None, None
        return root, rel_path
    return _text(default_root), _share_relpath_from_id(stream_id)


def _share_art_index_key(root: str) -> str:
    """Redis key for one share root's artwork index (legacy key for the global root)."""
    if not _text(root) or _text(root) == _text(_settings().get("share_root_path")):
        return SHARE_ART_INDEX_KEY
    return f"{SHARE_ART_INDEX_KEY}:{_share_root_scope(root)}"


def _share_relpath_from_id(stream_id: str) -> Optional[str]:
    padding = "=" * (-len(stream_id) % 4)
    try:
        return base64.urlsafe_b64decode(stream_id + padding).decode("utf-8")
    except Exception:
        return None


def _share_contained_path(root: str, rel_path: str) -> Optional[str]:
    """Resolve one share-relative path, refusing anything that escapes the root."""
    if not rel_path or rel_path.startswith("/") or ".." in Path(rel_path).parts:
        return None
    resolved = os.path.realpath(os.path.join(root, rel_path))
    root = os.path.realpath(root)
    if resolved != root and not resolved.startswith(root + os.sep):
        return None
    return resolved


def _share_store_art(
    art_index: Dict[str, Dict[str, Any]],
    source_path: str,
    version: str,
    image: Optional[Dict[str, Any]] = None,
    scope: str = "",
) -> str:
    """Register one artwork source; embedded images are extracted to a cache file."""
    digest = hashlib.sha1(source_path.encode("utf-8")).hexdigest()[:20]
    # Scoped ids carry their share root's scope so per-Person libraries keep
    # artwork separate from the household's.
    art_id = f"{scope}:{digest}" if scope else digest
    if image and isinstance(image.get("data"), bytes) and image["data"]:
        cache_path = os.path.join(
            _share_art_cache_dir(), f"{digest}.{_text(image.get('mime')) or 'jpg'}"
        )
        if not os.path.isfile(cache_path):
            try:
                os.makedirs(_share_art_cache_dir(), exist_ok=True)
                with open(cache_path, "wb") as handle:
                    handle.write(image["data"])
            except OSError:
                return ""
        art_index[art_id] = {"path": cache_path, "version": version}
    elif os.path.isfile(source_path):
        art_index[art_id] = {"path": source_path, "version": version}
    else:
        return ""
    return art_id


@dataclass
class NetworkShareMusicProvider:
    """A mounted SMB/CIFS or NFS share treated as a local music folder.

    The Tater host (or container, via a compose bind mount) owns the mount; this
    provider never mounts anything itself. Files stream through the core's own
    Range-capable stream server so share paths and credentials never reach
    playback targets.
    """

    root_path: str
    provider_id: str = "network_share"

    @classmethod
    def from_settings(cls, cfg: Dict[str, str]) -> "NetworkShareMusicProvider":
        return cls(root_path=_text(cfg.get("share_root_path")))

    @property
    def connected(self) -> bool:
        root = _text(self.root_path)
        if not root:
            return False
        try:
            return os.path.isdir(root) and os.access(root, os.R_OK)
        except OSError:
            return False

    def stream_url(self, track: Dict[str, Any], *, audio_sync: bool = False) -> str:
        del audio_sync  # No stdlib transcode path; satellites handle the container.
        stream_id = _text(track.get("provider_track_id")) or _share_stream_id(
            _text(track.get("path"))
        )
        if not stream_id:
            return _text(track.get("stream_url"))
        return _stream_proxy_url("share", stream_id)

    def artwork_url(self, track: Dict[str, Any]) -> str:
        art_id = _text(track.get("artwork_item_id"))
        if not art_id or art_id == _text(track.get("id")):
            return ""
        return _stream_proxy_url("share_art", art_id)

    def catalog(self) -> Dict[str, Any]:
        root = _text(self.root_path)
        if not self.connected:
            raise ValueError(
                "Mount the network share on the Tater host and set its folder path before syncing."
            )
        catalog_id = "share:" + hashlib.sha1(root.encode("utf-8")).hexdigest()[:16]
        # Per-Person share roots get their own artwork index and root-scoped
        # stream ids so they can coexist with the household's share library.
        art_scope = "" if not _text(root) else _share_root_scope(root)
        if root == _text(_settings().get("share_root_path")):
            art_scope = ""
        tracks: List[Dict[str, Any]] = []
        art_index: Dict[str, Dict[str, Any]] = {}
        path_to_id: Dict[str, str] = {}
        m3u_files: List[str] = []
        nfo_cache: Dict[str, Dict[str, str]] = {}
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted(name for name in dirnames if not name.startswith("."))
            album_art: Optional[Dict[str, Any]] = None
            folder_art_id = ""
            for name in sorted(filenames):
                if name.casefold() in SHARE_FOLDER_ARTWORK_NAMES:
                    source = os.path.join(dirpath, name)
                    folder_art_id = _share_store_art(
                        art_index, source, str(int(os.path.getmtime(source))), scope=art_scope
                    )
                    break
            for name in sorted(filenames):
                if Path(name).suffix.casefold() in SHARE_PLAYLIST_EXTENSIONS:
                    m3u_files.append(os.path.relpath(os.path.join(dirpath, name), root))
            for name in sorted(filenames):
                if Path(name).suffix.casefold() not in SHARE_AUDIO_EXTENSIONS:
                    continue
                if len(tracks) >= MAX_CATALOG_TRACKS:
                    break
                full_path = os.path.join(dirpath, name)
                rel_path = os.path.relpath(full_path, root)
                try:
                    stat = os.stat(full_path)
                    tags = _share_read_tags(full_path)
                except OSError:
                    continue
                if tags.pop("album_artist_fallback", False):
                    # No AlbumArtist ID3 tag: a local album.nfo outranks the
                    # track-artist fallback (compilations, Various Artists).
                    nfo = nfo_cache.get(dirpath)
                    if nfo is None:
                        nfo = _share_album_nfo(dirpath, filenames)
                        nfo_cache[dirpath] = nfo
                    if nfo.get("albumartist") or nfo.get("artist"):
                        tags["album_artist"] = nfo.get("albumartist") or nfo.get("artist")
                artwork: Dict[str, Any] = {}
                embedded = tags.pop("picture", None)
                version = str(int(stat.st_mtime))
                if folder_art_id:
                    artwork = {"id": folder_art_id, "version": version}
                elif isinstance(embedded, dict) and embedded.get("data"):
                    art_id = _share_store_art(
                        art_index, full_path, version, image=embedded, scope=art_scope
                    )
                    if art_id:
                        artwork = {"id": art_id, "version": version}
                genres = _genres(tags.get("genre"))
                track_id = (
                    _share_track_id(rel_path)
                    if not art_scope
                    else "track:" + hashlib.sha256(f"{root}\x00{rel_path}".encode("utf-8")).hexdigest()[:24]
                )
                path_to_id[rel_path.replace(os.sep, "/")] = track_id
                tracks.append(
                    {
                        "id": track_id,
                        "provider_track_id": _share_scoped_stream_id(rel_path, root),
                        "title": _text(tags.get("title")) or Path(name).stem,
                        "artist": _text(tags.get("artist")),
                        "album_artist": _text(tags.get("album_artist")),
                        "album": _text(tags.get("album")),
                        "genres": genres,
                        "genre": ", ".join(genres),
                        "year": _text(tags.get("year"))[:4],
                        "track_number": _as_int(tags.get("track"), 0, 0, 10000),
                        "disc_number": _as_int(tags.get("disc"), 0, 0, 1000),
                        "duration_seconds": max(
                            0.0, _as_float(tags.get("duration"))
                        ),
                        "path": rel_path,
                        "container": Path(name).suffix.lstrip(".").lower(),
                        "size_bytes": stat.st_size,
                        "modified_unix": int(stat.st_mtime),
                        "artwork_item_id": artwork.get("id", ""),
                        "artwork_version": artwork.get("version", ""),
                        "provider": self.provider_id,
                    }
                )
            if len(tracks) >= MAX_CATALOG_TRACKS:
                break
        store = globals().get("redis_client")
        if store is not None:
            _save_json(store, _share_art_index_key(root), art_index)
        return {
            "catalog_id": catalog_id,
            "tracks": tracks,
            "total": len(tracks),
            "playlists": _share_m3u_playlists(root, m3u_files, path_to_id),
            "libraries": {catalog_id: "Network Share"},
        }


@dataclass
class SubsonicMusicProvider:
    """One Subsonic-API music server (Navidrome, Airsonic, Gonic, Ampache, …).

    Speaks the salted-token REST dialect (``u``/``t``/``s`` params) with a fresh
    random salt per request, so nothing credential-like is ever stable or
    reusable. Streams and artwork are proxied through this core's stream server
    (the salted token *is* the credential and must never sit in a URL a playback
    target fetches). Original MP3/FLAC/WAV bytes stream as-is — satellites
    decode all three — and non-decodable containers ask the server to transcode
    to WAV instead.
    """

    server_url: str
    username: str
    password: str
    # Person id when this instance serves a linked Person's own server account.
    stream_scope: str = ""
    provider_id = "subsonic"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "SubsonicMusicProvider":
        return cls(
            server_url=_normalize_server_url(settings.get("subsonic_server_url")),
            username=_text(settings.get("subsonic_username")),
            password=_text(settings.get("subsonic_password")),
        )

    @property
    def connected(self) -> bool:
        # No network probe here: `connected` mirrors the other providers'
        # settings-shape check, and the connect/test flow does the live ping.
        return bool(self.server_url and self.username and self.password)

    def _request_params(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Subsonic auth params with a fresh salted token for one request."""
        salt = uuid.uuid4().hex
        token = hashlib.md5(f"{self.password}{salt}".encode("utf-8")).hexdigest()
        params: Dict[str, Any] = {
            "u": self.username,
            "t": token,
            "s": salt,
            "v": SUBSONIC_API_VERSION,
            "c": SUBSONIC_CLIENT_NAME,
            "f": "json",
        }
        params.update(extra or {})
        return params

    def request(
        self,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        """One GET against the REST endpoint, unwrapped to the response dict."""
        if not self.server_url:
            raise ValueError("Subsonic server URL is not configured.")
        url = f"{self.server_url}/rest/{path}"
        response = requests.get(
            url,
            params=self._request_params(params),
            headers={"Accept": "application/json"},
            timeout=max(5, int(timeout)),
        )
        if response.status_code in (401, 403):
            raise PermissionError("Subsonic rejected this core's credentials.")
        if not response.ok:
            raise RuntimeError(f"Subsonic returned HTTP {response.status_code} for {path}.")
        body = response.json() if response.content else {}
        wrapper = body.get("subsonic-response") if isinstance(body, dict) else None
        if not isinstance(wrapper, dict):
            raise RuntimeError(f"Subsonic returned an unparseable response for {path}.")
        if wrapper.get("status") != "ok":
            error = wrapper.get("error") if isinstance(wrapper.get("error"), dict) else {}
            code = _as_int(error.get("code"), 0, 0, 10**6)
            message = _text(error.get("text")) or "Subsonic rejected the request."
            # 40: wrong user/password, 41: token auth not supported for this
            # account, 50: user lacks permission.
            if code in (40, 41, 50):
                raise PermissionError(message)
            raise RuntimeError(message)
        return wrapper

    def ping(self) -> None:
        """Connection check used by the test action (raises on failure)."""
        self.request("ping", timeout=15)

    def catalog(self) -> Dict[str, Any]:
        albums: List[Dict[str, Any]] = []
        offset = 0
        while offset < MAX_CATALOG_TRACKS and len(albums) < MAX_CATALOG_TRACKS:
            page = self.request(
                "getAlbumList2",
                params={
                    "type": "alphabeticalByName",
                    "size": SUBSONIC_PAGE_SIZE,
                    "offset": offset,
                },
                timeout=180,
            )
            rows = page.get("albumList2", {}).get("album", [])
            if not isinstance(rows, list):
                break
            for row in rows:
                if isinstance(row, dict) and _text(row.get("id")):
                    albums.append(row)
            offset += len(rows)
            if not rows or len(rows) < SUBSONIC_PAGE_SIZE:
                break
        tracks: List[Dict[str, Any]] = []
        for album in albums:
            if len(tracks) >= MAX_CATALOG_TRACKS:
                break
            detail = self.request("getAlbum", params={"id": _text(album.get("id"))}, timeout=120)
            songs = detail.get("album", {}).get("song", [])
            if not isinstance(songs, list):
                continue
            for song in songs:
                if isinstance(song, dict) and _text(song.get("id")):
                    tracks.append(_subsonic_track_row(song, album))
        playlists = []
        try:
            playlists = self.user_playlists()
        except Exception as exc:
            # A playlist listing failure must never break the song sync.
            logger.warning("[Music] Subsonic playlist listing failed: %s", exc)
        catalog_id = f"subsonic:{hashlib.sha1(self.server_url.encode('utf-8')).hexdigest()[:16]}"
        return {
            "catalog_id": catalog_id,
            "tracks": tracks[:MAX_CATALOG_TRACKS],
            "total": len(tracks),
            "playlists": playlists,
            "libraries": {catalog_id: self.server_url},
        }

    def user_playlists(self) -> List[Dict[str, Any]]:
        """The signed-in Subsonic user's playlists with their song ids."""
        playlists: List[Dict[str, Any]] = []
        wrapper = self.request("getPlaylists", timeout=60)
        rows = wrapper.get("playlists", {}).get("playlist", [])
        for row in rows if isinstance(rows, list) else []:
            playlist_id = _text(row.get("id")) if isinstance(row, dict) else ""
            name = _text(row.get("name")) if isinstance(row, dict) else ""
            if not playlist_id or not name:
                continue
            track_ids: List[str] = []
            try:
                detail = self.request("getPlaylist", params={"id": playlist_id}, timeout=60)
                entries = detail.get("playlist", {}).get("entry", [])
                for entry in entries if isinstance(entries, list) else []:
                    if isinstance(entry, dict) and _text(entry.get("id")):
                        track_ids.append(_text(entry.get("id")))
            except Exception as exc:
                logger.warning("[Music] Subsonic playlist %s listing failed: %s", playlist_id, exc)
            playlists.append(
                {
                    "id": f"subsonic_playlist:{playlist_id}",
                    "name": name,
                    "description": _text(row.get("comment")),
                    "track_ids": track_ids,
                }
            )
            if len(playlists) >= 200:
                break
        return playlists

    def similar_tracks(
        self,
        seeds: List[Dict[str, Any]],
        *,
        count: int = CONTINUATION_BATCH_TRACKS,
    ) -> List[Dict[str, Any]]:
        """Server-suggested tracks after one seed; [] when unavailable.

        Implementations vary widely (Navidrome uses last.fm scrobbles), so this
        is best-effort: any failure or empty result just falls back to the
        library mix.
        """
        for seed in seeds or []:
            track_id = _text(seed.get("provider_track_id")) or _text(seed.get("id"))
            if not track_id:
                continue
            try:
                song = self.request("getSong", params={"id": track_id}, timeout=15)
                artist_id = _text(song.get("song", {}).get("artistId"))
                if not artist_id:
                    continue
                similar = self.request(
                    "getSimilarSongs2",
                    params={"id": artist_id, "count": max(1, int(count))},
                    timeout=15,
                )
                rows = similar.get("similarSongs2", {}).get("song", [])
            except Exception as exc:
                logger.warning("[Music] Subsonic similar-tracks lookup failed: %s", exc)
                continue
            results: List[Dict[str, Any]] = []
            for row in rows if isinstance(rows, list) else []:
                if isinstance(row, dict) and _text(row.get("id")):
                    results.append(_subsonic_track_row(row))
                if len(results) >= count:
                    break
            return results
        return []

    def stream_url(self, track: Dict[str, Any], *, audio_sync: bool = False) -> str:
        del audio_sync  # Original containers decode natively; offsets stay aligned.
        item_id = _text(track.get("provider_track_id")) or _text(track.get("id"))
        if not item_id:
            return _text(track.get("stream_url"))
        container = _text(track.get("container")).casefold()
        if container and container not in SAT_SAFE_AUDIO_CONTAINERS:
            # Satellites decode WAV/MP3/FLAC only; ask the server to transcode
            # anything else instead of serving undecodable bytes.
            item_id = f"{item_id}~wav"
        kind = f"subsonic:{self.stream_scope}" if self.stream_scope else "subsonic"
        return _stream_proxy_url(kind, item_id)

    def artwork_url(self, track: Dict[str, Any]) -> str:
        art_id = _text(track.get("artwork_item_id")) or _text(track.get("album_id"))
        if not art_id:
            return ""
        kind = f"subsonic_art:{self.stream_scope}" if self.stream_scope else "subsonic_art"
        return _stream_proxy_url(kind, art_id)

    def proxy_request(
        self,
        item_id: str,
        *,
        art: bool = False,
        sync: bool = False,
    ) -> "tuple[str, Dict[str, str]]":
        """Build (url, headers) for one proxied Subsonic stream or artwork request.

        The salted auth token is generated fresh for every proxied request
        server-side, so no credential ever lands in a playback target's URL.
        """
        item_id = _text(item_id)
        params: Dict[str, Any] = {}
        if item_id.endswith("~wav"):
            item_id = item_id[: -len("~wav")]
            params["format"] = "wav"
        params["id"] = item_id
        return (
            f"{self.server_url}/rest/{'getCoverArt' if art else 'stream'}"
            f"?{urlencode(self._request_params(params))}",
            {"Accept": "*/*"},
        )


def _subsonic_track_row(song: Dict[str, Any], album: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """One Subsonic song as a generic provider track row (for _normalize_track)."""
    album = album if isinstance(album, dict) else {}
    cover_art = _text(song.get("coverArt")) or _text(album.get("coverArt"))
    return {
        "id": _text(song.get("id")),
        "provider_track_id": _text(song.get("id")),
        "title": _text(song.get("title")),
        "artist": _text(song.get("artist")) or _text(album.get("artist")),
        "album_artist": _text(album.get("artist")) or _text(song.get("albumArtist")),
        "album": _text(album.get("name")) or _text(song.get("album")),
        "albumId": _text(song.get("albumId")) or _text(album.get("id")),
        "genres": [song.get("genre")] if _text(song.get("genre")) else [],
        "genre": _text(song.get("genre")),
        "year": _text(song.get("year")) or _text(album.get("year")),
        "track_number": song.get("track"),
        "disc_number": song.get("discNumber"),
        "duration_seconds": song.get("duration"),
        "container": _text(song.get("suffix")).lower(),
        "artwork_item_id": _text(cover_art),
        "artistId": _text(song.get("artistId")) or _text(album.get("artistId")),
        "path": _text(song.get("path")),
        "provider": "subsonic",
    }


# --------------------------------------------------------------------------
# plex.tv authentication (isolated here so endpoint fixes stay local).
# --------------------------------------------------------------------------


def _plex_client_identifier(store: Any = None) -> str:
    """Stable per-core uuid plex.tv expects on every request it sees."""
    client = store or globals().get("redis_client")
    if client is not None:
        value = _text(_settings(client).get("plex_client_identifier"))
        if value:
            return value
        value = uuid.uuid4().hex
        _save_hash(client, SETTINGS_KEY, {"plex_client_identifier": value})
        return value
    return uuid.uuid4().hex


def _plex_tv_headers(token: str = "") -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
        "X-Plex-Product": "Personal Music Core",
        "X-Plex-Version": __version__,
        "X-Plex-Client-Identifier": _plex_client_identifier(),
    }
    if token:
        headers["X-Plex-Token"] = token
    return headers


def _plex_tv_request(
    method: str,
    path: str,
    *,
    token: str = "",
    form: Optional[Dict[str, str]] = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    """One plex.tv API call (every plex.tv endpoint lives behind this helper)."""
    response = requests.request(
        method.upper(),
        f"{PLEX_TV_API_BASE}{path}",
        headers=_plex_tv_headers(token),
        data=form or None,
        timeout=max(5, int(timeout)),
    )
    if response.status_code in (401, 403):
        raise PermissionError("plex.tv rejected the credentials.")
    if not response.ok:
        raise RuntimeError(f"plex.tv returned HTTP {response.status_code} for {path}.")
    if not response.content:
        return {}
    try:
        body = response.json()
    except Exception as exc:
        raise RuntimeError(f"plex.tv returned an unparseable response for {path}.") from exc
    # /resources answers with a top-level JSON array; keep lists too.
    return body if isinstance(body, (dict, list)) else {}


def _plex_tv_signin(login: str, password: str) -> str:
    """plex.tv sign-in with one account's own credentials → its plex.tv token."""
    if not login or not password:
        raise ValueError("Enter the Plex username and password to sign in with.")
    body = _plex_tv_request(
        "POST",
        "/users/signin",
        form={"login": login, "password": password},
    )
    token = _text(body.get("authToken"))
    if not token:
        raise RuntimeError("plex.tv sign-in did not return an auth token.")
    return token


def _plex_tv_home_users(token: str) -> List[Dict[str, str]]:
    """Plex Home members visible to the owner token (uuid + title rows)."""
    body = _plex_tv_request("GET", "/home/users", token=token)
    users = body.get("users") if isinstance(body.get("users"), list) else []
    rows: List[Dict[str, str]] = []
    for row in users:
        if not isinstance(row, dict):
            continue
        uuid_value = _text(row.get("uuid") or row.get("id"))
        if uuid_value:
            rows.append({"uuid": uuid_value, "title": _text(row.get("title"))})
    return rows


def _plex_tv_switch_home_user(token: str, home_uuid: str, pin: str = "") -> str:
    """Switch into one Plex Home member's context → their per-user token."""
    form = {"pin": pin} if pin else {}
    body = _plex_tv_request(
        "POST",
        f"/home/users/{quote(str(home_uuid), safe='')}/switch",
        token=token,
        form=form,
    )
    new_token = _text(body.get("authToken"))
    if not new_token:
        raise RuntimeError("plex.tv did not return a token for that Home user.")
    return new_token


def _plex_tv_resources(token: str) -> List[Dict[str, Any]]:
    """plex.tv-known servers (owned + shared) with their connection URIs."""
    body = _plex_tv_request("GET", "/resources", token=token)
    rows = body if isinstance(body, list) else []
    servers: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict) or _text(row.get("product")).casefold() != "plex media server":
            continue
        connections = [conn for conn in row.get("connections") or [] if isinstance(conn, dict)]
        uris = [_text(conn.get("uri")) for conn in connections if _text(conn.get("uri"))]
        servers.append(
            {
                "name": _text(row.get("name")),
                "owned": _as_bool(row.get("owned"), False),
                "connections": uris,
            }
        )
    return servers


def _plex_tv_user_id(token: str) -> str:
    """The signed-in account's own plex.tv user uuid."""
    body = _plex_tv_request("GET", "/user", token=token)
    return _text(body.get("uuid") or body.get("id"))


@dataclass
class PlexMusicProvider:
    """One Plex Media Server music library, addressed with an X-Plex-Token.

    The token travels server-side only: the core stream proxy attaches it to
    every stream and artwork request, so per-user tokens never sit in URLs that
    playback targets fetch. Plex.tv sign-ins (three auth modes, see
    _plex_resolve_credentials) converge on the same provider state: a server
    URL plus the resolved user's token.
    """

    server_url: str
    token: str
    # Person id when this instance serves a linked Person's own Plex account.
    stream_scope: str = ""
    # plex.tv user uuid of the resolved identity (drives the auth cache key).
    user_id: str = ""
    provider_id = "plex"

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "PlexMusicProvider":
        return cls(
            server_url=_normalize_server_url(settings.get("plex_server_url")),
            token=_text(settings.get("plex_token")),
            user_id=_text(settings.get("plex_user_id")),
        )

    @classmethod
    def from_link_values(
        cls, values: Dict[str, Any], stream_scope: str = ""
    ) -> "PlexMusicProvider":
        return cls(
            server_url=_normalize_server_url(values.get("server_url")),
            token=_text(values.get("token")),
            user_id=_text(values.get("user_id")),
            stream_scope=stream_scope,
        )

    @property
    def connected(self) -> bool:
        return bool(self.server_url and self.token)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        if not self.server_url:
            raise ValueError("Plex server URL is not configured.")
        query: Dict[str, Any] = dict(params or {})
        query.setdefault("X-Plex-Token", self.token)
        response = requests.request(
            method.upper(),
            f"{self.server_url}/{path.lstrip('/')}",
            params=query,
            headers={"Accept": "application/json", **(headers or {})},
            timeout=max(5, int(timeout)),
        )
        if response.status_code in (401, 403):
            self.clear_cached_auth()
            raise PermissionError(
                "Plex rejected this core's token — re-test the Plex connection."
            )
        if not response.ok:
            raise RuntimeError(f"Plex returned HTTP {response.status_code} for {path}.")
        try:
            body = response.json() if response.content else {}
        except Exception:
            return {}
        return body if isinstance(body, dict) else {}

    def _metadata(self, body: Dict[str, Any]) -> List[Dict[str, Any]]:
        holder = body.get("MediaContainer")
        rows = holder.get("Metadata") if isinstance(holder, dict) else None
        return [row for row in (rows or []) if isinstance(row, dict)] if isinstance(rows, list) else []

    def catalog(self) -> Dict[str, Any]:
        sections = self.request("GET", "library/sections")
        holder = sections.get("MediaContainer") if isinstance(sections, dict) else {}
        rows = holder.get("Directory") if isinstance(holder, dict) else []
        sections_seen: Dict[str, str] = {}
        tracks: List[Dict[str, Any]] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or _text(row.get("type")) != "artist":
                # Shared users only see the sections shared with them; skip the rest.
                continue
            section_key = _text(row.get("key"))
            if not section_key:
                continue
            sections_seen[section_key] = _text(row.get("title")) or "Music"
            offset = 0
            while len(tracks) < MAX_CATALOG_TRACKS:
                page = self.request(
                    "GET",
                    f"library/sections/{section_key}/all",
                    params={"type": 10},
                    headers={
                        "X-Plex-Container-Start": offset,
                        "X-Plex-Container-Size": PLEX_PAGE_SIZE,
                    },
                    timeout=180,
                )
                metadata = self._metadata(page)
                for media_row in metadata:
                    if len(tracks) >= MAX_CATALOG_TRACKS:
                        break
                    track_row = _plex_track_row(media_row)
                    if _text(track_row.get("id")):
                        tracks.append(track_row)
                if len(metadata) < PLEX_PAGE_SIZE or len(tracks) >= MAX_CATALOG_TRACKS:
                    break
                offset += len(metadata)
        playlists = []
        try:
            playlists = self.user_playlists()
        except Exception as exc:
            logger.warning("[Music] Plex playlist listing failed: %s", exc)
        catalog_id = f"plex:{hashlib.sha1(self.server_url.encode('utf-8')).hexdigest()[:16]}"
        return {
            "catalog_id": catalog_id,
            "tracks": tracks[:MAX_CATALOG_TRACKS],
            "total": len(tracks),
            "playlists": playlists,
            "libraries": {
                catalog_id: " · ".join(sections_seen.values()) or "Plex",
            },
        }

    def user_playlists(self) -> List[Dict[str, Any]]:
        """The signed-in Plex user's playlists with their song ids."""
        playlists: List[Dict[str, Any]] = []
        body = self.request("GET", "playlists", params={"playlistType": "audio"})
        holder = body.get("MediaContainer") if isinstance(body, dict) else {}
        rows = holder.get("Metadata") if isinstance(holder, dict) else []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or _text(row.get("playlistType")) != "audio":
                continue
            playlist_id = _text(row.get("ratingKey"))
            name = _text(row.get("title"))
            if not playlist_id or not name:
                continue
            track_ids: List[str] = []
            try:
                items = self.request("GET", f"playlists/{quote(str(playlist_id), safe='')}/items")
                for entry in self._metadata(items):
                    if _text(entry.get("ratingKey")):
                        track_ids.append(_text(entry.get("ratingKey")))
            except Exception as exc:
                logger.warning("[Music] Plex playlist %s listing failed: %s", playlist_id, exc)
            playlists.append(
                {
                    "id": f"plex_playlist:{playlist_id}",
                    "name": name,
                    "description": _text(row.get("summary")),
                    "track_ids": track_ids,
                }
            )
            if len(playlists) >= 200:
                break
        return playlists

    def stream_url(self, track: Dict[str, Any], *, audio_sync: bool = False) -> str:
        del audio_sync  # Original containers decode natively; offsets stay aligned.
        part_key = _text(track.get("stream_path")) or _text(track.get("provider_track_id"))
        if not part_key:
            return _text(track.get("stream_url"))
        kind = f"plex:{self.stream_scope}" if self.stream_scope else "plex"
        return _stream_proxy_url(kind, part_key)

    def artwork_url(self, track: Dict[str, Any]) -> str:
        art_path = _text(track.get("artwork_path"))
        if not art_path:
            return ""
        kind = f"plex_art:{self.stream_scope}" if self.stream_scope else "plex_art"
        return _stream_proxy_url(kind, art_path)

    def proxy_request(
        self,
        item_id: str,
        *,
        art: bool = False,
        sync: bool = False,
    ) -> "tuple[str, Dict[str, str]]":
        """Build (url, headers) for one proxied Plex stream or artwork request.

        item_id is the Plex library path for the part or thumb; the token is
        attached server-side so it never reaches a playback target.
        """
        item_id = _text(item_id)
        if not item_id.startswith("/"):
            item_id = f"/{item_id}"
        separator = "&" if "?" in item_id else "?"
        url = f"{self.server_url}{item_id}{separator}X-Plex-Token={quote(self.token, safe='')}"
        return url, {"Accept": "*/*"}

    def _auth_cache_identity(self) -> str:
        return self.user_id or "plex"

    def _auth_cache_key(self) -> str:
        return _provider_auth_cache_key("plex", self.server_url, self._auth_cache_identity())

    def _save_cached_auth(self, token: str, user_id: str, client: Any = None) -> None:
        store = client or globals().get("redis_client")
        if store is None:
            return
        _save_hash(
            store,
            self._auth_cache_key(),
            {"access_token": token, "user_id": user_id, "authenticated_at": time.time()},
        )

    def clear_cached_auth(self, client: Any = None) -> None:
        store = client or globals().get("redis_client")
        if store is not None:
            try:
                store.delete(self._auth_cache_key())
            except Exception:
                pass


def _plex_track_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """One Plex track (JSON) as a generic provider row (for _normalize_track)."""
    media_rows = row.get("Media") if isinstance(row.get("Media"), list) else []
    media = media_rows[0] if media_rows and isinstance(media_rows[0], dict) else {}
    part_rows = media.get("Part") if isinstance(media.get("Part"), list) else []
    part = part_rows[0] if part_rows and isinstance(part_rows[0], dict) else {}
    container = _text(media.get("container")).casefold() or (
        Path(_text(part.get("file"))).suffix.lstrip(".").casefold()
    )
    # Duration arrives in milliseconds; a >27h "song" is a ms value slipping through.
    duration = _as_float(row.get("duration"))
    if duration > 100000:
        duration /= 1000.0
    thumb = _text(row.get("thumb")) or _text(row.get("parentThumb"))
    return {
        "id": _text(row.get("ratingKey")),
        "provider_track_id": _text(row.get("ratingKey")),
        "stream_path": _text(part.get("key")),
        "title": _text(row.get("title")),
        "artist": _text(row.get("grandparentTitle")),
        "album_artist": _text(row.get("originalTitle")) or _text(row.get("grandparentTitle")),
        "album": _text(row.get("parentTitle")),
        "albumId": _text(row.get("parentRatingKey")),
        "genres": [row.get("genre")] if _text(row.get("genre")) else [],
        "year": _text(row.get("year")),
        "track_number": row.get("index"),
        "duration_seconds": max(0.0, duration),
        "container": container,
        "artwork_path": thumb,
        "path": _text(part.get("file")) or _text(part.get("key")),
        "provider": "plex",
    }


# --------------------------------------------------------------------------
# Provider field-spec registry — one table drives every provider form.
#
# The global source cards, a Person's primary-source fields, their second
# source's fields, the value dicts saved on the link, provider instantiation,
# and the connect/disconnect/test flows all read these specs. Adding a provider
# means one class plus one spec entry, not edits across every form branch.
# --------------------------------------------------------------------------


class ProviderFieldSpec:
    """Form + value plumbing for one catalog provider.

    Subclasses supply `provider_id`, the link-field table, and `build_provider`;
    `connect` (global card flow) and `test_link_form` (Person link test) default
    to sensible implementations where the shapes line up.
    """

    provider_id = ""
    # Token used in form field keys and settings keys ("share", not
    # "network_share", keeps the share provider's long-standing keys stable).
    form_token = ""
    # Link-value field table: dicts with key/label/kind/placeholder/description.
    # `kind` is "text" or "password"; password fields keep their saved value
    # when a form submits them blank.
    link_fields: Tuple[Dict[str, str], ...] = ()
    primary_option_label = ""
    extra_option_label = ""

    def form_key(self, prefix: str, key: str) -> str:
        return f"{prefix}_{self.form_token}_{key}"

    # -- form generation -----------------------------------------------------

    def person_fields(
        self,
        values: Dict[str, Any],
        *,
        prefix: str,
        label_prefix: str = "",
        with_descriptions: bool = True,
    ) -> List[Dict[str, Any]]:
        """One provider's link form fields (primary source or second source)."""
        rows: List[Dict[str, Any]] = []
        for spec in self.link_fields:
            key = _text(spec.get("key"))
            kind = _text(spec.get("kind")) or "text"
            row: Dict[str, Any] = {
                "key": self.form_key(prefix, key),
                "label": f"{label_prefix}{PROVIDER_LABELS[self.provider_id]} {spec.get('label', '')}".strip(),
                "type": kind,
                "value": "" if kind == "password" else _text(values.get(key)),
            }
            placeholder = _text(spec.get("placeholder"))
            if placeholder:
                row["placeholder"] = placeholder
            if with_descriptions:
                description = _text(spec.get("description"))
                if description:
                    row["description"] = description
            rows.append(row)
        return rows

    def source_options(self, kind: str) -> List[Dict[str, str]]:
        """The picker rows this provider offers (empty when not selectable)."""
        label = self.primary_option_label if kind == "primary" else self.extra_option_label
        return [{"value": self.provider_id, "label": label}] if label else []

    def connection_detail(self, cfg: Dict[str, Any]) -> str:
        return _text(cfg.get(f"{self.provider_id}_server_url")) or (
            f"Point this core at your {PROVIDER_LABELS[self.provider_id]} server to begin."
        )

    # -- value + provider building -------------------------------------------

    def build_values(
        self,
        prefix: str,
        values: Dict[str, Any],
        existing: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """One link value dict from the form values under `prefix`.

        Password fields keep their saved value when submitted blank; fields the
        form omitted entirely keep whatever was stored (the resolved caches,
        e.g. a Plex user uuid, survive a re-save).
        """
        existing = existing if isinstance(existing, dict) else {}
        link: Dict[str, Any] = {}
        for spec in self.link_fields:
            key = _text(spec.get("key"))
            kind = _text(spec.get("kind")) or "text"
            form_key = self.form_key(prefix, key)
            if form_key in values:
                if kind == "password":
                    link[key] = _text(values.get(form_key)) or _text(existing.get(key))
                else:
                    link[key] = _text(values.get(form_key))
            elif key in existing:
                link[key] = _text(existing.get(key))
        return link

    def build_provider(
        self, values: Dict[str, Any], stream_scope: str = ""
    ) -> Any:
        raise NotImplementedError

    def connect(self, values: Dict[str, Any], cfg: Dict[str, Any], store: Any) -> Dict[str, Any]:
        raise NotImplementedError

    def disconnect_keys(self) -> Tuple[str, ...]:
        """Settings keys removed when this provider is disconnected."""
        return tuple(f"{self.form_token}_{spec['key']}" for spec in self.link_fields)


class _EmbyStyleProviderFieldSpec(ProviderFieldSpec):
    """Shared plumbing for the Emby-fork family (Emby, Jellyfin).

    Same link shape (server URL, username/password sign-in or an API key, an
    optional user id, optional library name/folder), with the auth mode derived
    from the submitted values exactly as the link form always did.
    """

    def build_values(
        self,
        prefix: str,
        values: Dict[str, Any],
        existing: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        link = super().build_values(prefix, values, existing)
        link["auth_mode"] = (
            "api_key"
            if _text(link.get("api_key")) and not _text(link.get("username"))
            else "user_token"
        )
        return link

    def build_provider(self, values: Dict[str, Any], stream_scope: str = "") -> Any:
        provider_class = _provider_class(self.provider_id)
        if provider_class is None:
            raise ValueError(f"{PROVIDER_LABELS.get(self.provider_id, self.provider_id)} support is not enabled in this build.")
        return provider_class(
            server_url=_normalize_server_url(values.get("server_url")),
            auth_mode="api_key"
            if _text(values.get("auth_mode")).casefold() == "api_key"
            else "user_token",
            username=_text(values.get("username")),
            password=_text(values.get("password")),
            api_key=_text(values.get("api_key")),
            user_id=_text(values.get("user_id")),
            library_name=_text(values.get("library_name")),
            library_folder=_text(values.get("library_folder")),
            stream_scope=stream_scope,
        )

    def global_fields(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        label = PROVIDER_LABELS[self.provider_id]
        settings_key = f"{self.provider_id}_server_url"
        auth_mode = _text(cfg.get(f"{self.provider_id}_auth_mode")).casefold() or "user_token"
        return [
            {
                "key": f"{self.provider_id}_server_url",
                "label": f"{label} Server URL",
                "type": "text",
                "required": True,
                "value": _text(cfg.get(f"{self.provider_id}_server_url"))
                or (_text(cfg.get("server_url")) if self.provider_id == "emby" else ""),
                "placeholder": "http://emby.local:8096"
                if self.provider_id == "emby"
                else "http://jellyfin.local:8096",
            },
            {
                "key": f"{self.provider_id}_auth_mode",
                "label": "Sign-In Style",
                "type": "select",
                "value": auth_mode,
                "options": [
                    {"value": "user_token", "label": f"{label} username & password"},
                    {"value": "api_key", "label": "Server API key"},
                ],
                "description": (
                    f"Username sign-in streams through Tater's built-in proxy and honors each "
                    f"{label} user's library access. An API key streams directly from the server "
                    f"and needs the User ID below."
                ),
            },
            {
                "key": f"{self.provider_id}_username",
                "label": f"{label} Username",
                "type": "text",
                "value": _text(cfg.get(f"{self.provider_id}_username")),
            },
            {
                "key": f"{self.provider_id}_password",
                "label": f"{label} Password",
                "type": "password",
                "value": "",
                "description": "Used for username sign-in. Leave blank to keep the saved password.",
            },
            {
                "key": f"{self.provider_id}_api_key",
                "label": f"{label} API Key",
                "type": "password",
                "value": _text(cfg.get(f"{self.provider_id}_api_key")),
                "description": (
                    f"Server API key from the {label} dashboard's security settings, for API-key sign-in."
                ),
            },
            {
                "key": f"{self.provider_id}_user_id",
                "label": f"{label} User ID (optional)",
                "type": "text",
                "value": _text(cfg.get(f"{self.provider_id}_user_id")),
                "description": (
                    "Required for API-key sign-in when the server has more than one user."
                ),
            },
            {
                "key": f"{self.provider_id}_library_name",
                "label": f"{label} Library Name (optional)",
                "type": "text",
                "value": _text(cfg.get(f"{self.provider_id}_library_name")),
                "description": (
                    "Only needed when this user can see several music libraries."
                ),
            },
            {
                "key": f"{self.provider_id}_library_folder",
                "label": f"{label} Library Folder (optional)",
                "type": "text",
                "value": _text(cfg.get(f"{self.provider_id}_library_folder")),
                "description": (
                    "Subfolder of the library to sync (name or full server path). Leave blank to "
                    "sync the whole library; useful for mixed-content libraries."
                ),
            },
        ]


_EMBY_LINK_FIELDS: Tuple[Dict[str, str], ...] = (
    {"key": "server_url", "label": "Server URL", "placeholder": "http://emby.local:8096"},
    {
        "key": "password",
        "label": "Password",
        "kind": "password",
        "description": "Leave blank to keep the saved password.",
    },
    {
        "key": "api_key",
        "label": "API Key",
        "kind": "password",
    },
    {"key": "username", "label": "Username"},
    {"key": "user_id", "label": "User ID (optional)"},
    {"key": "library_name", "label": "Library Name (optional)"},
    {
        "key": "library_folder",
        "label": "Library Folder (optional)",
        "placeholder": "Music",
        "description": (
            "Subfolder of the library to sync (name or full server path). Leave blank to sync "
            "the whole library; useful when one mixed-content library holds their music, TV, "
            "and movies."
        ),
    },
)


class _EmbyProviderFieldSpec(_EmbyStyleProviderFieldSpec):
    provider_id = "emby"
    form_token = "emby"
    link_fields = _EMBY_LINK_FIELDS
    primary_option_label = "Emby (own user/library)"
    extra_option_label = "Emby (a second account or library)"

    def connection_detail(self, cfg: Dict[str, Any]) -> str:
        return _text(
            cfg.get("emby_server_url") or cfg.get("server_url")
        ) or "Point this core at your Emby server to begin."

    def connect(self, values: Dict[str, Any], cfg: Dict[str, Any], store: Any) -> Dict[str, Any]:
        return _connect_emby_style(self.provider_id, values, store)

    def disconnect(self, store: Any) -> Dict[str, Any]:
        return _disconnect_emby_style(self.provider_id, store)

    def test_link_form(
        self, values: Dict[str, Any], existing: Dict[str, Any], store: Any
    ) -> str:
        return _test_emby_style_link_form(self.provider_id, values, existing, store)


class _JellyfinProviderFieldSpec(_EmbyStyleProviderFieldSpec):
    provider_id = "jellyfin"
    form_token = "jellyfin"
    link_fields = _EMBY_LINK_FIELDS
    primary_option_label = "Jellyfin (own user/library)"
    extra_option_label = "Jellyfin (a second account or library)"

    def connection_detail(self, cfg: Dict[str, Any]) -> str:
        return _text(cfg.get("jellyfin_server_url")) or (
            "Point this core at your Jellyfin server (http://<host>:8096) to begin."
        )

    def connect(self, values: Dict[str, Any], cfg: Dict[str, Any], store: Any) -> Dict[str, Any]:
        return _connect_emby_style(self.provider_id, values, store)

    def disconnect(self, store: Any) -> Dict[str, Any]:
        return _disconnect_emby_style(self.provider_id, store)

    def test_link_form(
        self, values: Dict[str, Any], existing: Dict[str, Any], store: Any
    ) -> str:
        return _test_emby_style_link_form(self.provider_id, values, existing, store)


class _SubsonicProviderFieldSpec(ProviderFieldSpec):
    provider_id = "subsonic"
    form_token = "subsonic"
    link_fields: Tuple[Dict[str, str], ...] = (
        {
            "key": "server_url",
            "label": "Server URL",
            "placeholder": "https://music.example.com",
            "description": "Base URL of the Subsonic-compatible server (Navidrome, Airsonic, Gonic, …).",
        },
        {"key": "username", "label": "Username"},
        {
            "key": "password",
            "label": "Password",
            "kind": "password",
            "description": "Leave blank to keep the saved password.",
        },
    )
    primary_option_label = "Subsonic (own server account)"
    extra_option_label = "Subsonic (a second server account)"

    def build_provider(self, values: Dict[str, Any], stream_scope: str = "") -> Any:
        return SubsonicMusicProvider(
            server_url=_normalize_server_url(values.get("server_url")),
            username=_text(values.get("username")),
            password=_text(values.get("password")),
            stream_scope=stream_scope,
        )

    def global_fields(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {
                "key": "subsonic_server_url",
                "label": "Subsonic Server URL",
                "type": "text",
                "required": True,
                "value": _text(cfg.get("subsonic_server_url")),
                "placeholder": "https://music.example.com",
                "description": (
                    "Base URL of a Subsonic-compatible server — Navidrome, Airsonic, Gonic, "
                    "Ampache with the Subsonic API, and friends."
                ),
            },
            {
                "key": "subsonic_username",
                "label": "Subsonic Username",
                "type": "text",
                "value": _text(cfg.get("subsonic_username")),
            },
            {
                "key": "subsonic_password",
                "label": "Subsonic Password",
                "type": "password",
                "value": "",
                "description": "Leave blank to keep the saved password.",
            },
        ]

    def connection_detail(self, cfg: Dict[str, Any]) -> str:
        return _text(cfg.get("subsonic_server_url")) or (
            "Point this core at your Subsonic-compatible server to begin."
        )

    def connect(self, values: Dict[str, Any], cfg: Dict[str, Any], store: Any) -> Dict[str, Any]:
        return _connect_subsonic(values, store)

    def disconnect(self, store: Any) -> Dict[str, Any]:
        return _disconnect_generic(self.provider_id, store)

    def test_link_form(
        self, values: Dict[str, Any], existing: Dict[str, Any], store: Any
    ) -> str:
        return _test_subsonic_link_form(values, existing, store)


class _PlexProviderFieldSpec(ProviderFieldSpec):
    provider_id = "plex"
    form_token = "plex"
    link_fields: Tuple[Dict[str, str], ...] = (
        {
            "key": "auth_mode",
            "label": "Sign-In Style",
            "kind": "select",
            "description": (
                "Home user: the owner signs in and this link uses one named Plex Home member. "
                "Own account: signs in with this account's own plex.tv username and password. "
                "Manual token: paste a server token directly."
            ),
        },
        {
            "key": "username",
            "label": "Username",
            "description": "plex.tv username for Home-user or own-account sign-in.",
        },
        {
            "key": "password",
            "label": "Password",
            "kind": "password",
            "description": "Leave blank to keep the saved password.",
        },
        {
            "key": "home_user",
            "label": "Plex Home User",
            "description": "The Home member's name to play as (Home-user sign-in only).",
        },
        {
            "key": "home_user_pin",
            "label": "Plex Home User PIN",
            "kind": "password",
            "description": "Only needed when that Home member is PIN-protected. Leave blank to keep the saved PIN.",
        },
        {
            "key": "server_url",
            "label": "Server URL",
            "placeholder": "http://plex.local:32400",
            "description": "Leave blank and plex.tv discovery fills in the server this account can reach.",
        },
        {
            "key": "token",
            "label": "Plex Token",
            "kind": "password",
            "description": "Pasted token for manual sign-in, or the resolved token. Leave blank to keep the saved token.",
        },
    )
    primary_option_label = "Plex (own account)"
    extra_option_label = "Plex (a second account)"

    def person_fields(
        self,
        values: Dict[str, Any],
        *,
        prefix: str,
        label_prefix: str = "",
        with_descriptions: bool = True,
    ) -> List[Dict[str, Any]]:
        rows = super().person_fields(
            values, prefix=prefix, label_prefix=label_prefix, with_descriptions=with_descriptions
        )
        for row in rows:
            if row["key"] == self.form_key(prefix, "auth_mode"):
                row["options"] = _plex_auth_mode_options()
                row["value"] = (
                    _text(values.get("auth_mode")).casefold() or DEFAULT_PLEX_AUTH_MODE
                )
        return rows

    def build_values(
        self,
        prefix: str,
        values: Dict[str, Any],
        existing: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        existing = existing if isinstance(existing, dict) else {}
        link = super().build_values(prefix, values, existing)
        mode = _text(link.get("auth_mode")).casefold()
        if mode not in PLEX_AUTH_MODES:
            mode = _text(existing.get("auth_mode")).casefold()
        link["auth_mode"] = mode if mode in PLEX_AUTH_MODES else DEFAULT_PLEX_AUTH_MODE
        return link

    def build_provider(self, values: Dict[str, Any], stream_scope: str = "") -> Any:
        return PlexMusicProvider.from_link_values(values, stream_scope)

    def global_fields(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        mode = _text(cfg.get("plex_auth_mode")).casefold() or DEFAULT_PLEX_AUTH_MODE
        return [
            {
                "key": "plex_auth_mode",
                "label": "Sign-In Style",
                "type": "select",
                "value": mode if mode in PLEX_AUTH_MODES else DEFAULT_PLEX_AUTH_MODE,
                "options": _plex_auth_mode_options(),
                "description": (
                    "Home user: the owner signs in and picks one Plex Home member. Own account: "
                    "signs in with that account's own plex.tv username and password. Manual "
                    "token: paste a server token directly."
                ),
            },
            {
                "key": "plex_username",
                "label": "Plex Username",
                "type": "text",
                "value": _text(cfg.get("plex_username")),
            },
            {
                "key": "plex_password",
                "label": "Plex Password",
                "type": "password",
                "value": "",
                "description": "Used for Home-user and own-account sign-in. Leave blank to keep the saved password.",
            },
            {
                "key": "plex_home_user",
                "label": "Plex Home User",
                "type": "text",
                "value": _text(cfg.get("plex_home_user")),
                "description": "The Plex Home member's name to play as (Home-user sign-in only).",
            },
            {
                "key": "plex_home_user_pin",
                "label": "Plex Home User PIN",
                "type": "password",
                "value": "",
                "description": "Only when that Home member is PIN-protected. Leave blank to keep the saved PIN.",
            },
            {
                "key": "plex_server_url",
                "label": "Plex Server URL",
                "type": "text",
                "value": _text(cfg.get("plex_server_url")),
                "placeholder": "http://plex.local:32400",
                "description": (
                    "Leave blank and plex.tv discovery pre-fills the server this account can "
                    "reach; a manual entry always wins."
                ),
            },
            {
                "key": "plex_token",
                "label": "Plex Token",
                "type": "password",
                "value": _text(cfg.get("plex_token")),
                "description": (
                    "Pasted X-Plex-Token for manual sign-in. Other modes resolve and store one automatically."
                ),
            },
        ]

    def connection_detail(self, cfg: Dict[str, Any]) -> str:
        if _text(cfg.get("plex_server_url")):
            return _text(cfg.get("plex_server_url"))
        return "Sign in to plex.tv (or paste a token) and the server fills in from discovery."

    def connect(self, values: Dict[str, Any], cfg: Dict[str, Any], store: Any) -> Dict[str, Any]:
        return _connect_plex(values, store)

    def disconnect(self, store: Any) -> Dict[str, Any]:
        return _disconnect_generic(self.provider_id, store)

    def test_link_form(
        self, values: Dict[str, Any], existing: Dict[str, Any], store: Any
    ) -> str:
        return _test_plex_link_form(values, existing, store)


class _ShareProviderFieldSpec(ProviderFieldSpec):
    provider_id = "network_share"
    form_token = "share"
    link_fields: Tuple[Dict[str, str], ...] = (
        {
            "key": "root_path",
            "label": "Mounted Share Folder",
            "placeholder": "/mnt/music/<person>",
            "description": (
                "Folder path of this Person's own share subfolder as mounted on the Tater host."
            ),
        },
    )
    primary_option_label = "Network share (own folder)"
    extra_option_label = "Network share (a second folder)"

    def build_provider(self, values: Dict[str, Any], stream_scope: str = "") -> Any:
        del stream_scope  # Share streams scope themselves via their root path.
        return NetworkShareMusicProvider(root_path=_text(values.get("root_path")))

    def global_fields(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            {
                "key": "share_root_path",
                "label": "Mounted Share Folder",
                "type": "text",
                "required": True,
                "value": _text(cfg.get("share_root_path")),
                "placeholder": "/mnt/music",
                "description": (
                    "Folder path of the SMB/CIFS or NFS share as mounted on the Tater host "
                    "(for Docker, add the share as a bind-mount volume and use its container path)."
                ),
            }
        ]

    def connection_detail(self, cfg: Dict[str, Any]) -> str:
        return _text(cfg.get("share_root_path")) or (
            "Mount the SMB/NFS share on the Tater host, then point this core at the mounted folder."
        )

    def connect(self, values: Dict[str, Any], cfg: Dict[str, Any], store: Any) -> Dict[str, Any]:
        root_path = _text(values.get("share_root_path")) or _text(cfg.get("share_root_path"))
        provider = NetworkShareMusicProvider(root_path=root_path)
        if not root_path:
            raise ValueError("Enter the mounted network-share folder path first.")
        if not provider.connected:
            raise ValueError(
                "That folder path is not readable from Tater. Mount the SMB/NFS share on the host "
                "(or bind-mount it into the container) and try again."
            )
        _save_hash(store, SETTINGS_KEY, {"share_root_path": root_path, "provider": self.provider_id})
        catalog = _sync_catalog(store, self.provider_id)
        return {
            "ok": True,
            "message": (
                f"{PROVIDER_LABELS[self.provider_id]} connected and loaded "
                f"{len(catalog.get('tracks') or [])} tracks."
            ),
        }

    def disconnect(self, store: Any) -> Dict[str, Any]:
        return _disconnect_share(store)

    def test_link_form(
        self, values: Dict[str, Any], existing: Dict[str, Any], store: Any
    ) -> str:
        raise ValueError("Network shares are checked when the link is saved.")


PROVIDER_FIELD_SPECS: Dict[str, ProviderFieldSpec] = {
    "emby": _EmbyProviderFieldSpec(),
    "jellyfin": _JellyfinProviderFieldSpec(),
    "subsonic": _SubsonicProviderFieldSpec(),
    "plex": _PlexProviderFieldSpec(),
    "network_share": _ShareProviderFieldSpec(),
}

# Every provider form field (primary and second source), remembered across the
# link test's tab refetch.
PERSON_LINK_TEST_FIELD_KEYS = tuple(
    [
        *_PERSON_LINK_TEST_CORE_FIELD_KEYS,
        *(
            PROVIDER_FIELD_SPECS[provider_id].form_key(prefix, _text(field.get("key")))
            for provider_id in CATALOG_PROVIDER_ORDER
            for prefix in ("person_link", "person_link_extra")
            for field in PROVIDER_FIELD_SPECS[provider_id].link_fields
        ),
    ]
)


def _plex_auth_mode_options() -> List[Dict[str, str]]:
    return [
        {
            "value": "home_user",
            "label": "Plex Home member (owner signs in)",
        },
        {
            "value": "own_account",
            "label": "Their own plex.tv account",
        },
        {
            "value": "manual_token",
            "label": "Manual token (paste it)",
        },
    ]


def _connect_emby_style(provider_id: str, values: Dict[str, Any], store: Any) -> Dict[str, Any]:
    """Global-card connect for the Emby-fork family (Emby and Jellyfin)."""
    cfg = _settings(store)
    spec = PROVIDER_FIELD_SPECS[provider_id]
    auth_mode = (
        "api_key"
        if _text(values.get(f"{provider_id}_auth_mode")).casefold() == "api_key"
        else "user_token"
    )
    server_url = _normalize_server_url(
        values.get(f"{provider_id}_server_url")
        or cfg.get(f"{provider_id}_server_url")
        or (cfg.get("server_url") if provider_id == "emby" else "")
    )
    if not server_url:
        raise ValueError(f"Enter the {PROVIDER_LABELS[provider_id]} server URL first.")
    username = _text(values.get(f"{provider_id}_username") or cfg.get(f"{provider_id}_username"))
    password = _text(values.get(f"{provider_id}_password")) or _text(cfg.get(f"{provider_id}_password"))
    api_key = _text(values.get(f"{provider_id}_api_key")) or _text(cfg.get(f"{provider_id}_api_key"))
    user_id = _text(values.get(f"{provider_id}_user_id") or cfg.get(f"{provider_id}_user_id"))
    library_name = _text(values.get(f"{provider_id}_library_name") or cfg.get(f"{provider_id}_library_name"))
    library_folder = _text(values.get(f"{provider_id}_library_folder") or cfg.get(f"{provider_id}_library_folder"))
    provider = spec.build_provider(
        {
            "server_url": server_url,
            "auth_mode": auth_mode,
            "username": username,
            "password": password,
            "api_key": api_key,
            "user_id": user_id,
            "library_name": library_name,
            "library_folder": library_folder,
        }
    )
    if not provider.connected:
        raise ValueError(
            f"Enter the {PROVIDER_LABELS[provider_id]} server URL plus a username and password, "
            "or an API key."
        )
    provider.clear_cached_auth(store)
    resolved_user_id = user_id
    try:
        if auth_mode == "user_token":
            _token, resolved_user_id = provider.authenticate(force=True, client=store)
        else:
            resolved_user_id = provider.resolve_user_id(store)
    except PermissionError as exc:
        raise ValueError(f"{PROVIDER_LABELS[provider_id]} rejected the credentials: {exc}") from exc
    updates = {
        f"{provider_id}_server_url": server_url,
        f"{provider_id}_auth_mode": auth_mode,
        f"{provider_id}_username": username,
        f"{provider_id}_password": password,
        f"{provider_id}_api_key": api_key,
        f"{provider_id}_user_id": resolved_user_id,
        f"{provider_id}_library_name": library_name,
        f"{provider_id}_library_folder": library_folder,
        "provider": provider_id,
    }
    if provider_id == "emby":
        # Legacy key kept in sync for older readers.
        updates["server_url"] = server_url
    _save_hash(store, SETTINGS_KEY, updates)
    catalog = _sync_catalog(store, provider_id)
    return {
        "ok": True,
        "message": (
            f"{PROVIDER_LABELS[provider_id]} connected and loaded "
            f"{len(catalog.get('tracks') or [])} tracks."
        ),
    }


def _disconnect_emby_style(provider_id: str, store: Any) -> Dict[str, Any]:
    return _disconnect_generic(provider_id, store)


def _disconnect_generic(provider_id: str, store: Any) -> Dict[str, Any]:
    spec = PROVIDER_FIELD_SPECS.get(provider_id)
    if spec is None:
        raise ValueError(f"{PROVIDER_LABELS.get(provider_id, provider_id)} support is not enabled in this build.")
    fields = list(spec.disconnect_keys())
    if provider_id == "emby":
        fields.append("server_url")
    player = _player(store)
    if _provider_id(player.get("provider")) == provider_id:
        _stop_player(store=store)
    if store is not None:
        store.hdel(SETTINGS_KEY, *fields, "provider")
        try:
            # Legacy shared cache key plus any per-identity token caches.
            keys_to_delete = [EMBY_AUTH_CACHE_KEY] if provider_id == "emby" else []
            if hasattr(store, "keys"):
                try:
                    keys_to_delete.extend(
                        key for key in store.keys(f"personal_music_core:{provider_id}:auth:*") or []
                        if isinstance(key, str)
                    )
                except Exception:
                    pass
            if keys_to_delete:
                store.delete(*keys_to_delete)
        except Exception:
            pass
        cached = _load_json(store, CATALOG_KEY, {})
        if _provider_id(cached.get("provider")) == provider_id:
            store.delete(CATALOG_KEY)
            with _catalog_memory_cache_lock:
                _catalog_memory_cache.update(
                    {"store": store, "loaded_at": time.monotonic(), "payload": {}}
                )
    _save_hash(store, RUNTIME_KEY, {"status": "disconnected", "last_error": ""})
    return {"ok": True, "message": f"{PROVIDER_LABELS[provider_id]} disconnected locally."}


def _disconnect_share(store: Any) -> Dict[str, Any]:
    player = _player(store)
    if _provider_id(player.get("provider")) == "network_share":
        _stop_player(store=store)
    if store is not None:
        store.hdel(SETTINGS_KEY, "share_root_path", "provider")
        try:
            store.delete(SHARE_ART_INDEX_KEY)
            try:
                os.rmdir(_share_art_cache_dir())
            except OSError:
                pass
        except Exception:
            pass
        cached = _load_json(store, CATALOG_KEY, {})
        if _provider_id(cached.get("provider")) == "network_share":
            store.delete(CATALOG_KEY)
            with _catalog_memory_cache_lock:
                _catalog_memory_cache.update(
                    {"store": store, "loaded_at": time.monotonic(), "payload": {}}
                )
    _save_hash(store, RUNTIME_KEY, {"status": "disconnected", "last_error": ""})
    return {"ok": True, "message": f"{PROVIDER_LABELS['network_share']} disconnected locally."}


def _connect_subsonic(values: Dict[str, Any], store: Any) -> Dict[str, Any]:
    cfg = _settings(store)
    server_url = _normalize_server_url(
        values.get("subsonic_server_url") or cfg.get("subsonic_server_url")
    )
    username = _text(values.get("subsonic_username") or cfg.get("subsonic_username"))
    password = _text(values.get("subsonic_password")) or _text(cfg.get("subsonic_password"))
    provider = SubsonicMusicProvider(
        server_url=server_url,
        username=username,
        password=password,
    )
    if not provider.connected:
        raise ValueError("Enter the Subsonic server URL plus a username and password.")
    try:
        provider.ping()
    except PermissionError as exc:
        raise ValueError(f"Subsonic rejected the credentials: {exc}") from exc
    except RuntimeError as exc:
        raise ValueError(f"Could not reach the Subsonic server: {exc}") from exc
    _save_hash(
        store,
        SETTINGS_KEY,
        {
            "subsonic_server_url": server_url,
            "subsonic_username": username,
            "subsonic_password": password,
            "provider": "subsonic",
        },
    )
    catalog = _sync_catalog(store, "subsonic")
    return {
        "ok": True,
        "message": (
            f"{PROVIDER_LABELS['subsonic']} connected and loaded "
            f"{len(catalog.get('tracks') or [])} tracks."
        ),
    }


def _plex_resolve_credentials(
    values: Dict[str, Any],
    existing: Dict[str, Any],
    *,
    store: Any = None,
) -> Dict[str, Any]:
    """Resolve (server URL, token, user id) for one Plex configuration.

    Three auth modes converge on that one state: `home_user` signs in as the
    owner and switches into the named Home member (PIN form field when that
    member is protected); `own_account` signs in with that account's own
    plex.tv credentials (shared users cannot be switched into by the owner —
    the plex.tv boundary); `manual_token` pastes a token plus server URL.
    Server discovery pre-fills the URL; a manual entry always wins.
    """
    existing = existing if isinstance(existing, dict) else {}
    mode = (
        _text(values.get("plex_auth_mode")).casefold()
        or _text(existing.get("auth_mode")).casefold()
        or DEFAULT_PLEX_AUTH_MODE
    )
    if mode not in PLEX_AUTH_MODES:
        mode = DEFAULT_PLEX_AUTH_MODE
    username = _text(values.get("plex_username")) or _text(existing.get("username"))
    password = _text(values.get("plex_password")) or _text(existing.get("password"))
    home_user = _text(values.get("plex_home_user")) or _text(existing.get("home_user"))
    home_pin = _text(values.get("plex_home_user_pin")) or _text(existing.get("home_user_pin"))
    server_url = _normalize_server_url(
        values.get("plex_server_url") or existing.get("server_url")
    )
    pasted_token = _text(values.get("plex_token")) or _text(existing.get("token"))

    if mode == "manual_token":
        if not pasted_token:
            raise ValueError("Paste the Plex token to connect with.")
        if not server_url:
            raise ValueError("Enter the Plex server URL to connect with.")
        return {
            "auth_mode": mode,
            "username": username,
            "password": password,
            "home_user": home_user,
            "home_user_pin": home_pin,
            "server_url": server_url,
            "token": pasted_token,
            "user_id": _text(existing.get("user_id")),
            "detail": "the pasted token",
        }

    if not username or not password:
        raise ValueError(
            "Enter the Plex username and password to sign in with, or switch to a manual token."
        )
    try:
        owner_token = _plex_tv_signin(username, password)
        if mode == "home_user":
            wanted = home_user.casefold()
            members = _plex_tv_home_users(owner_token)
            match = next(
                (member for member in members if member["title"].casefold() == wanted),
                None,
            )
            if match is None:
                available = ", ".join(member["title"] for member in members) or "none listed"
                raise ValueError(
                    f"No Plex Home member named '{home_user}' (visible: {available})."
                )
            user_token = _plex_tv_switch_home_user(owner_token, match["uuid"], home_pin)
            user_id = match["uuid"]
            detail = f"switched into the {match['title']} Home member"
        else:
            user_token = owner_token
            user_id = _plex_tv_user_id(owner_token)
            detail = f"{username} signed in to plex.tv"
        if not server_url:
            servers = _plex_tv_resources(owner_token)
            owned = [server for server in servers if server.get("owned")]
            candidates = owned or servers
            for server in candidates:
                for uri in server.get("connections") or []:
                    try:
                        server_url = _normalize_server_url(uri)
                        break
                    except ValueError:
                        continue
                if server_url:
                    break
            if not server_url:
                raise ValueError(
                    "plex.tv discovery found no reachable server; enter the Plex server URL manually."
                )
    except PermissionError as exc:
        raise ValueError(f"Plex rejected the credentials: {exc}") from exc
    except RuntimeError as exc:
        raise ValueError(f"Could not reach plex.tv: {exc}") from exc
    return {
        "auth_mode": mode,
        "username": username,
        "password": password,
        "home_user": home_user,
        "home_user_pin": home_pin,
        "server_url": server_url,
        "token": user_token,
        "user_id": user_id,
        "detail": f"{detail}; server {server_url} from discovery",
    }


def _plex_resolve_link_values(
    link_values: Dict[str, Any],
    existing: Dict[str, Any],
    *,
    store: Any = None,
) -> Dict[str, Any]:
    """Fill in a Plex link value dict's resolved token (per its auth mode)."""
    if _text(link_values.get("token")):
        return link_values
    built = {
        "plex_auth_mode": link_values.get("auth_mode"),
        "plex_username": link_values.get("username"),
        "plex_password": link_values.get("password"),
        "plex_home_user": link_values.get("home_user"),
        "plex_home_user_pin": link_values.get("home_user_pin"),
        "plex_server_url": link_values.get("server_url"),
        "plex_token": "",
    }
    resolved = _plex_resolve_credentials(built, existing, store=store)
    merged = dict(link_values)
    merged["token"] = resolved["token"]
    merged["user_id"] = resolved.get("user_id", "")
    merged["server_url"] = resolved["server_url"]
    return merged


def _connect_plex(values: Dict[str, Any], store: Any) -> Dict[str, Any]:
    cfg = _settings(store)
    resolved = _plex_resolve_credentials(values, cfg, store=store)
    provider = PlexMusicProvider(
        server_url=resolved["server_url"],
        token=resolved["token"],
        user_id=resolved.get("user_id", ""),
    )
    provider._save_cached_auth(resolved["token"], resolved.get("user_id", ""), store)
    _save_hash(
        store,
        SETTINGS_KEY,
        {
            "plex_auth_mode": resolved["auth_mode"],
            "plex_username": resolved["username"],
            "plex_password": resolved["password"],
            "plex_home_user": resolved["home_user"],
            "plex_home_user_pin": resolved["home_user_pin"],
            "plex_server_url": resolved["server_url"],
            "plex_token": resolved["token"],
            "plex_user_id": resolved.get("user_id", ""),
            "provider": "plex",
        },
    )
    catalog = _sync_catalog(store, "plex")
    return {
        "ok": True,
        "message": (
            f"{PROVIDER_LABELS['plex']} connected and loaded "
            f"{len(catalog.get('tracks') or [])} tracks."
        ),
    }


def _test_emby_style_link_form(
    provider_id: str,
    values: Dict[str, Any],
    existing: Dict[str, Any],
    store: Any,
) -> str:
    """Test one Person's Emby/Jellyfin link values without saving them."""
    label = PROVIDER_LABELS[provider_id]
    built = PROVIDER_FIELD_SPECS[provider_id].build_values("person_link", values, existing)
    if not _normalize_server_url(built.get("server_url")):
        raise ValueError(f"Enter the {label} server URL to test.")
    provider = PROVIDER_FIELD_SPECS[provider_id].build_provider(built)
    if not provider.connected:
        raise ValueError(f"Enter a {label} server URL plus a username and password, or an API key.")
    try:
        if _text(built.get("auth_mode")) == "api_key":
            provider.resolve_user_id(client=store)
            detail = f"the API key works against {built.get('server_url')}"
        else:
            provider.authenticate(force=True, client=store)
            detail = f"{built.get('username')} signed in to {built.get('server_url')}"
        # Also resolve the library (and its configured subfolder) so a wrong
        # Library Name or Library Folder surfaces in the test, not at sync.
        view = provider.music_view(client=store)
        folder = provider.music_folder(view, client=store)
        folder_name = _text(folder.get("Name"))
        if folder_name and _text(folder.get("Id")) != _text(view.get("Id")):
            detail += f", scoped to the {folder_name} folder"
    except PermissionError as exc:
        raise ValueError(f"{label} rejected the credentials: {_text(exc)}") from exc
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Could not reach {label}: {_text(exc)}") from exc
    return f"{label} connection works — {detail}."


def _test_subsonic_link_form(
    values: Dict[str, Any],
    existing: Dict[str, Any],
    store: Any,
) -> str:
    """Test one Person's Subsonic link values without saving them."""
    built = PROVIDER_FIELD_SPECS["subsonic"].build_values("person_link", values, existing)
    provider = SubsonicMusicProvider(
        server_url=_normalize_server_url(built.get("server_url")),
        username=_text(built.get("username")),
        password=_text(built.get("password")),
    )
    if not provider.connected:
        raise ValueError("Enter the Subsonic server URL plus a username and password to test.")
    try:
        provider.ping()
    except PermissionError as exc:
        raise ValueError(f"Subsonic rejected the credentials: {exc}") from exc
    except RuntimeError as exc:
        raise ValueError(f"Could not reach the Subsonic server: {exc}") from exc
    return f"Subsonic connection works — {built.get('username')} reached {built.get('server_url')}."


def _test_plex_link_form(
    values: Dict[str, Any],
    existing: Dict[str, Any],
    store: Any,
) -> str:
    """Test one Person's Plex link values without saving them."""
    link_values = PROVIDER_FIELD_SPECS["plex"].build_values("person_link", values, existing)
    built = {
        "plex_auth_mode": link_values.get("auth_mode"),
        "plex_username": link_values.get("username"),
        "plex_password": link_values.get("password"),
        "plex_home_user": link_values.get("home_user"),
        "plex_home_user_pin": link_values.get("home_user_pin"),
        "plex_server_url": link_values.get("server_url"),
        "plex_token": link_values.get("token"),
    }
    resolved = _plex_resolve_credentials(built, {}, store=store)
    provider = PlexMusicProvider(
        server_url=resolved["server_url"],
        token=resolved["token"],
        user_id=resolved.get("user_id", ""),
    )
    try:
        provider.request("GET", "library/sections", timeout=15)
    except PermissionError as exc:
        raise ValueError(f"Plex rejected the token at {resolved['server_url']}: {exc}") from exc
    except RuntimeError as exc:
        raise ValueError(f"Could not reach the Plex server: {exc}") from exc
    provider._save_cached_auth(resolved["token"], resolved.get("user_id", ""), store)
    return f"Plex connection works — {resolved['detail']}."


# --------------------------------------------------------------------------
# Streaming providers (future)
#
# Catalog providers (Emby, network shares) index a library into this core's own
# catalog. A *streaming* provider (Spotify, Apple Music, Tidal, …) would instead
# surface recommendations and streams from its service, the way Music Assistant
# does. The scaffolding below is deliberately open-ended: Endless Playback's
# "Automatic" and "Similar to what you played" modes call
# _streaming_similar_tracks(), which fans out to every connected streaming
# provider. Today the registry is empty, so those modes fall back to the library
# and nothing here changes behaviour.
#
# To add one later:
#   1. Write a provider class that subclasses StreamingMusicProvider and
#      implements catalog()/stream_url()/artwork_url()/connected (mirroring
#      EmbyMusicProvider's surface) plus similar_tracks().
#   2. Register it in STREAMING_PROVIDER_CLASSES under a stable provider id.
#   3. Teach _provider_id() the new id, add it to PROVIDER_LABELS, and allow its
#      tracks through the queue paths (they already play whatever
#      track["provider"] names, because _play_track resolves the provider per
#      track).
# --------------------------------------------------------------------------


class StreamingMusicProvider:
    """Interface reference for future streaming providers (Spotify, …).

    Subclasses must provide `provider_id`, `from_settings`, `connected`, the
    catalog provider surface used by playback (catalog/stream_url/artwork_url),
    and `similar_tracks` for the provider-backed Endless Playback modes.
    """

    provider_id = ""

    @classmethod
    def from_settings(cls, settings: Dict[str, Any]) -> "StreamingMusicProvider":
        raise NotImplementedError("Streaming providers must implement from_settings().")

    @property
    def connected(self) -> bool:
        return False

    def similar_tracks(
        self,
        seed_tracks: List[Dict[str, Any]],
        *,
        count: int = CONTINUATION_BATCH_TRACKS,
    ) -> List[Dict[str, Any]]:
        """Tracks from this service similar to the seeds; [] when unsupported."""
        return []


# provider id -> provider class. Empty until a streaming provider is added;
# keep ids stable (they end up in history events and queue state).
STREAMING_PROVIDER_CLASSES: Dict[str, type] = {}


def _streaming_providers(client: Any = None, person_id: Any = "") -> List[Any]:
    """Every registered streaming provider that is currently connected."""
    store = client or globals().get("redis_client")
    settings: Dict[str, Any] = _settings(store)
    providers: List[Any] = []
    for provider_id, provider_class in STREAMING_PROVIDER_CLASSES.items():
        try:
            provider = provider_class.from_settings(settings)
        except Exception:
            continue
        try:
            if provider.connected:
                providers.append(provider)
        except Exception:
            continue
    del person_id  # person-scoped streaming credentials land here later
    return providers


def _streaming_similar_tracks(
    seed_tracks: List[Dict[str, Any]],
    *,
    count: int = CONTINUATION_BATCH_TRACKS,
    person_id: Any = "",
    client: Any = None,
) -> List[Dict[str, Any]]:
    """Similar tracks from connected streaming providers (empty until one exists).

    Catalog providers that implement similar_tracks (Subsonic, …) are consulted
    for their own seeds too; a failure or empty result just falls back to the
    library mix.
    """
    seeds = [dict(track) for track in (seed_tracks or []) if isinstance(track, dict)]
    if not seeds:
        return []
    collected: List[Dict[str, Any]] = []
    for provider in _streaming_providers(client, person_id):
        try:
            rows = provider.similar_tracks(seeds, count=count) or []
        except Exception as exc:
            logger.warning("[Music] %s similar-tracks lookup failed: %s", provider.provider_id, exc)
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            track = _normalize_track(dict(row))
            track.setdefault("provider", provider.provider_id)
            collected.append(track)
            if len(collected) >= count:
                return collected
    # The seed's own catalog provider (if it can suggest similar tracks).
    tried: set = set()
    for seed in seeds:
        provider_id = _provider_id(seed.get("provider"), "")
        if provider_id not in CATALOG_PROVIDER_IDS or provider_id in tried:
            continue
        tried.add(provider_id)
        seed_scope = _text(seed.get("person_scope")) or _text(person_id)
        try:
            provider = _provider(client, provider_id, seed_scope)
            similar = getattr(provider, "similar_tracks", None)
            if not callable(similar):
                continue
            rows = similar([seed], count=count) or []
        except Exception as exc:
            logger.warning("[Music] %s similar-tracks lookup failed: %s", provider_id, exc)
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            track = _normalize_track(dict(row))
            track["provider"] = provider_id
            if _text(seed.get("person_scope")):
                track["person_scope"] = _text(seed.get("person_scope"))
            collected.append(track)
            if len(collected) >= count:
                return collected
    return collected


def _provider_class(provider_id: Any) -> Optional[type]:
    """The settings-built class for one catalog provider id (None if unknown)."""
    classes = {
        "emby": EmbyMusicProvider,
        "jellyfin": JellyfinMusicProvider,
        "subsonic": SubsonicMusicProvider,
        "plex": PlexMusicProvider,
        "network_share": NetworkShareMusicProvider,
    }
    return classes.get(_provider_id(provider_id, ""))


def _provider(client: Any = None, provider_id: Any = "", person_id: Any = "") -> Any:
    # A linked Person plays from their own source configuration, when set.
    linked = _person_link_provider(person_id, provider_id, client) if _text(person_id) else None
    if linked is not None:
        return linked
    selected = _provider_id(provider_id)
    provider_class = _provider_class(selected)
    if provider_class is None:
        raise ValueError(
            f"{PROVIDER_LABELS.get(selected, selected)} support is not enabled in this build."
        )
    return provider_class.from_settings(_settings(client))


def _person_source_id(person_id: Any, client: Any = None) -> str:
    """Provider id one Person resolves to (link override, else the global source)."""
    linked = _person_link_provider(person_id, "", client) if _text(person_id) else None
    if linked is not None:
        return linked.provider_id
    return _provider_id(_settings(client).get("provider"))


def _paired(
    settings: Optional[Dict[str, Any]] = None,
    provider_id: Any = "",
) -> bool:
    del settings
    try:
        return bool(_provider(None, provider_id).connected)
    except Exception:
        return False


def _detected_lan_address() -> str:
    """Best-effort LAN address of this host, without sending any traffic."""
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            probe.connect(("10.255.255.255", 1))
            return _text(probe.getsockname()[0])
        finally:
            probe.close()
    except Exception:
        return "127.0.0.1"


def _stream_port(client: Any = None) -> int:
    cfg = _settings(client)
    return _as_int(cfg.get("stream_bind_port"), STREAM_DEFAULT_PORT, 1, 65535)


def _stream_base_url(client: Any = None) -> str:
    cfg = _settings(client)
    host = _text(cfg.get("stream_host")) or _detected_lan_address()
    return f"http://{host}:{_stream_port(client)}"


def _stream_token(client: Any = None) -> str:
    cfg = _settings(client)
    token = _text(cfg.get("stream_token"))
    if token:
        return token
    token = uuid.uuid4().hex
    _save_hash(client or globals().get("redis_client"), SETTINGS_KEY, {"stream_token": token})
    return token


def _stream_proxy_url(kind: str, item_id: Any, *, sync: bool = False) -> str:
    item_id = _text(item_id)
    if not item_id:
        return ""
    return (
        f"{_stream_base_url()}/stream/{quote(_stream_token(), safe='')}"
        f"/{kind}/{quote(str(item_id), safe='')}"
        + ("?sync=1" if sync else "")
    )


def _genre_key(value: Any) -> str:
    return " ".join(
        "".join(char.lower() if char.isalnum() else " " for char in _text(value)).split()
    )


def _genre_marker_matches(key: str, marker: str) -> bool:
    return key == marker or f" {marker} " in f" {key} "


def _genres(value: Any, fallback: Any = "") -> List[str]:
    raw: List[Any]
    if isinstance(value, list) and any(_text(item) for item in value):
        raw = value
    else:
        text = _text(value or fallback)
        for separator in (";", "|"):
            text = text.replace(separator, ",")
        raw = text.split(",") if text else []
    result: List[str] = []
    seen = set()
    for item in raw:
        genre = _text(item)
        source_key = _genre_key(genre)
        if not source_key:
            continue
        expanded = [GENRE_CANONICAL_NAMES.get(source_key, genre)]
        expanded.extend(
            name
            for name, markers in GENRE_FAMILIES
            if any(_genre_marker_matches(source_key, marker) for marker in markers)
        )
        for candidate in expanded:
            key = candidate.casefold()
            if not candidate or key in seen:
                continue
            seen.add(key)
            result.append(candidate)
    return result


def _format_duration(seconds: Any) -> str:
    duration = max(0, int(_as_float(seconds)))
    minutes, secs = divmod(duration, 60)
    if minutes >= 60:
        hours, minutes = divmod(minutes, 60)
        return f"{hours}:{minutes:02d}:{secs:02d}"
    return f"{minutes}:{secs:02d}"


def _normalize_track(row: Dict[str, Any]) -> Dict[str, Any]:
    # Accepts provider-native rows (Emby BaseItemDto) plus the generic row shape
    # used by this file's own catalog helpers.
    track_id = _text(row.get("Id") or row.get("ratingKey") or row.get("rating_key") or row.get("key") or row.get("id"))
    title = _text(row.get("Name") or row.get("title")) or "Untitled"
    raw_artists = row.get("Artists")
    if isinstance(raw_artists, list) and raw_artists:
        artist = ", ".join(_text(value) for value in raw_artists if _text(value))
    else:
        artist = _text(row.get("artist"))
    album_artist = _text(row.get("AlbumArtist") or row.get("albumArtist") or row.get("album_artist"))
    album = _text(row.get("Album") or row.get("album"))
    genres = _genres(row.get("Genres") or row.get("genres"), row.get("genre"))
    runtime_ticks = _as_float(row.get("RunTimeTicks"))
    duration = _as_float(
        row.get("durationSeconds")
        or row.get("duration_seconds")
        or row.get("duration")
        or (runtime_ticks / 10_000_000.0 if runtime_ticks > 0 else 0.0)
    )
    if duration > 100000:
        duration /= 1000.0
    source_index = _as_int(row.get("sourceIndex") or row.get("source_index"), 0, 0, 10000)
    path = _text(row.get("Path") or row.get("path") or row.get("partKey") or row.get("part_key"))
    artwork_path = _text(row.get("artwork_path"))
    # Cover art for music usually hangs off the Album item, not the song; keep
    # the album id so artwork fetching can fall back to it when the song item
    # carries no Primary image.
    album_id = _text(row.get("AlbumId") or row.get("albumId") or row.get("album_id"))
    artwork_item_id = _text(row.get("artwork_item_id") or track_id)
    image_tags = row.get("ImageTags") if isinstance(row.get("ImageTags"), dict) else {}
    artwork_version = _text(row.get("artwork_version")) or _text(image_tags.get("Primary"))
    if not track_id:
        identity = "\x00".join(
            [
                _text(row.get("categoryId") or row.get("category_id")),
                str(source_index),
                path,
                artist,
                album,
                title,
            ]
        )
        track_id = "track:" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
    duration_display = _text(row.get("durationDisplay") or row.get("duration_display"))
    if not duration_display and duration > 0:
        duration_display = _format_duration(duration)
    year = _text(row.get("ProductionYear") or row.get("date") or row.get("year"))
    if not year:
        premiere = _text(row.get("PremiereDate"))
        year = premiere[:4] if len(premiere) >= 4 else ""
    return {
        "id": track_id,
        "title": title,
        "artist": artist,
        "album_artist": album_artist,
        "album": album,
        "genres": genres,
        "genre": ", ".join(genres),
        "year": year,
        "track_number": _as_int(row.get("IndexNumber") or row.get("index") or row.get("track_number"), 0, 0, 10000),
        "disc_number": _as_int(
            row.get("ParentIndexNumber") or row.get("disc") or row.get("disc_number"),
            0,
            0,
            1000,
        ),
        "duration_seconds": max(0.0, duration),
        "duration_display": duration_display,
        "category_id": _text(row.get("categoryId") or row.get("category_id")),
        "source_index": source_index,
        "path": path,
        "stream_path": _text(row.get("stream_path")),
        "provider_track_id": _text(
            row.get("provider_track_id")
            or row.get("Id")
            or row.get("ratingKey")
            or row.get("rating_key")
            or row.get("key")
        ),
        "container": _text(
            row.get("Container") or row.get("container") or Path(path).suffix.lstrip(".")
        ).lower(),
        "media_type": _text(row.get("media_type") or row.get("content_type")).lower(),
        "size_bytes": _as_int(row.get("sizeBytes") or row.get("size_bytes"), 0, 0, 10**15),
        "modified_unix": _as_int(row.get("modifiedUnix") or row.get("modified_unix"), 0, 0, 10**12),
        "album_id": album_id,
        "artwork_path": artwork_path,
        "artwork_item_id": artwork_item_id,
        "artwork_version": artwork_version,
        "has_artwork": bool(
            artwork_path or (artwork_item_id and artwork_version) or album_id
        ),
        "provider": _provider_id(row.get("provider")),
    }


def _facet_values(tracks: Iterable[Dict[str, Any]], key: str) -> List[str]:
    values: Dict[str, str] = {}
    for track in tracks:
        raw_values = track.get(key)
        if isinstance(raw_values, list):
            candidates = raw_values
        else:
            candidates = [raw_values]
        for raw in candidates:
            value = _text(raw)
            if value:
                values[value.casefold()] = value
    return sorted(values.values(), key=str.casefold)


def _catalog(client: Any = None, provider_id: Any = "", person_id: Any = "") -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    now = time.monotonic()
    wanted_provider = _provider_id(provider_id, "")
    wanted_person = _text(person_id)
    if not wanted_provider:
        wanted_provider = (
            _person_source_id(wanted_person, store) if wanted_person else "emby"
        )
    with _catalog_memory_cache_lock:
        cached = _catalog_memory_cache.get("payload")
        cached_fresh = (
            _catalog_memory_cache.get("store") is store
            and isinstance(cached, dict)
            and now - float(_catalog_memory_cache.get("loaded_at") or 0.0)
            < CATALOG_MEMORY_CACHE_TTL_SECONDS
        )
        if cached_fresh and _text(_catalog_memory_cache.get("person")) == wanted_person:
            if cached and _provider_id(cached.get("provider")) == wanted_provider:
                return cached
            # Mismatched or empty cached payload: fall through and reload.

        payload = _load_json(store, _catalog_key(wanted_person), {})
        if not isinstance(payload, dict):
            payload = {}
        if _provider_id(payload.get("provider")) != wanted_provider:
            payload = {}
        for track in payload.get("tracks") or []:
            if isinstance(track, dict):
                _normalize_cached_artwork(track)
                genres = _genres(track.get("genres"), track.get("genre"))
                track["genres"] = genres
                track["genre"] = ", ".join(genres)
        if isinstance(payload.get("tracks"), list):
            payload["genres"] = _facet_values(payload["tracks"], "genres")
        _catalog_memory_cache.update(
            {"store": store, "loaded_at": now, "payload": payload, "person": wanted_person}
        )
        return payload


def _person_catalog(
    client: Any = None,
    provider_id: Any = "",
    person_id: Any = "",
) -> Dict[str, Any]:
    """One Person's catalog across every source they have linked.

    Single-source People (and personless/household callers) behave exactly like
    _catalog. A Person with two linked sources gets one merged payload: the
    track lists are concatenated (each track keeps its own provider so playback,
    history, and stream URLs stay per source) and the facets are rebuilt.
    """
    store = client or globals().get("redis_client")
    wanted_person = _text(person_id)
    sources = _person_catalog_source_ids(wanted_person, store)
    if len(sources) <= 1:
        # Single-source People keep the exact _catalog semantics (including an
        # explicit provider hint narrowing or mismatching the payload).
        return _catalog(store, sources[0] if sources else provider_id, wanted_person)
    # Multi-source Person: merge every linked source. Any provider hint a
    # caller derived from this Person's own link is a subset of the sources,
    # so the merged payload is always the right answer here.
    payloads = [_catalog(store, sources[0], wanted_person)]
    for extra_source in sources[1:]:
        payloads.append(
            _catalog(store, extra_source, _person_extra_slot(wanted_person, extra_source))
        )
    tracks: List[Dict[str, Any]] = []
    playlists: List[Dict[str, Any]] = []
    seen_ids: set = set()
    for payload in payloads:
        for track in payload.get("tracks") or []:
            if not isinstance(track, dict):
                continue
            _normalize_cached_artwork(track)
            genres = _genres(track.get("genres"), track.get("genre"))
            track["genres"] = genres
            track["genre"] = ", ".join(genres)
            track_id = _text(track.get("id"))
            if track_id and track_id in seen_ids:
                continue
            if track_id:
                seen_ids.add(track_id)
            tracks.append(track)
        for playlist in payload.get("playlists") or []:
            if isinstance(playlist, dict) and _text(playlist.get("name")):
                playlists.append(playlist)
    return {
        "provider": sources[0],
        "artwork_schema": CATALOG_ARTWORK_SCHEMA,
        "tracks": tracks,
        "artists": _facet_values(
            [{"artist": _text(row.get("album_artist")) or _text(row.get("artist"))} for row in tracks],
            "artist",
        ),
        "albums": _facet_values(tracks, "album"),
        "genres": _facet_values(tracks, "genres"),
        "playlists": playlists,
        "synced_at": max(
            (_as_float(payload.get("synced_at")) for payload in payloads), default=0.0
        ),
    }


def _catalog_needs_artwork_refresh(
    client: Any = None,
    provider_id: Any = "",
    person_id: Any = "",
) -> bool:
    store = client or globals().get("redis_client")
    # The core loop checks this every second. Reuse the in-process catalog so a
    # multi-megabyte library is not decoded from Redis on every heartbeat.
    payload = _catalog(store, provider_id, person_id)
    if not isinstance(payload, dict) or not payload:
        return True
    return (
        _provider_id(payload.get("provider")) != _provider_id(provider_id)
        or _as_int(payload.get("artwork_schema"), 0, 0, 100) < CATALOG_ARTWORK_SCHEMA
    )


def _sync_catalog_impl(
    client: Any = None,
    provider_id: Any = "",
    person_id: Any = "",
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    selected = _provider_id(provider_id, "") or _person_source_id(person_id, store)
    provider = _provider(store, selected, person_id)
    if not provider.connected:
        raise ValueError(
            f"Connect {PROVIDER_LABELS.get(selected, selected)} before syncing its music library."
        )
    raw = provider.catalog()
    tracks = []
    sync_scope = _text(person_id)
    for row in (raw.get("tracks") if isinstance(raw, dict) else []) or []:
        if isinstance(row, dict):
            track = _normalize_track(row)
            track["provider"] = selected
            if sync_scope:
                # Stamp the sync scope so playback resolves this track against
                # the credentials it was synced with (a Person's own server
                # account streams with their credentials, not the household's).
                track["person_scope"] = sync_scope
            tracks.append(track)
    artists = _facet_values(
        [
            {
                "artist": _text(track.get("album_artist")) or _text(track.get("artist")),
            }
            for track in tracks
        ],
        "artist",
    )
    albums = _facet_values(tracks, "album")
    genres = _facet_values(tracks, "genres")
    playlists = [
        row
        for row in (raw.get("playlists") if isinstance(raw, dict) else []) or []
        if isinstance(row, dict) and _text(row.get("name")) and isinstance(row.get("track_ids"), list)
    ]
    payload = {
        "provider": selected,
        "artwork_schema": CATALOG_ARTWORK_SCHEMA,
        "catalog_id": _text(raw.get("catalog_id")) if isinstance(raw, dict) else "",
        "tracks": tracks,
        "artists": artists,
        "albums": albums,
        "genres": genres,
        "playlists": playlists,
        "libraries": raw.get("libraries") if isinstance(raw, dict) and isinstance(raw.get("libraries"), dict) else {},
        "synced_at": time.time(),
    }
    _save_json(store, _catalog_key(person_id), payload)
    _record_catalog_stats(
        store,
        person_id,
        {
            "status": "ok",
            "provider": selected,
            "track_count": len(tracks),
            "artist_count": len(artists),
            "album_count": len(albums),
            "genre_count": len(genres),
            "synced_at": payload["synced_at"],
        },
    )
    with _catalog_memory_cache_lock:
        _catalog_memory_cache.update(
            {
                "store": store,
                "loaded_at": time.monotonic(),
                "payload": payload,
                "person": _text(person_id),
            }
        )
    if not _text(person_id):
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "status": "connected",
                "provider": selected,
                "last_sync_at": payload["synced_at"],
                "last_error": "",
                "track_count": len(tracks),
                "artist_count": len(artists),
                "album_count": len(albums),
                "genre_count": len(genres),
            },
        )
    return payload


def _sync_catalog(
    client: Any = None,
    provider_id: Any = "",
    person_id: Any = "",
) -> Dict[str, Any]:
    global _catalog_sync_started_at
    if not _catalog_sync_lock.acquire(blocking=False):
        raise RuntimeError("Music library sync is already running.")
    store = client or globals().get("redis_client")
    _catalog_sync_started_at = time.time()
    try:
        payload = _sync_catalog_impl(store, provider_id, person_id)
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_sync_finished_at": finished_at,
                "last_sync_duration_ms": max(0.0, (finished_at - _catalog_sync_started_at) * 1000.0),
                "last_sync_error": "",
                "last_sync_error_at": "",
                "sync_run_count": _as_int(runtime.get("sync_run_count"), 0, 0, 1_000_000_000) + 1,
            },
        )
        return payload
    except Exception as exc:
        finished_at = time.time()
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_sync_finished_at": finished_at,
                "last_sync_duration_ms": max(0.0, (finished_at - _catalog_sync_started_at) * 1000.0),
                "last_sync_error": _text(exc)[:500],
                "last_sync_error_at": finished_at,
            },
        )
        raise
    finally:
        _catalog_sync_started_at = 0.0
        _catalog_sync_lock.release()


def _schedule_catalog_sync(person_id: Any = "", client: Any = None) -> bool:
    """Sync one catalog off-thread (a Person's own, or "" for the household).

    Used by tab actions that would otherwise leave a never-synced catalog
    invisible. Returns False when a sync worker is already running; on-demand
    and scheduled syncs all share _catalog_sync_lock underneath. The outcome is
    recorded in CATALOG_STATS_KEY, which the link and search cards display.
    """
    global _catalog_sync_thread
    store = client or globals().get("redis_client")
    person_id = _text(person_id)
    with _state_lock:
        if _catalog_sync_thread is not None and _catalog_sync_thread.is_alive():
            return False
        # Cards show "Syncing…" until the worker records the outcome.
        _record_catalog_stats(store, person_id, {"status": "syncing", "started_at": time.time()})

        def worker() -> None:
            try:
                _sync_catalog(store, "", person_id)
            except Exception as exc:
                logger.warning("[Music] catalog sync failed: %s", exc)
                _record_catalog_stats(
                    store,
                    person_id,
                    {
                        "status": "error",
                        "error": _text(exc)[:200],
                        "failed_at": time.time(),
                    },
                )

        _catalog_sync_thread = threading.Thread(
            target=worker,
            daemon=True,
            name="music-catalog-sync",
        )
        _catalog_sync_thread.start()
        return True


def _clean_query_tokens(value: Any) -> List[str]:
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in _text(value))
    tokens = [token for token in cleaned.split() if token and token not in GENERIC_SEARCH_WORDS]
    return tokens


def _track_haystack(track: Dict[str, Any]) -> str:
    return " ".join(
        [
            _text(track.get("title")),
            _text(track.get("artist")),
            _text(track.get("album_artist")),
            _text(track.get("album")),
            _text(track.get("genre")),
            _text(track.get("year")),
        ]
    ).casefold()


def _matches_filter(track: Dict[str, Any], key: str, value: Any) -> bool:
    wanted = _text(value).casefold()
    if not wanted:
        return True
    if key == "genre":
        wanted_values = {_text(item).casefold() for item in _genres([value])}
        values = {
            _text(item).casefold()
            for item in _genres(track.get("genres"), track.get("genre"))
        }
        return bool(wanted_values & values) or any(
            wanted in item for item in values
        )
    if key == "artist":
        value = f"{_text(track.get('artist'))} {_text(track.get('album_artist'))}".casefold()
        return wanted in value
    return wanted in _text(track.get(key)).casefold()


def _score_track(track: Dict[str, Any], filters: Dict[str, Any]) -> int:
    score = 0
    for key, weight in (("title", 140), ("artist", 100), ("album", 80), ("genre", 60)):
        wanted = _text(filters.get(key)).casefold()
        if not wanted:
            continue
        if key == "genre":
            values = [
                _text(item).casefold()
                for item in _genres(track.get("genres"), track.get("genre"))
            ]
        elif key == "artist":
            values = [_text(track.get("artist")).casefold(), _text(track.get("album_artist")).casefold()]
        else:
            values = [_text(track.get(key)).casefold()]
        if wanted in values:
            score += weight
        elif any(wanted in item for item in values):
            score += max(1, weight // 2)
    query_tokens = _clean_query_tokens(filters.get("query"))
    haystack = _track_haystack(track)
    score += sum(12 for token in query_tokens if token in haystack)
    return score


def _search_tracks(
    *,
    query: Any = "",
    title: Any = "",
    artist: Any = "",
    album: Any = "",
    genre: Any = "",
    limit: int = MAX_SEARCH_RESULTS,
    client: Any = None,
    provider_id: Any = "",
    person_id: Any = "",
) -> List[Dict[str, Any]]:
    payload = _person_catalog(client, provider_id, person_id)
    tracks = payload.get("tracks") if isinstance(payload.get("tracks"), list) else []
    filters = {
        "query": _text(query),
        "title": _text(title),
        "artist": _text(artist),
        "album": _text(album),
        "genre": _text(genre),
    }
    query_tokens = _clean_query_tokens(query)
    rows: List[Dict[str, Any]] = []
    for track in tracks:
        if not isinstance(track, dict):
            continue
        if any(
            not _matches_filter(track, key, filters[key])
            for key in ("title", "artist", "album", "genre")
            if filters[key]
        ):
            continue
        haystack = _track_haystack(track)
        if query_tokens and any(token not in haystack for token in query_tokens):
            continue
        rows.append({**track, "_score": _score_track(track, filters)})
    rows.sort(
        key=lambda row: (
            -_as_int(row.get("_score"), 0, 0, 100000),
            _text(row.get("album_artist") or row.get("artist")).casefold(),
            _text(row.get("album")).casefold(),
            _as_int(row.get("track_number"), 0, 0, 10000),
            _text(row.get("title")).casefold(),
        )
    )
    result = []
    for row in rows[: max(1, min(1000, int(limit)))]:
        cleaned = dict(row)
        cleaned.pop("_score", None)
        result.append(cleaned)
    return result


def _public_track(track: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        key: track.get(key)
        for key in (
            "id",
            "title",
            "artist",
            "album_artist",
            "album",
            "genres",
            "genre",
            "year",
            "track_number",
            "duration_seconds",
            "duration_display",
            "provider",
        )
    }
    result["artwork_url"] = _artwork_display_url(track) if track else ""
    return result


def _player_key(person_id: Any = "") -> str:
    """Queue storage key: shared household queue for "", per-Person otherwise."""
    wanted = _text(person_id)
    return f"{PLAYER_KEY}:{wanted}" if wanted else PLAYER_KEY


def _queue_id_for_person(person_id: Any) -> str:
    """Every Person gets their own queue; only personless requests share one."""
    return _text(person_id)


def _register_queue(person_id: Any, client: Any = None) -> None:
    """Track queue slots so the background loop advances every active queue."""
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    if store is None or not queue_id:
        return
    registry = _queue_registry(store)
    if queue_id in registry:
        return
    registry[queue_id] = {"person_id": queue_id, "created_at": time.time()}
    _save_json(store, QUEUE_REGISTRY_KEY, registry)


def _queue_registry(client: Any = None) -> Dict[str, Dict[str, Any]]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, QUEUE_REGISTRY_KEY, {})
    if not isinstance(payload, dict):
        return {}
    return {
        _text(queue_id): row
        for queue_id, row in payload.items()
        if _text(queue_id) and isinstance(row, dict)
    }


def _active_queue_ids(client: Any = None) -> List[str]:
    """All queue slots that exist: the shared queue plus every Person queue."""
    store = client or globals().get("redis_client")
    return ["", *_queue_registry(store).keys()]


# --------------------------------------------------------------------------
# Multi-queue coordination: room occupancy, room bindings, conflict handling,
# and pending confirmations. A room plays at most one queue's stream at a
# time; these helpers decide who owns a room and how takeovers are resolved.
# --------------------------------------------------------------------------

def _queue_conflict_mode(person_id: Any, client: Any = None) -> str:
    """Per-Person conflict behavior from their link, else the shared default."""
    mode = ""
    if _text(person_id):
        mode = _text(_person_link(person_id, client).get("queue_conflict_mode")).casefold()
    if mode not in QUEUE_CONFLICT_MODES:
        mode = _text(_settings(client).get("queue_conflict_mode")).casefold()
    return mode if mode in QUEUE_CONFLICT_MODES else DEFAULT_QUEUE_CONFLICT_MODE


def _room_bindings(client: Any = None) -> Dict[str, str]:
    """Persistent room -> Person bindings ("the Kitchen plays Alex's music")."""
    store = client or globals().get("redis_client")
    if store is None:
        return {}
    try:
        raw = store.hgetall(ROOM_BINDINGS_KEY) or {}
    except Exception:
        return {}
    bindings: Dict[str, str] = {}
    for target, person in raw.items():
        target_name = _text(target)
        person_id = _text(person)
        if target_name and person_id:
            bindings[target_name] = person_id
    return bindings


def _bound_targets_for_person(person_id: Any, client: Any = None) -> List[str]:
    wanted = _text(person_id)
    if not wanted:
        return []
    return sorted(
        target for target, person in _room_bindings(client).items() if person == wanted
    )


def _set_room_bindings(targets: Any, person_id: Any, client: Any = None) -> List[str]:
    """Bind rooms to a Person's queue (or clear them when person_id is empty)."""
    store = client or globals().get("redis_client")
    wanted = _text(person_id)
    changed: List[str] = []
    for target in _normalize_stereo_targets(targets):
        target_name = _text(target)
        if not target_name:
            continue
        try:
            if wanted:
                store.hset(ROOM_BINDINGS_KEY, target_name, wanted)
            else:
                store.hdel(ROOM_BINDINGS_KEY, target_name)
        except Exception:
            continue
        changed.append(target_name)
    return changed


def _occupied_targets(client: Any = None) -> Dict[str, str]:
    """Map each destination with a live (playing or paused) queue to its queue id."""
    store = client or globals().get("redis_client")
    occupied: Dict[str, str] = {}
    for queue_id in _active_queue_ids(store):
        player = _player(store, queue_id)
        if _text(player.get("status")).lower() not in {"playing", "paused"}:
            continue
        for target in _list(player.get("targets") or player.get("target")):
            occupied[target] = queue_id
    return occupied


def _queue_conflicts(
    client: Any,
    person_id: Any,
    targets: Any,
) -> Dict[str, Any]:
    """Who is playing on the requested rooms, and is the Person's own queue elsewhere?"""
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    wanted = set(_normalize_stereo_targets(targets))
    occupied = _occupied_targets(store)
    foreign = {target: qid for target, qid in occupied.items() if qid != queue_id}
    own = _player(store, queue_id)
    own_targets = _list(own.get("targets") or own.get("target"))
    own_playing_elsewhere = (
        _text(own.get("status")).lower() in {"playing", "paused"}
        and bool(own_targets)
        and not (set(own_targets) & wanted)
    )
    return {
        "foreign_targets": sorted(foreign),
        "foreign_queues": sorted(set(foreign.values())),
        "own_playing_elsewhere": own_playing_elsewhere,
        "own_queue": own,
    }


def _release_targets_to(
    store: Any,
    targets: Any,
    *,
    except_queue_id: Any = "",
) -> List[str]:
    """Stop other queues on the given rooms and shrink their target lists.

    A queue that keeps at least one room is paused with its position intact, so
    its owner can resume it elsewhere; a queue with no rooms left is stopped.
    """
    wanted = set(_normalize_stereo_targets(targets))
    except_queue_id = _queue_id_for_person(except_queue_id)
    released: List[str] = []
    for queue_id in _active_queue_ids(store):
        if queue_id == except_queue_id:
            continue
        with _state_lock:
            other = _player(store, queue_id)
            if _text(other.get("status")).lower() not in {"playing", "paused"}:
                continue
            old_targets = _list(other.get("targets") or other.get("target"))
            stolen = [target for target in old_targets if target in wanted]
            if not stolen:
                continue
            # Only stop the media sessions that belong to the stolen rooms.
            sessions = _playback_voice_core_sessions(other)
            sessions_for_stolen = [
                session
                for session in sessions
                if set(
                    _expand_session_selectors(session)
                )
                & wanted
            ]
            try:
                warnings = _stop_target(
                    stolen,
                    expected_voice_core_sessions=sessions_for_stolen,
                )
            except Exception as exc:
                warnings = [_text(exc)]
            remaining = [target for target in old_targets if target not in wanted]
            position = _player_position_seconds(other)
            if remaining:
                other.update(
                    {
                        "status": "paused",
                        "started_at": 0.0,
                        "position_offset_seconds": position,
                        "targets": remaining,
                    }
                )
            else:
                other.update(
                    {
                        "status": "stopped",
                        "started_at": 0.0,
                        "position_offset_seconds": 0.0,
                        "targets": [],
                    }
                )
            if warnings:
                other["warnings"] = [
                    *_list(other.get("warnings")),
                    *warnings,
                ]
            _save_player(other, store, queue_id)
            released.extend(stolen)
    return released


def _expand_session_selectors(session: Dict[str, Any]) -> List[str]:
    """Every speaker a playback session covers, including stereo pair members."""
    selectors = _list(session.get("selectors") or session.get("target"))
    expanded: List[str] = list(selectors)
    try:
        from tater_voice import stereo_pairs

        for selector in selectors:
            pair = (
                stereo_pairs.get_pair(selector)
                if stereo_pairs.is_stereo_selector(selector)
                else {}
            )
            if isinstance(pair, dict):
                expanded.extend(
                    value
                    for value in (
                        _text(pair.get("left_selector")),
                        _text(pair.get("right_selector")),
                    )
                    if value
                )
    except Exception:
        pass
    return expanded


def _pending_confirmation_key(person_id: Any) -> str:
    return f"{PENDING_CONFIRM_KEY_PREFIX}{_text(person_id) or 'shared'}"


def _save_pending_confirmation(
    store: Any,
    person_id: Any,
    payload: Dict[str, Any],
) -> None:
    body = dict(payload)
    body["expires_at"] = time.time() + PENDING_CONFIRM_TTL_SECONDS
    _save_json(store, _pending_confirmation_key(person_id), body)


def _load_pending_confirmation(
    store: Any,
    person_id: Any,
) -> Dict[str, Any]:
    payload = _load_json(store, _pending_confirmation_key(person_id), {})
    if not isinstance(payload, dict) or not payload:
        return {}
    if _as_float(payload.get("expires_at")) < time.time():
        _clear_pending_confirmation(store, person_id)
        return {}
    return payload


def _clear_pending_confirmation(store: Any, person_id: Any) -> None:
    try:
        store.delete(_pending_confirmation_key(person_id))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Follow-Me presence (Home Assistant person tracking).
#
# Each linked Person can carry a Home Assistant person entity ("person.john").
# The entity's state is the friendly name of the zone the Person is in (or
# "not_home"), updated by whatever presence stack the user runs — BLE trackers
# computing the closest node, phone GPS, and so on. When the zone changes and
# holds for the move delay, the Person's queue hands off to that room at the
# same spot in the track, using the same room resolution as the voice
# "move my music" path (room store preferred player, then target label match).
# The HA base URL and token are reused from Tater's built-in Home Assistant
# integration; the core polls HA's REST API directly because the host's
# WebSocket state mirror can filter person entities out depending on its
# entity-scope setting.
# ---------------------------------------------------------------------------


def _follow_me_follow_enabled(cfg: Dict[str, Any]) -> bool:
    return _as_bool(cfg.get("follow_me_enabled"), False)


def _follow_me_poll_interval(cfg: Dict[str, Any]) -> int:
    return _as_int(
        cfg.get("follow_me_poll_interval_seconds"),
        FOLLOW_ME_DEFAULT_POLL_SECONDS,
        5,
        3600,
    )


def _follow_me_move_delay(cfg: Dict[str, Any]) -> float:
    raw = cfg.get("follow_me_move_delay_seconds")
    if _text(raw) == "":
        return FOLLOW_ME_DEFAULT_MOVE_DELAY_SECONDS
    return max(0.0, _as_float(raw, FOLLOW_ME_DEFAULT_MOVE_DELAY_SECONDS))


def _follow_me_takeover_mode(person_id: Any, client: Any = None) -> str:
    """Per-Person override, else the global setting, else auto take over."""
    store = client or globals().get("redis_client")
    link = _person_link(person_id, store)
    value = _text(link.get("follow_me_takeover_mode")).casefold()
    if value in FOLLOW_ME_TAKEOVER_MODES:
        return value
    value = _text(_settings(store).get("follow_me_takeover_mode")).casefold()
    if value in FOLLOW_ME_TAKEOVER_MODES:
        return value
    return DEFAULT_FOLLOW_ME_TAKEOVER_MODE


def _follow_me_away_action(person_id: Any, client: Any = None) -> str:
    """Per-Person override, else the global setting, else keep/pause."""
    store = client or globals().get("redis_client")
    link = _person_link(person_id, store)
    value = _text(link.get("follow_me_away_action")).casefold()
    if value in FOLLOW_ME_AWAY_ACTIONS:
        return value
    value = _text(_settings(store).get("follow_me_away_action")).casefold()
    if value in FOLLOW_ME_AWAY_ACTIONS:
        return value
    return DEFAULT_FOLLOW_ME_AWAY_ACTION


def _person_resume_delay(link_key: str, cfg_key: str, person_id: Any, client: Any = None) -> float:
    """Per-Person gaining-room resume delay, else the global setting (0-600 s)."""
    store = client or globals().get("redis_client")
    raw = _person_link(person_id, store).get(link_key)
    if _text(raw) == "":
        raw = _settings(store).get(cfg_key)
    return min(600.0, max(0.0, _as_float(raw, 0.0)))


def _transfer_resume_delay(person_id: Any = "", client: Any = None) -> float:
    """Delay before a moved queue resumes in its new rooms (room-to-room transfers)."""
    return _person_resume_delay(
        "transfer_resume_delay_seconds", "transfer_resume_delay_seconds", person_id, client
    )


def _follow_me_move_resume_delay(person_id: Any, client: Any = None) -> float:
    """Delay before the room a Person walked into resumes their music."""
    return _person_resume_delay(
        "follow_me_move_resume_delay_seconds",
        "follow_me_move_resume_delay_seconds",
        person_id,
        client,
    )


def _follow_me_resume_delay(person_id: Any, client: Any = None) -> float:
    """Delay before a room resumes music that Follow-Me had paused away."""
    return _person_resume_delay(
        "follow_me_resume_delay_seconds", "follow_me_resume_delay_seconds", person_id, client
    )


def _person_resume_room_mode(person_id: Any, client: Any = None) -> str:
    """What "resume my music" does from a room other than the queue's room."""
    store = client or globals().get("redis_client")
    mode = _text(_person_link(person_id, store).get("resume_room_mode")).casefold()
    if mode not in RESUME_ROOM_MODES:
        mode = _text(_settings(store).get("resume_room_mode")).casefold()
    return mode if mode in RESUME_ROOM_MODES else DEFAULT_RESUME_ROOM_MODE


def _ha_config(client: Any = None) -> Dict[str, str]:
    """Base URL and token of Tater's built-in Home Assistant integration."""
    store = client or globals().get("redis_client")
    try:
        from tater_voice.reply_playback import load_homeassistant_config

        conf = load_homeassistant_config(required=False, client=store)
        if isinstance(conf, dict):
            return {
                "base": _text(conf.get("base")).rstrip("/"),
                "token": _text(conf.get("token")),
            }
    except Exception:
        pass
    raw = store.hgetall(HA_SETTINGS_KEY) or {}
    base = _text(raw.get("HA_BASE_URL") or HA_DEFAULT_BASE_URL)
    return {"base": base.rstrip("/"), "token": _text(raw.get("HA_TOKEN"))}


def _ha_http_get(url: str, headers: Dict[str, str], timeout: Any):
    """One HA REST request; indirection so tests can stub the network."""
    return requests.get(url, headers=headers, timeout=timeout)


def _ha_person_location(client: Any, entity: str) -> Dict[str, Any]:
    """Current HA state of a person entity: {"state": ...} or {"error": ...}."""
    conf = _ha_config(client)
    if not _text(conf.get("token")):
        return {"error": "ha_not_configured"}
    url = f"{conf.get('base')}/api/states/{quote(_text(entity), safe='')}"
    try:
        response = _ha_http_get(
            url,
            {"Authorization": f"Bearer {conf.get('token')}"},
            HA_STATE_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        return {"error": _text(exc)[:300] or "ha_unreachable"}
    status = int(getattr(response, "status_code", 0) or 0)
    if status == 404:
        return {"error": "entity_not_found"}
    if status in {401, 403}:
        return {"error": "ha_unauthorized"}
    if status <= 0 or status >= 400:
        return {"error": f"ha_http_{status or 'error'}"}
    try:
        data = response.json()
    except Exception:
        return {"error": "ha_bad_response"}
    if not isinstance(data, dict):
        return {"error": "ha_bad_response"}
    return {"state": _text(data.get("state")), "last_changed": _as_float(data.get("last_changed"))}


def _follow_me_state_key(person_id: Any) -> str:
    return f"{FOLLOW_ME_KEY}:{_text(person_id) or 'shared'}"


def _follow_me_state(person_id: Any, client: Any = None) -> Dict[str, Any]:
    payload = _load_json(
        client or globals().get("redis_client"),
        _follow_me_state_key(person_id),
        {},
    )
    return payload if isinstance(payload, dict) else {}


def _save_follow_me_state(
    person_id: Any,
    state: Dict[str, Any],
    client: Any = None,
) -> None:
    _save_json(
        client or globals().get("redis_client"),
        _follow_me_state_key(person_id),
        state,
    )


def _normalize_room_token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", _text(value).casefold())


def _parse_room_overrides(raw: Any) -> Dict[str, str]:
    """Map normalized HA zone names to Tater room names ("The Kitchen=Kitchen")."""
    out: Dict[str, str] = {}
    for chunk in _list(raw):
        zone, _, room = chunk.partition("=")
        key = _normalize_room_token(zone)
        room_name = _text(room).strip()
        if key and room_name:
            out[key] = room_name
    return out


def _room_name_to_targets(room_name: Any, client: Any = None) -> str:
    """One target for a Tater room name: room-store preference, else label match."""
    store = client or globals().get("redis_client")
    name = _text(room_name)
    if not name:
        return ""
    preferred = _preferred_room_target([name], store)
    if preferred:
        return preferred
    return _room_target_from_query(name, _target_options())


def _resolve_follow_me_zone(
    zone: Any,
    link: Dict[str, Any],
    client: Any = None,
) -> tuple[str, str]:
    """(room_name, target) for an HA zone; ("", "") when the zone has no room."""
    zone_name = _text(zone)
    if not zone_name or zone_name.casefold() in {"not_home", "home"}:
        return "", ""
    overrides = _parse_room_overrides((link or {}).get("follow_me_room_overrides"))
    room_name = overrides.get(_normalize_room_token(zone_name), zone_name)
    return room_name, _room_name_to_targets(room_name, client)


def _speak_follow_me_prompt(targets: List[str], text: str) -> bool:
    """Best-effort TTS of the follow-me question on the destination room."""
    if not text:
        return False
    try:
        from speech_settings import get_speech_settings
        from speech_tts import speak_announcement_targets

        try:
            from tater_voice.runtime import run_async_blocking
        except Exception:
            run_async_blocking = None
        speech_settings = get_speech_settings() or {}
        ha_config = _ha_config()
        backend = (
            _text(speech_settings.get("announcement_tts_backend"))
            or _text(speech_settings.get("tts_backend"))
            or "wyoming"
        )
        result = speak_announcement_targets(
            text=text,
            backend=backend,
            ha_base=_text(ha_config.get("base")),
            token=_text(ha_config.get("token")),
            targets=list(targets),
            public_base_url="",
            default_backend=backend,
            tts_kind="follow_me",
        )
        if run_async_blocking is not None:
            result = run_async_blocking(result, timeout=120.0)
        else:
            result = asyncio.run(result)
        return bool(
            isinstance(result, dict)
            and (result.get("ok") or int(result.get("sent_count") or 0) > 0)
        )
    except Exception as exc:
        logger.debug("[Music] follow-me prompt playback skipped: %s", exc)
        return False


def _follow_me_move(
    person_id: Any,
    zone: Any,
    targets: List[str],
    client: Any = None,
) -> Dict[str, Any]:
    """Hand the Person's queue off to the room they walked into (same spot).

    Callers must hold _follow_me_lock. Returns one of:
      {"moved": True, "targets": [...]}
      {"moved": False, "reason": "no_playback" | "no_targets"
                                 | "awaiting_confirmation"}
    """
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    player = _player(store, queue_id)
    status = _text(player.get("status")).lower()
    if status not in {"playing", "paused"} or not (player.get("queue") or []):
        return {"moved": False, "reason": "no_playback"}
    wanted = _normalize_stereo_targets(targets)
    if not wanted:
        return {"moved": False, "reason": "no_targets"}
    paused_by_follow_me = bool(_follow_me_state(person_id, store).get("paused_by_follow_me"))

    conflicts = _queue_conflicts(store, queue_id, wanted)
    if conflicts["foreign_targets"]:
        if _follow_me_takeover_mode(person_id, store) == "ask":
            pending = _load_pending_confirmation(store, person_id)
            if (
                _text(pending.get("type")) != "follow_takeover"
                or sorted(_list(pending.get("targets"))) != sorted(wanted)
            ):
                owners = sorted(
                    {_queue_owner_label(qid, store) for qid in conflicts["foreign_queues"]}
                )
                question = (
                    f"{' and '.join(owners)} still playing on {_target_summary(wanted)}. "
                    f"Say yes to move your music here."
                )
                _save_pending_confirmation(
                    store,
                    person_id,
                    {
                        "type": "follow_takeover",
                        "args": {},
                        "origin": {},
                        "targets": list(wanted),
                        "queue_id": queue_id,
                        "zone": _text(zone),
                    },
                )
                _speak_follow_me_prompt(wanted, question)
            state = _follow_me_state(person_id, store)
            state["status"] = "awaiting_confirmation"
            _save_follow_me_state(person_id, state, store)
            return {"moved": False, "reason": "awaiting_confirmation"}
        # auto: free the destination from other queues without asking; a queue
        # that keeps at least one room stays paused at its position.
        _release_targets_to(store, wanted, except_queue_id=queue_id)
    _route_player_targets(
        wanted,
        resume_delay=_follow_me_move_resume_delay(person_id, store),
        person_id=queue_id,
        client=store,
    )
    if paused_by_follow_me and _text(
        _player(store, queue_id).get("status")
    ).lower() == "paused":
        # Walked back into a speaker room after Follow-Me paused them: resume
        # (after the Person's resume delay, if one is set).
        resume_delay = _follow_me_resume_delay(person_id, store)
        if resume_delay > 0:
            resumed_player = _player(store, queue_id)
            resumed_player["resume_delay_until"] = time.time() + resume_delay
            resumed_player["resume_delay_position"] = _player_position_seconds(resumed_player)
            _save_player(resumed_player, store, queue_id)
        else:
            _resume_player(person_id=queue_id, client=store)
        state = _follow_me_state(person_id, store)
        state.pop("paused_by_follow_me", None)
        _save_follow_me_state(person_id, state, store)
    return {"moved": True, "targets": _list(_player(store, queue_id).get("targets"))}


def _follow_me_apply_away_action(
    person_id: Any,
    *,
    client: Any = None,
) -> bool:
    """Pause the Person's queue (position kept) if it is currently playing.

    Callers set the paused_by_follow_me flag on their own tracking state and
    persist it, so a single owner writes the state.
    """
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    if _text(_player(store, queue_id).get("status")).lower() != "playing":
        return False
    _pause_player(person_id=queue_id, client=store)
    return True


def _follow_me_tick(client: Any = None) -> Dict[str, Any]:
    """One pass: check every linked Person's HA entity and follow the movers."""
    store = client or globals().get("redis_client")
    with _follow_me_lock:
        cfg = _settings(store)
        if not _follow_me_follow_enabled(cfg):
            return {"ok": True, "skipped": "disabled"}
        if not _text(_ha_config(store).get("token")):
            return {"ok": True, "skipped": "ha_not_configured"}
        delay = _follow_me_move_delay(cfg)
        now = time.time()
        summary: Dict[str, Any] = {
            "ok": True,
            "checked": 0,
            "moved": 0,
            "paused": 0,
            "awaiting": 0,
            "errors": [],
        }
        for person_id, link in _person_links(store).items():
            entity = _text(link.get("follow_me_person_entity"))
            if not entity:
                continue
            summary["checked"] += 1
            state = _follow_me_state(person_id, store)
            player = _player(store, _queue_id_for_person(person_id))
            # The flag only means "Follow-Me paused this"; any playback since
            # then (manual resume, new request, track advance) supersedes it.
            if bool(state.get("paused_by_follow_me")) and _text(
                player.get("status")
            ).lower() == "playing":
                state.pop("paused_by_follow_me", None)
            location = _ha_person_location(store, entity)
            if "error" in location:
                state.update(
                    {
                        "last_error": _text(location.get("error")),
                        "last_error_at": now,
                        "last_check_at": now,
                        "status": "error",
                    }
                )
                _save_follow_me_state(person_id, state, store)
                summary["errors"].append(f"{person_id}: {location.get('error')}")
                continue
            zone = _text(location.get("state"))
            if zone != _text(state.get("zone")):
                # A new room supersedes any pending takeover question.
                _clear_pending_confirmation(store, person_id)
                state.update(
                    {
                        "zone": zone,
                        "zone_since": now,
                        "last_error": "",
                        "status": "tracking",
                    }
                )
            state["last_check_at"] = now
            _save_follow_me_state(person_id, state, store)
            if now - _as_float(state.get("zone_since")) < delay:
                continue
            if zone.casefold() in {"not_home", ""}:
                if _follow_me_away_action(person_id, store) in {"keep_pause", "pause"}:
                    if _follow_me_apply_away_action(person_id, client=store):
                        summary["paused"] += 1
                        state["paused_by_follow_me"] = True
                        state["status"] = "paused_away"
                    else:
                        state["status"] = "away_idle"
                else:
                    state["status"] = "away_kept"
                _save_follow_me_state(person_id, state, store)
                continue
            room_name, target = _resolve_follow_me_zone(zone, link, store)
            if not target:
                state["resolved_room"] = ""
                state["resolved_targets"] = []
                if _follow_me_away_action(person_id, store) == "pause":
                    if _follow_me_apply_away_action(person_id, client=store):
                        summary["paused"] += 1
                        state["paused_by_follow_me"] = True
                        state["status"] = "paused_dead_zone"
                    else:
                        state["status"] = "dead_zone_idle"
                else:
                    state["status"] = "dead_zone"
                _save_follow_me_state(person_id, state, store)
                continue
            result = _follow_me_move(person_id, zone, [target], store)
            # _follow_me_move persists its own state changes (awaiting_confirmation,
            # cleared paused_by_follow_me); build on that copy, not the stale one.
            state = _follow_me_state(person_id, store)
            state.update({"resolved_room": room_name, "resolved_targets": [target]})
            if result.get("moved"):
                summary["moved"] += 1
                state["last_move_at"] = time.time()
                state["status"] = "following"
            elif result.get("reason") == "awaiting_confirmation":
                summary["awaiting"] += 1
                # Status was already set by _follow_me_move.
            else:
                state["status"] = "following"
            _save_follow_me_state(person_id, state, store)
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_follow_me_at": time.time(),
                "follow_me_last_error": "",
                "follow_me_run_count": _as_int(
                    runtime.get("follow_me_run_count"), 0, 0, 1_000_000_000
                )
                + 1,
            },
        )
        return summary


def _player(client: Any = None, person_id: Any = "") -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    payload = _load_json(store, _player_key(queue_id), {})
    if not isinstance(payload, dict):
        payload = {}
    payload["queue_id"] = queue_id
    stale_provider = (
        _text(payload.get("provider")).casefold() not in {"", *CATALOG_PROVIDER_IDS}
    )
    payload.setdefault("status", "idle")
    payload.setdefault("provider", "emby")
    payload.setdefault("queue", [])
    if stale_provider:
        payload.update(
            {
                "status": "stopped",
                "queue": [],
                "queue_original": [],
                "current": {},
                "index": -1,
                "started_at": 0.0,
                "continuation_pending": False,
            }
        )
    for collection_key in ("queue", "queue_original"):
        for track in payload.get(collection_key) or []:
            if isinstance(track, dict):
                _normalize_cached_artwork(track)
    current = payload.get("current") if isinstance(payload.get("current"), dict) else {}
    if current:
        _normalize_cached_artwork(current)
    payload.setdefault("index", -1)
    targets = _normalize_stereo_targets(payload.get("targets") or payload.get("target"))
    payload["targets"] = targets
    payload["target"] = targets[0] if targets else ""
    payload["mixed_sync_adjustment_ms"] = _mixed_sync_adjustment(targets, _settings(store))
    payload.setdefault("shuffle", False)
    payload.setdefault("repeat", "off")
    payload.setdefault(
        "continuous_radio",
        _provider_id(payload.get("provider")) in CATALOG_PROVIDER_IDS,
    )
    payload.setdefault("continuation_pending", False)
    payload.setdefault("radio_name", "Tater Continuous Radio")
    if queue_id:
        # Person queues always know their owner so history attribution and the
        # queue registry stay consistent even for queues created earlier.
        payload.setdefault("person_id", queue_id)
    return payload


def _save_player(
    player: Dict[str, Any],
    client: Any = None,
    person_id: Any = None,
) -> None:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(
        person_id if person_id is not None else player.get("queue_id")
    )
    player["queue_id"] = queue_id
    if queue_id:
        player["person_id"] = queue_id
        # Keep the queue registry current so occupancy scans and the run loop
        # always see every Person queue that exists.
        _register_queue(queue_id, store)
    targets = _normalize_stereo_targets(player.get("targets") or player.get("target"))
    player["targets"] = targets
    player["target"] = targets[0] if targets else ""
    player["updated_at"] = time.time()
    _save_json(store, _player_key(queue_id), player)


def _persist_shared_player_volume(
    player: Dict[str, Any],
    volume_percent: Any,
    *,
    client: Any = None,
) -> int:
    """Keep the shared player and every selected destination at one volume."""
    store = client or globals().get("redis_client")
    volume = _as_int(
        volume_percent,
        _as_int(player.get("volume_percent"), 75, 0, 100),
        0,
        100,
    )
    player["volume_percent"] = volume
    targets = _list(player.get("targets") or player.get("target"))
    if targets:
        cfg = _settings(store)
        _save_player_calibrations(
            store,
            {
                target: {
                    **_target_calibration(target, cfg, default_volume=volume),
                    "volume_percent": volume,
                }
                for target in targets
            },
        )
    return volume


def _set_player_volume(
    player: Dict[str, Any],
    volume_percent: Any,
    *,
    store: Any = None,
) -> Dict[str, Any]:
    """Set every destination in the group to one absolute volume ("All" control).

    Speakers already receive the same value — a group at 12/10/7/15 set to 10
    ends up at 10/10/10/10 — and the level is persisted per target so it
    survives the next track start.
    """
    store = store or globals().get("redis_client")
    previous_volume = _as_int(player.get("volume_percent"), 75, 0, 100)
    volume = _as_int(
        volume_percent,
        previous_volume,
        0,
        100,
    )
    live_result = {"sent_count": 0, "warnings": []}
    playing_hardware = _text(player.get("status")).lower() == "playing" and any(
        not _is_screen_target(target)
        for target in _list(player.get("targets") or player.get("target"))
    )
    if playing_hardware:
        live_result = _set_target_volume(player, volume)
        if _as_int(live_result.get("sent_count"), 0, 0, 10000) <= 0:
            warning = "; ".join(
                _text(value)
                for value in list(live_result.get("warnings") or [])
                if _text(value)
            )
            raise ValueError(warning or "The active players could not change volume.")
    _persist_shared_player_volume(player, volume, client=store)
    if volume > 0:
        player["muted"] = False
        player.pop("pre_mute_volume", None)
    elif not _as_bool(player.get("muted")):
        # Dragging the slider to zero mutes the group too; unmute restores it.
        player["muted"] = True
        player.setdefault("pre_mute_volume", previous_volume)
    return live_result


def _apply_player_mute(
    player: Dict[str, Any],
    *,
    mute: bool,
    client: Any = None,
) -> Dict[str, Any]:
    """Mute or unmute every member of the destination group in one action.

    Muting remembers the volume it replaced (per queue) so unmuting puts the
    whole group back where it was. Mute is applied as volume 0, which every
    supported target type (satellites, stereo pairs, Sonos, AirPlay, media
    players) honors.
    """
    store = client or globals().get("redis_client")
    volume = _as_int(player.get("volume_percent"), 75, 0, 100)
    if mute:
        if _as_bool(player.get("muted")) and volume == 0:
            return {"sent_count": 0, "warnings": []}
        player["pre_mute_volume"] = volume or _as_int(
            player.get("pre_mute_volume"), 75, 0, 100
        )
        player["muted"] = True
        new_volume = 0
    else:
        player["muted"] = False
        new_volume = _as_int(player.pop("pre_mute_volume", 0), 0, 0, 100)
        if new_volume <= 0:
            new_volume = volume if volume > 0 else 75
    live_result = {"sent_count": 0, "warnings": []}
    playing_hardware = _text(player.get("status")).lower() == "playing" and any(
        not _is_screen_target(target)
        for target in _list(player.get("targets") or player.get("target"))
    )
    if playing_hardware:
        live_result = _set_target_volume(player, new_volume)
        if _as_int(live_result.get("sent_count"), 0, 0, 10000) <= 0:
            warning = "; ".join(
                _text(value)
                for value in list(live_result.get("warnings") or [])
                if _text(value)
            )
            raise ValueError(warning or "The active players could not change volume.")
    _persist_shared_player_volume(player, new_volume, client=store)
    return live_result


def _apply_mute_warnings(player: Dict[str, Any], live_result: Any) -> None:
    """Surface partial-failure warnings from a group mute on the player card."""
    warnings = [
        _text(value)
        for value in list((live_result or {}).get("warnings") or [])
        if _text(value)
    ]
    if warnings:
        player["warnings"] = warnings


def _listening_history(client: Any = None, person_id: Any = "") -> List[Dict[str, Any]]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, _history_key(person_id), [])
    if not isinstance(payload, list):
        return []
    return [dict(row) for row in payload if isinstance(row, dict)]


def get_music_activity_events(
    *, redis_client=None, limit: int = 100, **_kwargs
) -> List[Dict[str, Any]]:
    """Expose privacy-safe listening activity from this core's own feed key."""
    store = redis_client or globals().get("redis_client")
    payload = _load_json(store, ACTIVITY_KEY, [])
    rows = [dict(row) for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
    return rows[-_as_int(limit, 100, 1, MAX_ACTIVITY_EVENTS):]


def _publish_music_activity(track: Dict[str, Any], *, client: Any = None) -> None:
    store = client or globals().get("redis_client")
    title = _text(track.get("title"))
    if store is None or not title:
        return
    now = time.time()
    rows = get_music_activity_events(
        redis_client=store, limit=MAX_ACTIVITY_EVENTS
    )
    track_id = _text(track.get("id"))
    if rows:
        latest = rows[-1]
        if (
            _text(latest.get("media_id")) == track_id
            and now - _as_float(latest.get("occurred_at")) < 30.0
        ):
            return
    rows.append(
        {
            "source": "personal_music_core",
            "media_id": track_id or title,
            "media_type": "music",
            "title": title,
            "state": "started",
            "occurred_at": now,
            "metadata": {
                "action": "played",
                "artist": _text(track.get("artist") or track.get("album_artist")),
                "album": _text(track.get("album")),
                "genres": [_text(value) for value in track.get("genres") or [] if _text(value)],
                "provider": _provider_id(track.get("provider")),
            },
        }
    )
    _save_json(store, ACTIVITY_KEY, rows[-MAX_ACTIVITY_EVENTS:])


def _record_listening_history(
    track: Dict[str, Any],
    targets: Any = None,
    *,
    person_id: Any = "",
    client: Any = None,
) -> None:
    """Record successful starts without retaining credentials or stream URLs."""
    store = client or globals().get("redis_client")
    track_id = _text(track.get("id"))
    title = _text(track.get("title"))
    if store is None or not (track_id or title):
        return
    selected_person_id = _text(person_id) or _text(_settings(store).get("prompt_person_id"))
    now = time.time()
    history = _listening_history(store, selected_person_id)
    if history:
        latest = history[-1]
        if (
            _text(latest.get("track_id")) == track_id
            and _text(latest.get("person_id")) == selected_person_id
            and now - _as_float(latest.get("played_at")) < 30.0
        ):
            return
    history.append(
        {
            "track_id": track_id,
            "title": title or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album_artist": _text(track.get("album_artist") or track.get("artist")),
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)],
            "provider": _provider_id(track.get("provider")),
            "targets": _list(targets),
            "person_id": selected_person_id,
            "person_name": _people_person_name(selected_person_id, store) if selected_person_id else "",
            "played_at": now,
        }
    )
    _save_json(store, _history_key(selected_person_id), history[-MAX_HISTORY_EVENTS:])
    _publish_music_activity(track, client=store)


def _recommendations(client: Any = None, person_id: Any = "") -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, _recommendations_key(person_id), {})
    return payload if isinstance(payload, dict) else {}


def _music_prompt_profile(client: Any = None, person_id: Any = "") -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    payload = _load_json(store, _profile_key(person_id), {})
    return payload if isinstance(payload, dict) else {}


def _profile_history(
    client: Any,
    *,
    person_id: Any,
    provider_id: Any,
) -> List[Dict[str, Any]]:
    selected_person = _text(person_id)
    selected_provider = _provider_id(provider_id)
    return [
        row
        for row in _listening_history(client, selected_person)
        if _provider_id(row.get("provider")) == selected_provider
        and (not _text(row.get("person_id")) or _text(row.get("person_id")) == selected_person)
    ]


def _recommendation_candidates(
    catalog: Dict[str, Any],
    history: List[Dict[str, Any]],
    *,
    limit: int = MAX_RECOMMENDATION_CANDIDATES,
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    if not tracks:
        return [], {}

    artist_counts: Dict[str, int] = {}
    album_counts: Dict[str, int] = {}
    genre_counts: Dict[str, int] = {}
    recent_track_ids = set()
    recent_albums = set()
    for event in history[-120:]:
        artist = _text(event.get("album_artist") or event.get("artist")).casefold()
        album = _text(event.get("album")).casefold()
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        if album:
            album_counts[album] = album_counts.get(album, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre).casefold()
            if token:
                genre_counts[token] = genre_counts.get(token, 0) + 1
    for event in history[-40:]:
        track_id = _text(event.get("track_id"))
        album = _text(event.get("album")).casefold()
        if track_id:
            recent_track_ids.add(track_id)
        if album:
            recent_albums.add(album)

    def affinity(track: Dict[str, Any]) -> int:
        artist = _text(track.get("album_artist") or track.get("artist")).casefold()
        album = _text(track.get("album")).casefold()
        genres = [_text(value).casefold() for value in track.get("genres") or []]
        return (
            artist_counts.get(artist, 0) * 5
            + album_counts.get(album, 0) * 3
            + sum(genre_counts.get(genre, 0) * 2 for genre in genres if genre)
        )

    ordered_tracks = sorted(
        tracks,
        key=lambda track: (
            _text(track.get("id")) in recent_track_ids,
            -affinity(track),
            _text(track.get("artist") or track.get("album_artist")).casefold(),
            _text(track.get("album")).casefold(),
            _as_int(track.get("disc_number"), 0, 0, 1000),
            _as_int(track.get("track_number"), 0, 0, 10000),
            _text(track.get("title")).casefold(),
        ),
    )

    albums: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for track in tracks:
        artist = _text(track.get("album_artist") or track.get("artist"))
        album = _text(track.get("album"))
        if not album:
            continue
        albums.setdefault((artist.casefold(), album.casefold()), []).append(track)

    ranked_albums = sorted(
        albums.items(),
        key=lambda entry: (
            entry[0][1] in recent_albums,
            -max(affinity(track) for track in entry[1]),
            entry[0],
        ),
    )
    candidates: List[Dict[str, Any]] = []
    candidate_map: Dict[str, Dict[str, Any]] = {}
    album_limit = min(80, max(1, limit // 2))
    for (_artist_key, _album_key), album_tracks in ranked_albums[:album_limit]:
        album_tracks = sorted(
            album_tracks,
            key=lambda track: (
                _as_int(track.get("disc_number"), 0, 0, 1000),
                _as_int(track.get("track_number"), 0, 0, 10000),
                _text(track.get("title")).casefold(),
            ),
        )
        hero = next(
            (track for track in album_tracks if _as_bool(track.get("has_artwork"), False)),
            album_tracks[0],
        )
        artist = _text(hero.get("album_artist") or hero.get("artist"))
        album = _text(hero.get("album")) or "Untitled album"
        candidate_id = "album:" + hashlib.sha256(
            f"{_provider_id(hero.get('provider'))}\x00{artist.casefold()}\x00{album.casefold()}".encode("utf-8")
        ).hexdigest()[:18]
        candidate = {
            "id": candidate_id,
            "type": "album",
            "title": album,
            "artist": artist,
            "album": album,
            "genres": sorted(
                {
                    _text(genre)
                    for track in album_tracks
                    for genre in track.get("genres") or []
                    if _text(genre)
                }
            )[:8],
            "track_count": len(album_tracks),
            "track_ids": [_text(track.get("id")) for track in album_tracks if _text(track.get("id"))],
            "image_track_id": _text(hero.get("id")),
        }
        candidates.append({key: value for key, value in candidate.items() if key not in {"track_ids", "image_track_id"}})
        candidate_map[candidate_id] = candidate

    song_limit = max(0, limit - len(candidates))
    for track in ordered_tracks[:song_limit]:
        track_id = _text(track.get("id"))
        if not track_id:
            continue
        candidate_id = f"song:{track_id}"
        artist = _text(track.get("artist") or track.get("album_artist"))
        candidate = {
            "id": candidate_id,
            "type": "song",
            "title": _text(track.get("title")) or "Untitled",
            "artist": artist,
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)][:8],
            "track_count": 1,
            "track_ids": [track_id],
            "image_track_id": track_id,
        }
        candidates.append({key: value for key, value in candidate.items() if key not in {"track_ids", "image_track_id"}})
        candidate_map[candidate_id] = candidate
    return candidates[:limit], candidate_map


def _music_llm_json(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    system_prompt: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    if llm_client is None:
        raise RuntimeError("No primary LLM is configured for Tater.")
    response = loop.run_until_complete(
        llm_client.chat(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            max_tokens=None,
            temperature=0.4,
        )
    )
    raw = _text(((response or {}).get("message") or {}).get("content"))
    blob = extract_json(raw) or raw
    try:
        parsed = json.loads(blob)
    except Exception as exc:
        raise RuntimeError("The music recommendation model did not return valid JSON.") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError("The music recommendation model returned an unsupported response.")
    return parsed


def _profile_ranked_values(
    history: List[Dict[str, Any]],
    *,
    field: str,
    limit: int,
) -> List[tuple[str, int]]:
    counts: Dict[str, tuple[str, int]] = {}
    for event in history:
        raw_values = event.get(field)
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        for raw_value in values:
            value = _text(raw_value)
            key = value.casefold()
            if not value or not key:
                continue
            original, count = counts.get(key, (value, 0))
            counts[key] = (original, count + 1)
    ranked = sorted(counts.values(), key=lambda row: (-row[1], row[0].casefold()))
    return ranked[: max(1, limit)]


def _generate_music_prompt_profile_impl(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    client: Any = None,
    person_id: Any = "",
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    person_id = _text(person_id) or _text(cfg.get("prompt_person_id"))
    if not person_id:
        raise ValueError("Choose a Person in Personal Music Core Settings before building music prompt context.")
    person_name = _people_person_name(person_id, store)
    if not person_name:
        raise ValueError("The selected Personal Music Core Person no longer exists.")
    provider_id = _person_source_id(person_id, store)
    history = _profile_history(store, person_id=person_id, provider_id=provider_id)
    if not history:
        raise ValueError(f"Play some music for {person_name} before building a music profile.")

    artists = _profile_ranked_values(history, field="album_artist", limit=20)
    if not artists:
        artists = _profile_ranked_values(history, field="artist", limit=20)
    genres = _profile_ranked_values(history, field="genres", limit=20)
    recent_tracks: List[Dict[str, Any]] = []
    seen_tracks = set()
    for event in reversed(history):
        title = _text(event.get("title"))
        artist = _text(event.get("artist") or event.get("album_artist"))
        identity = (_text(event.get("track_id")) or title.casefold(), artist.casefold())
        if not title or identity in seen_tracks:
            continue
        seen_tracks.add(identity)
        recent_tracks.append(
            {
                "title": title,
                "artist": artist,
                "album": _text(event.get("album")),
                "played_at": _as_float(event.get("played_at")),
            }
        )
        if len(recent_tracks) >= 6:
            break

    result = _music_llm_json(
        loop,
        llm_client,
        (
            "Build a compact factual music taste profile for one person from listening counts and recent tracks. "
            "Do not infer demographic, emotional, medical, political, or other sensitive traits. Use only artist "
            "and genre names supplied in the input. Return JSON only in this exact shape: "
            '{"taste_summary":"one short sentence","favorite_artists":["name"],'
            '"favorite_genres":["name"]}.'
        ),
        {
            "person_name": person_name,
            "artist_counts": [{"name": name, "plays": count} for name, count in artists],
            "genre_counts": [{"name": name, "plays": count} for name, count in genres],
            "recent_tracks": recent_tracks,
        },
    )
    allowed_artists = {name.casefold(): name for name, _count in artists}
    allowed_genres = {name.casefold(): name for name, _count in genres}
    favorite_artists = [
        allowed_artists[value.casefold()]
        for value in _list(result.get("favorite_artists"))
        if value.casefold() in allowed_artists
    ][:8]
    favorite_genres = [
        allowed_genres[value.casefold()]
        for value in _list(result.get("favorite_genres"))
        if value.casefold() in allowed_genres
    ][:8]
    if not favorite_artists:
        favorite_artists = [name for name, _count in artists[:6]]
    if not favorite_genres:
        favorite_genres = [name for name, _count in genres[:6]]

    profile = {
        "person_id": person_id,
        "person_name": person_name,
        "provider": provider_id,
        "generated_at": time.time(),
        "history_event_count": len(history),
        "taste_summary": _text(result.get("taste_summary"))[:320],
        "favorite_artists": favorite_artists,
        "favorite_genres": favorite_genres,
        "recent_tracks": recent_tracks,
    }
    _save_json(store, _profile_key(person_id), profile)
    return profile


def _generate_music_prompt_profile(
    client: Any = None,
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    llm_client: Any = None,
    person_id: Any = "",
) -> Dict[str, Any]:
    global _profile_started_at
    if not _profile_lock.acquire(blocking=False):
        raise RuntimeError("The Personal Music Core prompt profile is already being refreshed.")
    store = client or globals().get("redis_client")
    _profile_started_at = time.time()
    owns_loop = loop is None
    active_loop = loop or asyncio.new_event_loop()
    if owns_loop:
        asyncio.set_event_loop(active_loop)
    try:
        model = llm_client if llm_client is not None else _get_primary_llm_client_from_env()
        cfg = _settings(store)
        profile = _generate_music_prompt_profile_impl(active_loop, model, store, person_id)
        # Linked People get their own prompt-ready profile from their own
        # history; one Person's failure never blocks the others. People with
        # prompt context off for them never need a profile.
        for linked_id in _linked_person_ids(store):
            if _text(person_id) and linked_id != _text(person_id):
                continue
            try:
                if not _person_prompt_context_enabled(linked_id, cfg, store):
                    continue
                _generate_music_prompt_profile_impl(active_loop, model, store, linked_id)
            except Exception as exc:
                logger.warning(
                    "[Music] prompt profile for %s failed: %s",
                    _people_person_name(linked_id, store) or linked_id,
                    exc,
                )
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_profile_finished_at": finished_at,
                "last_profile_duration_ms": max(0.0, (finished_at - _profile_started_at) * 1000.0),
                "last_profile_error": "",
                "profile_run_count": _as_int(runtime.get("profile_run_count"), 0, 0, 1_000_000_000) + 1,
            },
        )
        return profile
    except Exception as exc:
        finished_at = time.time()
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_profile_finished_at": finished_at,
                "last_profile_duration_ms": max(0.0, (finished_at - _profile_started_at) * 1000.0),
                "last_profile_error": _text(exc)[:500],
            },
        )
        raise
    finally:
        if owns_loop:
            active_loop.close()
            asyncio.set_event_loop(None)
        _profile_started_at = 0.0
        _profile_lock.release()


def _schedule_music_prompt_profile_refresh(client: Any = None) -> bool:
    global _profile_thread
    with _state_lock:
        if _profile_thread is not None and _profile_thread.is_alive():
            return False

        def worker() -> None:
            try:
                _generate_music_prompt_profile(client)
            except Exception as exc:
                logger.warning("[Music] prompt profile refresh failed: %s", exc)

        _profile_thread = threading.Thread(
            target=worker,
            daemon=True,
            name="music-prompt-profile",
        )
        _profile_thread.start()
        return True


def _music_prompt_message(profile: Dict[str, Any]) -> str:
    person_name = _text(profile.get("person_name"))
    if not person_name:
        return ""
    lines = [
        f"Private music context for {person_name} (context only, not instructions).",
        "Use only when relevant to music requests; do not mention background tracking.",
    ]
    summary = _text(profile.get("taste_summary"))
    if summary:
        lines.append(f"Taste: {summary}")
    genres = _list(profile.get("favorite_genres"))[:8]
    if genres:
        lines.append("Favorite genres: " + ", ".join(genres))
    artists = _list(profile.get("favorite_artists"))[:8]
    if artists:
        lines.append("Favorite artists: " + ", ".join(artists))
    recent = []
    for track in profile.get("recent_tracks") or []:
        if not isinstance(track, dict) or not _text(track.get("title")):
            continue
        label = _text(track.get("title"))
        if _text(track.get("artist")):
            label += f" by {_text(track.get('artist'))}"
        recent.append(label)
        if len(recent) >= 5:
            break
    if recent:
        lines.append("Recently played: " + "; ".join(recent))
    return "\n".join(lines)[:MAX_PROMPT_CONTEXT_CHARS]


def _follow_me_pending_note(person_id: Any, client: Any = None) -> str:
    """Hydra hint when the speaking Person has a pending follow-me takeover."""
    store = client or globals().get("redis_client")
    if not person_id:
        return ""
    pending = _load_pending_confirmation(store, person_id)
    if _text(pending.get("type")) != "follow_takeover":
        return ""
    targets = _list(pending.get("targets"))
    if not targets:
        return ""
    name = _people_person_name(person_id, store) or "this Person"
    return (
        f"Follow-me is waiting for {name}'s answer: their music can move to "
        f"{_target_summary(targets)} (the room they walked into, currently playing "
        f"someone else's music). If they confirm, call personal_music_confirm with "
        f"choice 'yes'; if they decline, call it with choice 'no'."
    )


def _music_prompt_fragment_message(
    store: Any,
    origin: Optional[Dict[str, Any]],
    memory_context: Optional[Dict[str, Any]],
    personal_context: Optional[Dict[str, Any]],
) -> str:
    cfg = _settings(store)
    configured_person_id = _text(cfg.get("prompt_person_id"))
    active_person_id = _context_person_id(origin, memory_context, personal_context)
    # Whoever is speaking decides whether music context is injected at all;
    # an unlinked speaker follows the global toggle.
    if not _person_prompt_context_enabled(active_person_id, cfg, store):
        return ""
    if active_person_id and _music_prompt_profile(store, active_person_id):
        # Linked People read their own scoped profile, whoever is configured.
        profile = _music_prompt_profile(store, active_person_id)
        configured_person_id = active_person_id
    else:
        if not configured_person_id or active_person_id != configured_person_id:
            return ""
        profile = _music_prompt_profile(store)
    if not profile:
        return ""
    if (
        _text(profile.get("person_id")) != configured_person_id
        or _provider_id(profile.get("provider")) != _person_source_id(configured_person_id, store)
    ):
        return ""
    live_recent_tracks = []
    seen_tracks = set()
    for event in reversed(
        _profile_history(
            store,
            person_id=configured_person_id,
            provider_id=_person_source_id(configured_person_id, store),
        )
    ):
        title = _text(event.get("title"))
        artist = _text(event.get("artist") or event.get("album_artist"))
        identity = (_text(event.get("track_id")) or title.casefold(), artist.casefold())
        if not title or identity in seen_tracks:
            continue
        seen_tracks.add(identity)
        live_recent_tracks.append({"title": title, "artist": artist})
        if len(live_recent_tracks) >= 5:
            break
    if live_recent_tracks:
        profile = {**profile, "recent_tracks": live_recent_tracks}
    return _music_prompt_message(profile)


def get_hydra_system_prompt_fragments(
    *,
    role: str,
    redis_client: Any = None,
    origin: Optional[Dict[str, Any]] = None,
    memory_context: Optional[Dict[str, Any]] = None,
    personal_context: Optional[Dict[str, Any]] = None,
    **_kwargs,
) -> Dict[str, List[str]]:
    normalized_role = _text(role).lower()
    if normalized_role not in {"", "chat", "hermes", "memory_context", "music_context"}:
        return {}
    store = redis_client or globals().get("redis_client")
    message = _music_prompt_fragment_message(store, origin, memory_context, personal_context)
    # A pending follow-me takeover must survive even when no music profile is
    # configured, so the Person's yes/no reaches the confirm tool.
    follow_me_note = _follow_me_pending_note(
        _context_person_id(origin, memory_context, personal_context),
        store,
    )
    if not message and not follow_me_note:
        return {}
    if follow_me_note:
        message = f"{message} {follow_me_note}".strip()
    return {
        "chat": [message],
        "hermes": [message],
        "memory_context": [message],
        "music_context": [message],
    }


def _generate_recommendations_impl(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    client: Any = None,
    person_id: Any = "",
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    recommendations_label = _recommendations_label(store)
    cfg = _settings(store)
    person_id = _text(person_id) or _text(cfg.get("prompt_person_id"))
    provider_id = _person_source_id(person_id, store)
    if provider_id not in CATALOG_PROVIDER_IDS:
        raise ValueError(f"{recommendations_label} require a catalog-based music provider.")
    provider = _provider(store, provider_id, person_id)
    if not provider.connected:
        raise ValueError(f"Connect {PROVIDER_LABELS[provider_id]} before making recommendations.")
    history = [
        row
        for row in _listening_history(store, person_id)
        if _provider_id(row.get("provider")) == provider_id
    ]
    if not history:
        raise ValueError(f"Play at least one song before asking {assistant_name} for recommendations.")
    catalog = _person_catalog(store, provider_id, person_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(store, provider_id, person_id)
    candidates, candidate_map = _recommendation_candidates(catalog, history)
    if not candidates:
        raise ValueError("The active music library has no recommendation candidates.")

    playlist_count = _person_recommendation_int(
        person_id,
        "recommendation_playlist_count",
        cfg,
        default=3,
        minimum=1,
        maximum=6,
        client=store,
    )
    item_count = _person_recommendation_int(
        person_id,
        "recommendation_items_per_playlist",
        cfg,
        default=6,
        minimum=3,
        maximum=12,
        client=store,
    )
    artist_counts: Dict[str, int] = {}
    genre_counts: Dict[str, int] = {}
    for event in history[-120:]:
        artist = _text(event.get("album_artist") or event.get("artist"))
        if artist:
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre)
            if token:
                genre_counts[token] = genre_counts.get(token, 0) + 1
    recent = [
        {
            "title": _text(event.get("title")),
            "artist": _text(event.get("artist") or event.get("album_artist")),
            "album": _text(event.get("album")),
            "genres": list(event.get("genres") or [])[:6],
        }
        for event in reversed(history[-40:])
    ]
    result = _music_llm_json(
        loop,
        llm_client,
        (
            f"You are {assistant_name}, a warm, imaginative personal music curator. Build named playlists from the "
            "listener's recent history and the supplied catalog candidates. Choose only exact candidate IDs "
            "from the catalog. Mix familiar taste with useful discovery, avoid filling every playlist with "
            "the same artist or album, and let playlists mix albums and individual songs when that makes sense. "
            "Give every playlist a short memorable name and a one-sentence description. Give every selection "
            "a short reason. Return JSON only in this exact shape: "
            '{"summary":"one friendly sentence","playlists":[{"name":"playlist name",'
            '"description":"one sentence","items":[{"candidate_id":"exact id","reason":"short reason"}]}]}. '
            f"Return up to {playlist_count} playlists with up to {item_count} unique selections in each."
        ),
        {
            "listening_patterns": {
                "top_artists": sorted(artist_counts.items(), key=lambda row: (-row[1], row[0]))[:20],
                "top_genres": sorted(genre_counts.items(), key=lambda row: (-row[1], row[0]))[:20],
            },
            "recent_listening": recent,
            "catalog_candidates": candidates,
        },
    )

    playlists: List[Dict[str, Any]] = []
    for position, raw_playlist in enumerate(result.get("playlists") or []):
        if not isinstance(raw_playlist, dict) or len(playlists) >= playlist_count:
            continue
        selections: List[Dict[str, Any]] = []
        flattened_track_ids: List[str] = []
        seen_candidates = set()
        seen_tracks = set()
        for raw_item in raw_playlist.get("items") or []:
            if isinstance(raw_item, str):
                raw_item = {"candidate_id": raw_item}
            if not isinstance(raw_item, dict) or len(selections) >= item_count:
                continue
            candidate_id = _text(raw_item.get("candidate_id"))
            candidate = candidate_map.get(candidate_id)
            if not candidate or candidate_id in seen_candidates:
                continue
            seen_candidates.add(candidate_id)
            track_ids = [
                track_id
                for track_id in candidate.get("track_ids") or []
                if track_id and track_id not in seen_tracks
            ]
            if not track_ids:
                continue
            seen_tracks.update(track_ids)
            flattened_track_ids.extend(track_ids)
            selections.append(
                {
                    "candidate_id": candidate_id,
                    "type": candidate["type"],
                    "title": candidate["title"],
                    "artist": candidate["artist"],
                    "album": candidate["album"],
                    "track_count": candidate["track_count"],
                    "track_ids": track_ids,
                    "image_track_id": candidate.get("image_track_id"),
                    "reason": _text(raw_item.get("reason"))[:240]
                    or f"{assistant_name} thinks this fits the mood of this mix.",
                }
            )
        if not selections:
            continue
        playlists.append(
            {
                "id": uuid.uuid4().hex[:12],
                "name": _text(raw_playlist.get("name"))[:80] or f"{assistant_name} Mix {position + 1}",
                "description": _text(raw_playlist.get("description"))[:300]
                or "A fresh mix shaped by what has been playing lately.",
                "items": selections,
                "track_ids": flattened_track_ids,
            }
        )
    if not playlists:
        raise RuntimeError("The music recommendation model did not select any valid catalog items.")

    now = time.time()
    published = {
        "provider": provider_id,
        "person_id": person_id,
        "generated_at": now,
        "summary": _text(result.get("summary"))[:500]
        or f"{assistant_name} made a few fresh mixes from what has been playing lately.",
        "history_event_count": len(history),
        "playlists": playlists,
    }
    _save_json(store, _recommendations_key(person_id), published)
    _save_hash(
        store,
        RUNTIME_KEY,
        {
            "last_recommendation_at": now,
            "last_recommendation_attempt_at": now,
            "last_recommendation_error": "",
            "last_recommendation_error_at": "",
        },
    )
    return published


def _generate_recommendations(
    client: Any = None,
    *,
    loop: Optional[asyncio.AbstractEventLoop] = None,
    llm_client: Any = None,
    person_id: Any = "",
    force: bool = False,
) -> Dict[str, Any]:
    global _recommendation_started_at
    if not _recommendation_lock.acquire(blocking=False):
        raise RuntimeError("Tater music recommendations are already being refreshed.")
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    _recommendation_started_at = time.time()
    owns_loop = loop is None
    active_loop = loop or asyncio.new_event_loop()
    if owns_loop:
        asyncio.set_event_loop(active_loop)
    try:
        model = llm_client if llm_client is not None else _get_primary_llm_client_from_env()
        result = _generate_recommendations_impl(active_loop, model, store, person_id)
        # Every Person with a linked music source gets their own mixes from
        # their own listening history. Failures for one Person never block the
        # rest; the primary result above is still reported.
        for linked_id in _linked_person_ids(store):
            if _text(person_id) and linked_id != _text(person_id):
                continue
            try:
                if not _person_recommendations_enabled(linked_id, cfg, store):
                    continue
                if not force:
                    # A Person with a longer-than-global refresh override keeps
                    # their existing mixes until their own interval elapses.
                    interval = (
                        _person_recommendation_int(
                            linked_id,
                            "recommendation_interval_hours",
                            cfg,
                            default=12,
                            minimum=1,
                            maximum=168,
                            client=store,
                        )
                        * 3600
                    )
                    last_generated = _as_float(
                        _recommendations(store, linked_id).get("generated_at")
                    )
                    if last_generated and time.time() - last_generated < interval:
                        continue
                _generate_recommendations_impl(active_loop, model, store, linked_id)
            except Exception as exc:
                logger.warning(
                    "[Music] recommendations for %s failed: %s",
                    _people_person_name(linked_id, store) or linked_id,
                    exc,
                )
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_recommendation_finished_at": finished_at,
                "last_recommendation_duration_ms": max(
                    0.0,
                    (finished_at - _recommendation_started_at) * 1000.0,
                ),
                "recommendation_run_count": _as_int(
                    runtime.get("recommendation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                )
                + 1,
            },
        )
        return result
    except Exception as exc:
        now = time.time()
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_recommendation_attempt_at": now,
                "last_recommendation_finished_at": now,
                "last_recommendation_duration_ms": max(
                    0.0,
                    (now - _recommendation_started_at) * 1000.0,
                ),
                "last_recommendation_error": _text(exc)[:500],
                "last_recommendation_error_at": now,
            },
        )
        raise
    finally:
        if owns_loop:
            active_loop.close()
            asyncio.set_event_loop(None)
        _recommendation_started_at = 0.0
        _recommendation_lock.release()


def _schedule_recommendation_refresh(client: Any = None, *, force: bool = False) -> bool:
    """Start one detached refresh so model latency never pauses queue advancement."""
    global _recommendation_thread
    with _state_lock:
        if _recommendation_thread is not None and _recommendation_thread.is_alive():
            return False

        def worker() -> None:
            try:
                _generate_recommendations(client, force=force)
            except Exception as exc:
                logger.warning("[Music] recommendation refresh failed: %s", exc)

        _recommendation_thread = threading.Thread(
            target=worker,
            name="music-recommendations",
            daemon=True,
        )
        _recommendation_thread.start()
        return True


def _radio_session_token(player: Dict[str, Any]) -> str:
    token = _text(player.get("queue_session_id"))
    if token:
        return token
    created_at = _text(player.get("created_at"))
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    identity = "\x00".join(
        [
            created_at,
            _provider_id(player.get("provider")),
            *[_text(track.get("id")) for track in queue[:4] if isinstance(track, dict)],
        ]
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20] if identity else ""


def _continuation_candidate_tracks(
    player: Dict[str, Any],
    client: Any = None,
    *,
    limit: int = MAX_CONTINUATION_CANDIDATES,
    person_id: Any = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Rank real catalog tracks with the active song and queue as the strongest signal."""
    store = client or globals().get("redis_client")
    queue_person = _queue_id_for_person(
        person_id if person_id is not None else player.get("queue_id")
    )
    provider_id = _provider_id(player.get("provider"), _provider_id(_settings(store).get("provider")))
    catalog = _person_catalog(store, provider_id, queue_person)
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    if not tracks:
        return [], {}, []

    queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
    index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
    current = (
        dict(player.get("current"))
        if isinstance(player.get("current"), dict)
        else (queue[index] if queue else {})
    )
    nearby = queue[max(0, index - 2) : min(len(queue), index + 4)]
    current_artist = _text(current.get("album_artist") or current.get("artist")).casefold()
    current_album = _text(current.get("album")).casefold()
    current_genres = {
        _text(value).casefold() for value in current.get("genres") or [] if _text(value)
    }
    nearby_artists = {
        _text(track.get("album_artist") or track.get("artist")).casefold()
        for track in nearby
        if _text(track.get("album_artist") or track.get("artist"))
    }
    nearby_genres = {
        _text(genre).casefold()
        for track in nearby
        for genre in track.get("genres") or []
        if _text(genre)
    }

    # A multi-source Person's history spans every source they listen from.
    person_sources = (
        set(_person_catalog_source_ids(queue_person, store)) if queue_person else {provider_id}
    )
    history = [
        row
        for row in _listening_history(store, queue_person)[-120:]
        if not person_sources or _provider_id(row.get("provider")) in person_sources
    ]
    history_artist_counts: Dict[str, int] = {}
    history_genre_counts: Dict[str, int] = {}
    recent_history_ids = set()
    for event in history:
        artist = _text(event.get("album_artist") or event.get("artist")).casefold()
        if artist:
            history_artist_counts[artist] = history_artist_counts.get(artist, 0) + 1
        for genre in event.get("genres") or []:
            token = _text(genre).casefold()
            if token:
                history_genre_counts[token] = history_genre_counts.get(token, 0) + 1
    for event in history[-50:]:
        if _text(event.get("track_id")):
            recent_history_ids.add(_text(event.get("track_id")))

    queue_ids = {_text(track.get("id")) for track in queue if _text(track.get("id"))}

    def similarity(track: Dict[str, Any]) -> int:
        artist = _text(track.get("album_artist") or track.get("artist")).casefold()
        album = _text(track.get("album")).casefold()
        genres = {_text(value).casefold() for value in track.get("genres") or [] if _text(value)}
        score = 0
        if current_artist and artist == current_artist:
            score += 120
        if current_album and album == current_album:
            score += 45
        score += len(current_genres & genres) * 90
        if artist in nearby_artists:
            score += 55
        score += len(nearby_genres & genres) * 35
        score += min(25, history_artist_counts.get(artist, 0) * 3)
        score += min(30, sum(history_genre_counts.get(genre, 0) for genre in genres))
        return score

    session_token = _radio_session_token(player)

    def ordering(track: Dict[str, Any]) -> tuple[Any, ...]:
        track_id = _text(track.get("id"))
        dispersion = hashlib.sha256(f"{session_token}\x00{track_id}".encode("utf-8")).hexdigest()
        return (
            track_id in recent_history_ids,
            -similarity(track),
            dispersion,
        )

    unique_pool = [track for track in tracks if _text(track.get("id")) not in queue_ids]
    if not unique_pool:
        unique_pool = list(tracks)
    ordered = sorted(unique_pool, key=ordering)
    relevant = [track for track in ordered if similarity(track) > 0]
    discovery = [track for track in ordered if similarity(track) <= 0]
    selected = [*relevant[:160], *discovery[: max(0, limit - min(160, len(relevant)))]]
    selected = selected[:limit]
    candidate_map = {_text(track.get("id")): track for track in selected if _text(track.get("id"))}
    candidates = [
        {
            "id": track_id,
            "title": _text(track.get("title")) or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album": _text(track.get("album")),
            "genres": [_text(value) for value in track.get("genres") or [] if _text(value)][:8],
            "year": _text(track.get("year")),
        }
        for track_id, track in candidate_map.items()
    ]
    return candidates, candidate_map, selected


def _append_continuation_tracks(
    session_token: str,
    tracks: List[Dict[str, Any]],
    *,
    station_name: str = "",
    source: str = "ai",
    allow_repeats: bool = False,
    person_id: Any = None,
    client: Any = None,
) -> int:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(
        person_id if person_id is not None else ""
    )
    with _state_lock:
        player = _player(store, queue_id)
        if (
            _text(player.get("status")).lower() != "playing"
            or _provider_id(player.get("provider")) not in CATALOG_PROVIDER_IDS
            or _radio_session_token(player) != session_token
        ):
            return 0
        queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
        if not queue:
            return 0
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        existing_ids = {_text(track.get("id")) for track in queue if _text(track.get("id"))}
        incoming: List[Dict[str, Any]] = []
        seen_incoming = set()
        for track in tracks:
            if not isinstance(track, dict):
                continue
            track_id = _text(track.get("id"))
            if not track_id or track_id in seen_incoming:
                continue
            if not allow_repeats and track_id in existing_ids:
                continue
            seen_incoming.add(track_id)
            incoming.append(dict(track))
        if not incoming:
            return 0

        maximum = max(
            2,
            _as_int(_settings(store).get("maximum_queue_tracks"), 200, 1, 1000),
        )
        required_space = max(0, len(queue) + len(incoming) - maximum)
        trim_count = min(index, required_space)
        if trim_count:
            queue = queue[trim_count:]
            index -= trim_count
        available = max(0, maximum - len(queue))
        incoming = incoming[:available]
        if not incoming:
            return 0
        queue.extend(incoming)
        player.update(
            {
                "queue": queue,
                "queue_original": [dict(track) for track in queue],
                "index": index,
                "current": queue[index],
                "continuous_radio": True,
                "continuation_pending": False,
                "radio_name": _text(station_name)[:80]
                or _text(player.get("radio_name"))
                or "Tater Continuous Radio",
                "radio_source": source,
                "radio_last_refill_at": time.time(),
                "radio_last_refill_count": len(incoming),
            }
        )
        _save_player(player, store, queue_id)
        return len(incoming)


def _endless_mode_for_queue(player: Dict[str, Any], client: Any = None) -> str:
    """The Endless Playback mode that owns this queue's refills."""
    store = client or globals().get("redis_client")
    queue_person = _queue_id_for_person(player.get("queue_id"))
    return _person_endless_mode(queue_person, _settings(store), store)


def _history_play_stats(
    store: Any,
    person_id: Any,
    *,
    provider_id: Any = "",
) -> tuple[Dict[str, int], Dict[str, int], set]:
    """(per-track play counts, recent-history genre counts, recently played ids)."""
    history = [
        row
        for row in _listening_history(store, person_id)
        if not provider_id or _provider_id(row.get("provider")) == provider_id
    ]
    track_plays: Dict[str, int] = {}
    for event in history:
        track_id = _text(event.get("track_id"))
        if track_id:
            track_plays[track_id] = track_plays.get(track_id, 0) + 1
    genre_counts: Dict[str, int] = {}
    for event in history[-60:]:
        for genre in event.get("genres") or []:
            token = _genre_key(genre)
            if token:
                genre_counts[token] = genre_counts.get(token, 0) + 1
    recent_ids = {
        _text(event.get("track_id"))
        for event in history[-SMART_SHUFFLE_RECENT_EVENTS:]
        if _text(event.get("track_id"))
    }
    return track_plays, genre_counts, recent_ids


def _queue_listen_context(player: Dict[str, Any]) -> tuple[set, set]:
    """(genre keys, artist keys) the queue is currently listening to."""
    queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
    index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
    current = (
        dict(player.get("current"))
        if isinstance(player.get("current"), dict)
        else (queue[index] if queue else {})
    )
    nearby = queue[max(0, index - 2) : min(len(queue), index + 4)]
    rows = [current, *nearby]
    genre_keys = {
        _genre_key(genre)
        for row in rows
        for genre in row.get("genres") or []
        if _genre_key(genre)
    }
    artist_keys = {
        _text(row.get("album_artist") or row.get("artist")).casefold()
        for row in rows
        if _text(row.get("album_artist") or row.get("artist"))
    }
    return genre_keys, artist_keys


def _library_mix_tracks(
    player: Dict[str, Any],
    store: Any = None,
    *,
    count: int = CONTINUATION_BATCH_TRACKS,
    person_id: Any = None,
    allow_repeats: bool = False,
) -> List[Dict[str, Any]]:
    """Infinite Mix from the library: least-played first, biased to the genres the
    queue was just hearing, with the remaining gaps filled by varied random picks."""
    store = store or globals().get("redis_client")
    queue_person = _queue_id_for_person(
        person_id if person_id is not None else player.get("queue_id")
    )
    provider_id = _provider_id(player.get("provider"), _provider_id(_settings(store).get("provider")))
    catalog = _person_catalog(store, provider_id, queue_person)
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    if not tracks:
        return []
    queue_ids = {
        _text(track.get("id"))
        for track in player.get("queue") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    pool = [track for track in tracks if allow_repeats or _text(track.get("id")) not in queue_ids]
    if not pool:
        pool = list(tracks)
    track_plays, history_genre_counts, recent_ids = _history_play_stats(
        store,
        queue_person,
        provider_id=provider_id,
    )
    context_genres, _context_artists = _queue_listen_context(player)
    context_genres = set(context_genres) | set(history_genre_counts)

    session_token = _radio_session_token(player)

    def dispersion(track: Dict[str, Any]) -> str:
        token = _text(track.get("id")) or _text(track.get("title"))
        return hashlib.sha256(f"{session_token}\x00{token}".encode("utf-8")).hexdigest()

    def orders(track: Dict[str, Any]) -> tuple[Any, ...]:
        track_id = _text(track.get("id"))
        track_genres = {_genre_key(genre) for genre in track.get("genres") or []}
        # Least-played first (0 plays leads), recently played last within a tier,
        # then a stable per-session dispersion so each refill sounds fresh.
        return (
            not bool(track_genres & context_genres),
            track_plays.get(track_id, 0) // 3,
            track_id in recent_ids,
            dispersion(track),
        )

    ordered = sorted(pool, key=orders)
    selected: List[Dict[str, Any]] = []
    artist_counts: Dict[str, int] = {}
    # Walk in order but cap per-artist runs so one prolific artist cannot own the mix.
    for _ in range(3):
        remaining: List[Dict[str, Any]] = []
        for track in ordered:
            if len(selected) >= count:
                remaining.append(track)
                continue
            artist = _text(track.get("album_artist") or track.get("artist")).casefold()
            if artist and artist_counts.get(artist, 0) >= 3:
                remaining.append(track)
                continue
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
            selected.append(track)
        ordered = remaining
        if len(selected) >= count or not ordered:
            break
    return [dict(track) for track in selected[:count]]


def _endless_playlist_payload(store: Any, person_id: Any) -> Dict[str, Any]:
    """The published recommendation payload to pick the endless playlist from."""
    payload = _recommendations(store, person_id)
    if not (payload.get("playlists") or []):
        payload = _recommendations(store)
    return payload if isinstance(payload, dict) else {}


def _parse_folder_playlist_specs(raw: Any) -> List[tuple]:
    """Parse a Folder Playlists value into (playlist name, folder path) pairs."""
    specs: List[tuple] = []
    for chunk in _text(raw).split(","):
        name, separator, folder = chunk.partition("=")
        name, folder = _text(name), _text(folder).strip().strip("/")
        if separator and name and folder:
            specs.append((name, folder))
    return specs


def _folder_playlist_specs(cfg: Dict[str, Any]) -> List[tuple]:
    """The global Folder Playlists setting as (playlist name, folder path) pairs."""
    return _parse_folder_playlist_specs(cfg.get("folder_playlists"))


def _person_folder_playlist_specs(
    person_id: Any, cfg: Dict[str, Any], client: Any = None
) -> List[tuple]:
    """One Person's effective folder playlists: their own list when set on their
    link card, otherwise the global Folder Playlists setting."""
    raw = _text(_person_link(person_id, client).get("folder_playlists")).strip()
    if raw:
        return _parse_folder_playlist_specs(raw)
    return _folder_playlist_specs(cfg)


def _folder_defined_playlists(
    store: Any,
    person_id: Any,
    provider_id: Any,
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Playlists defined by a library folder in the Person's effective Folder
    Playlists list (their link-card override, or the global setting when blank).

    Rebuilt from the live catalog on every call, so a song added to the folder
    joins the playlist on the next sync without touching any stored playlist.
    """
    specs = _person_folder_playlist_specs(person_id, cfg, store)
    if not specs:
        return []
    catalog = _person_catalog(store, provider_id, person_id)
    tracks = [row for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    playlists: List[Dict[str, Any]] = []
    for name, folder in specs:
        needle = f"/{folder.casefold()}/"
        track_ids: List[str] = []
        for track in tracks:
            path = _text(track.get("path")).replace("\\", "/").casefold()
            if path and needle in f"/{path.lstrip('/')}/":
                track_id = _text(track.get("id"))
                if track_id:
                    track_ids.append(track_id)
        if track_ids:
            playlists.append(
                {
                    "id": f"folder_playlist:{hashlib.sha1(name.casefold().encode('utf-8')).hexdigest()[:16]}",
                    "name": name,
                    "description": f"All tracks under the {folder} folder",
                    "track_ids": track_ids,
                }
            )
    return playlists


def _catalog_user_playlists(
    store: Any,
    person_id: Any,
    provider_id: Any = "",
) -> List[Dict[str, Any]]:
    """Playlists the user made themselves: Emby playlists, share .m3u files, and
    the folder-defined playlists from the Folder Playlists setting.

    These refresh on every library sync (folder playlists on every lookup), so
    they need no extra requests at playback time.
    """
    cfg = _settings(store)
    playlists: List[Dict[str, Any]] = []
    seen_names: set = set()
    catalog = _person_catalog(store, provider_id, person_id)
    for row in catalog.get("playlists") or []:
        if (
            isinstance(row, dict)
            and _text(row.get("name"))
            and row.get("track_ids")
            and _text(row.get("name")).casefold() not in seen_names
        ):
            seen_names.add(_text(row.get("name")).casefold())
            playlists.append(dict(row))
    for row in _folder_defined_playlists(store, person_id, provider_id, cfg):
        if _text(row.get("name")).casefold() not in seen_names:
            seen_names.add(_text(row.get("name")).casefold())
            playlists.append(row)
    return playlists


def _find_named_playlist(
    store: Any,
    person_id: Any,
    playlist_name: Any,
    *,
    provider_id: Any = "",
) -> Dict[str, Any]:
    """Match a playlist by name across AI mixes, then user-created playlists."""
    payload = _endless_playlist_payload(store, person_id)
    wanted = _text(playlist_name).casefold()
    if wanted:
        playlist = next(
            (
                row
                for row in payload.get("playlists") or []
                if isinstance(row, dict) and _text(row.get("name")).casefold() == wanted
            ),
            None,
        )
        if isinstance(playlist, dict):
            return playlist
        user_playlists = _catalog_user_playlists(store, person_id, provider_id)
        return next(
            (
                row
                for row in user_playlists
                if _text(row.get("name")).casefold() == wanted
            ),
            {},
        )
    published = [row for row in payload.get("playlists") or [] if isinstance(row, dict)]
    if published:
        return published[0]
    user_playlists = _catalog_user_playlists(store, person_id, provider_id)
    return user_playlists[0] if user_playlists else {}


PLAYLIST_ORDERS = (
    "shuffle",
    "track_asc",
    "track_desc",
    "title_asc",
    "title_desc",
    "artist_asc",
    "artist_desc",
    "album_asc",
    "album_desc",
)


def _playlist_order_value(cfg: Dict[str, Any]) -> str:
    order = _text(cfg.get("recommendation_playlist_order")).casefold()
    return order if order in PLAYLIST_ORDERS else "shuffle"


def _order_playlist_tracks(
    tracks: List[Dict[str, Any]],
    order: Any,
) -> List[Dict[str, Any]]:
    """Apply the fixed Playlist Order setting to a resolved playlist.

    `shuffle` (the default) returns the list untouched; anything else returns a
    deterministically sorted copy so the playlist plays the same way every time.
    """
    order = _text(order).casefold()
    if order in ("", "shuffle") or len(tracks) <= 1:
        return [dict(track) for track in tracks]
    reverse = order.endswith("_desc")
    field = order.split("_", 1)[0]

    def sort_key(track: Dict[str, Any]) -> tuple:
        if field == "track":
            # Track number within the album; untagged tracks keep their relative
            # order at the end (a large stable sentinel).
            return (
                _as_int(track.get("disc_number"), 0, 0, 1000),
                _as_int(track.get("track_number"), 0, 0, 10000) or 100000,
                _text(track.get("title")).casefold(),
            )
        if field == "artist":
            return (
                _text(track.get("artist") or track.get("album_artist")).casefold(),
                _text(track.get("album")).casefold(),
                _as_int(track.get("track_number"), 0, 0, 10000),
            )
        if field == "album":
            return (
                _text(track.get("album")).casefold(),
                _as_int(track.get("disc_number"), 0, 0, 1000),
                _as_int(track.get("track_number"), 0, 0, 10000),
            )
        return (_text(track.get("title")).casefold(),)

    return [dict(track) for track in sorted(tracks, key=sort_key, reverse=reverse)]


def _resolve_playlist_tracks(
    store: Any,
    person_id: Any,
    playlist: Dict[str, Any],
    *,
    provider_id: Any = "",
) -> List[Dict[str, Any]]:
    """Map a published mix's track ids back onto real catalog tracks."""
    resolved: List[Dict[str, Any]] = []
    for raw_track_id in playlist.get("track_ids") or []:
        track_id = _text(raw_track_id)
        if not track_id:
            continue
        track = _find_track_in_catalog(_person_catalog(store, provider_id, person_id), track_id)
        if track is None:
            track = _find_track_in_catalog(_person_catalog(store, "", person_id), track_id)
        if track is not None:
            resolved.append(dict(track))
    return resolved


def _playlist_loop_tracks(
    player: Dict[str, Any],
    store: Any = None,
    *,
    count: int = CONTINUATION_BATCH_TRACKS,
    person_id: Any = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], int]:
    """The endless playlist's tracks, looping from the stored rotation offset."""
    store = store or globals().get("redis_client")
    queue_person = _queue_id_for_person(
        person_id if person_id is not None else player.get("queue_id")
    )
    cfg = _settings(store)
    provider_id = _provider_id(player.get("provider"), _provider_id(cfg.get("provider")))
    playlist_name = _person_endless_playlist(queue_person, cfg, store)
    playlist = _find_named_playlist(store, queue_person, playlist_name, provider_id=provider_id)
    if not playlist:
        return [], {}, 0
    tracks = _order_playlist_tracks(
        _resolve_playlist_tracks(store, queue_person, playlist, provider_id=provider_id),
        _playlist_order_value(cfg),
    )
    if not tracks:
        return [], playlist, 0
    # Rotation offset advances only by tracks actually appended, so a slow
    # refill or a restart keeps the loop continuous instead of skipping.
    offset = _as_int(player.get("playlist_loop_offset"), 0, 0, len(tracks)) % len(tracks)
    queued_ids = {
        _text(track.get("id"))
        for track in player.get("queue") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    batch: List[Dict[str, Any]] = []
    consumed = 0
    # Prefer tracks not already queued; a short playlist loops over itself
    # rather than falling short.
    for allow_queued in (False, True):
        batch = []
        scan = 0
        while len(batch) < count and scan < len(tracks) * 2:
            track = dict(tracks[(offset + scan) % len(tracks)])
            scan += 1
            track_id = _text(track.get("id"))
            if not allow_queued and track_id in queued_ids:
                continue
            if any(_text(row.get("id")) == track_id for row in batch):
                continue
            batch.append(track)
        consumed = scan
        if len(batch) >= count:
            break
    return batch, playlist, (offset + consumed) % len(tracks)


def _smart_pool_batch(
    player: Dict[str, Any],
    store: Any = None,
    *,
    count: int = CONTINUATION_BATCH_TRACKS,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Draw the next Smart Shuffle batch from the pool, rotating across sources."""
    pool = [dict(row) for row in player.get("smart_pool") or [] if isinstance(row, dict)]
    if not pool:
        return [], []
    batch = [row for row in pool[:count]]
    remaining = pool[len(batch):]
    return batch, remaining


def _save_smart_pool(queue_id: Any, remaining_pool: List[Dict[str, Any]], store: Any) -> None:
    """Persist the pool left after a Smart Shuffle batch was appended."""
    with _state_lock:
        player = _player(store, queue_id)
        player["smart_pool"] = [dict(row) for row in remaining_pool]
        _save_player(player, store, queue_id)


def _save_playlist_loop_offset(queue_id: Any, offset: int, store: Any) -> None:
    """Persist the endless playlist's rotation position after a refill."""
    with _state_lock:
        player = _player(store, queue_id)
        player["playlist_loop_offset"] = max(0, _as_int(offset, 0, 0, 100000))
        _save_player(player, store, queue_id)


def _smart_shuffle_order(
    tracks: List[Dict[str, Any]],
    recent_ids: set,
) -> List[Dict[str, Any]]:
    """Shuffled order with recently played tracks pushed to the back."""
    shuffled = [dict(track) for track in tracks]
    random.SystemRandom().shuffle(shuffled)
    fresh = [track for track in shuffled if _text(track.get("id")) not in recent_ids]
    recent = [track for track in shuffled if _text(track.get("id")) in recent_ids]
    return [*fresh, *recent]


def _smart_recent_ids(store: Any, person_id: Any) -> set:
    """Track ids in this Person's recent listening window."""
    _track_plays, _genre_counts, recent_ids = _history_play_stats(store, person_id)
    return recent_ids


def _smart_round_robin(
    pool: List[Dict[str, Any]],
    count: int,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Take the next `count` pool tracks, rotating across queued sources.

    The pool keeps insertion order but every source batch is tagged with
    `source_label`, so each pass contributes one track per source in turn
    (two albums plus two playlists alternate instead of playing in blocks).
    """
    if count <= 0 or not pool:
        return [], [dict(row) for row in pool]
    groups: Dict[str, List[Dict[str, Any]]] = {}
    order: List[str] = []
    for track in pool:
        label = _text(track.get("source_label")) or "__queue"
        if label not in groups:
            groups[label] = []
            order.append(label)
        groups[label].append(track)
    selected: List[Dict[str, Any]] = []
    progress = 0
    while len(selected) < count and any(groups[label] for label in order):
        for label in order:
            bucket = groups[label]
            if bucket and len(selected) < count:
                selected.append(bucket.pop(0))
        progress += 1
        if progress > len(pool):
            break
    remaining = [dict(track) for label in order for track in groups[label]]
    return selected, remaining


def _smart_window_tracks(
    tracks: List[Dict[str, Any]],
    *,
    shuffle: bool,
    recent_ids: set,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Split a selection into the on-screen queue window and the Smart Shuffle pool."""
    ordered = (
        _smart_shuffle_order(tracks, recent_ids)
        if shuffle
        else [dict(track) for track in tracks]
    )
    window = [dict(track) for track in ordered[:SMART_SHUFFLE_QUEUE_WINDOW]]
    pool = [dict(track) for track in ordered[SMART_SHUFFLE_QUEUE_WINDOW:]]
    return window, pool


def _smart_top_up_queue(player: Dict[str, Any], store: Any, queue_id: Any) -> None:
    """Top the remaining queue up to the Smart Shuffle window from the pool."""
    queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
    index = _as_int(player.get("index"), 0, 0, max(0, max(0, len(queue) - 1)))
    remaining_count = max(0, len(queue) - index - 1)
    need = SMART_SHUFFLE_QUEUE_WINDOW - remaining_count
    if need <= 0:
        return
    pool = [dict(row) for row in player.get("smart_pool") or [] if isinstance(row, dict)]
    if not pool:
        return
    drawn, remaining_pool = _smart_round_robin(pool, need)
    if not drawn:
        return
    maximum = max(
        2,
        _as_int(_settings(store).get("maximum_queue_tracks"), 200, 1, 1000),
    )
    # Trim played tracks from the head first so a long listen never exceeds the cap.
    trim_count = min(index, max(0, len(queue) + len(drawn) - maximum))
    if trim_count:
        queue = queue[trim_count:]
        index -= trim_count
    capacity = max(0, maximum - len(queue))
    drawn = drawn[:capacity]
    if not drawn:
        return
    player["queue"] = [*queue, *drawn]
    player["queue_original"] = [dict(track) for track in player["queue"]]
    player["index"] = index
    player["current"] = queue[index] if 0 <= index < len(queue) else (queue[0] if queue else {})
    player["smart_pool"] = remaining_pool


def _add_queue_tracks(
    args: Dict[str, Any],
    *,
    origin: Optional[Dict[str, Any]] = None,
    client: Any = None,
) -> Dict[str, Any]:
    """Queue more music on top of what's playing (albums, playlists, searches).

    With Smart Shuffle on, the new tracks join a pool that feeds the queue
    round-robin across every queued source, so two albums and two playlists
    mix on the fly instead of stacking into one enormous queue.
    """
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    speaking_person_id = _context_person_id(origin)
    person_id = speaking_person_id or _text(cfg.get("prompt_person_id"))
    queue_id = _queue_id_for_person(speaking_person_id)
    selected_provider = _person_source_id(person_id, store)
    maximum = _as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000)
    source_label = _text(args.get("source_label"))
    if not source_label:
        source_label = (
            _text(args.get("album"))
            or _text(args.get("artist"))
            or _text(args.get("genre"))
            or _text(args.get("playlist"))
            or _text(args.get("query"))
            or "Added tracks"
        )[:60]

    playlist_name = _text(args.get("playlist"))
    playlist_ordered = False
    if playlist_name:
        playlist = _find_named_playlist(
            store, person_id, playlist_name, provider_id=selected_provider
        )
        if not playlist:
            raise ValueError(
                f'No mix named "{playlist_name}" is on the Recommendations tab, and no '
                "playlist of that name exists in the library."
            )
        tracks = _resolve_playlist_tracks(store, person_id, playlist, provider_id=selected_provider)
        if not tracks:
            raise ValueError(f'The playlist "{_text(playlist.get("name"))}" has no playable tracks left.')
        tracks = _order_playlist_tracks(tracks, _playlist_order_value(cfg))
        playlist_ordered = _playlist_order_value(cfg) != "shuffle"
    else:
        tracks = _search_tracks(
            query=_text(args.get("query") or args.get("music")),
            title=_text(args.get("title") or args.get("track") or args.get("song")),
            artist=_text(args.get("artist")),
            album=_text(args.get("album")),
            genre=_text(args.get("genre")),
            limit=maximum,
            client=store,
            provider_id=selected_provider,
            person_id=person_id,
        )
    if not tracks:
        raise ValueError("No matching music was found to add to the queue.")
    for track in tracks:
        track["source_label"] = source_label

    player = _player(store, queue_id)
    queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
    status = _text(player.get("status")).lower()
    if not queue or status not in {"playing", "paused", "queued"}:
        smart = _person_smart_shuffle_enabled(person_id, cfg, store)
        return {
            "player": _create_and_start_queue(
                tracks,
                targets=_list(player.get("targets") or player.get("target")),
                shuffle=smart and not playlist_ordered,
                volume_percent=_as_int(player.get("volume_percent"), _as_int(cfg.get("default_volume_percent"), 75, 0, 100), 0, 100),
                person_id=person_id,
                client=store,
            ),
            "added": len(tracks),
            "queued_directly": True,
        }

    with _state_lock:
        player = _player(store, queue_id)
        queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        existing_ids = {_text(row.get("id")) for row in queue if _text(row.get("id"))}
        # Smart Shuffle pool tracks are already waiting to play — don't re-add them.
        existing_ids.update(
            _text(row.get("id"))
            for row in player.get("smart_pool") or []
            if isinstance(row, dict) and _text(row.get("id"))
        )
        added = [dict(track) for track in tracks if _text(track.get("id")) not in existing_ids]
        if not added:
            return {"player": player, "added": 0, "queued_directly": False}
        if _person_smart_shuffle_enabled(person_id, cfg, store):
            pool = [dict(row) for row in player.get("smart_pool") or [] if isinstance(row, dict)]
            player["smart_pool"] = [*pool, *added]
            _smart_top_up_queue(player, store, queue_id)
        else:
            remaining = queue[index + 1 :]
            head = queue[: index + 1]
            capacity = max(0, maximum - (len(head) + len(remaining) + len(added)))
            player["queue"] = [*head, *remaining, *added][:maximum]
            player["queue_original"] = [dict(track) for track in player["queue"]]
            player["index"] = index
            if capacity < 0:
                del added[capacity:]
        player["smart_shuffle"] = _person_smart_shuffle_enabled(person_id, cfg, store)
        _save_player(player, store, queue_id)
        return {
            "player": _player(store, queue_id),
            "added": len(added),
            "queued_directly": False,
        }


def _fallback_continuation_tracks(
    player: Dict[str, Any],
    client: Any = None,
    *,
    count: int = CONTINUATION_BATCH_TRACKS,
) -> List[Dict[str, Any]]:
    """Non-AI refill tracks, honouring the queue's Endless Playback mode."""
    store = client or globals().get("redis_client")
    queue_person = _queue_id_for_person(player.get("queue_id"))
    # A Smart Shuffle pool always feeds the queue before any Endless Playback mode.
    pool_batch, _remaining = _smart_pool_batch(player, store)
    if pool_batch:
        return pool_batch
    mode = _endless_mode_for_queue(player, store)
    if mode == "playlist_loop":
        batch, _playlist, _offset = _playlist_loop_tracks(player, store, count=count)
        if batch:
            return batch
    if mode in {"library_mix", "automatic", "similar_played"}:
        batch = _library_mix_tracks(player, store, count=count, person_id=queue_person)
        if batch:
            return batch
    _candidates, _candidate_map, ordered = _continuation_candidate_tracks(
        player,
        store,
        limit=MAX_CONTINUATION_CANDIDATES,
        person_id=queue_person,
    )
    if ordered:
        return [dict(track) for track in ordered[: max(1, count)]]
    provider_id = _provider_id(player.get("provider"))
    catalog = _person_catalog(store, provider_id, queue_person)
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    return tracks[: max(1, count)]


def _select_continuation_tracks(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    player: Dict[str, Any],
    client: Any = None,
) -> tuple[List[Dict[str, Any]], str]:
    store = client or globals().get("redis_client")
    candidates, candidate_map, ordered = _continuation_candidate_tracks(player, store)
    if not candidates:
        raise ValueError("The active music library has no tracks for continuous radio.")
    queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
    index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
    current = (
        dict(player.get("current"))
        if isinstance(player.get("current"), dict)
        else (queue[index] if queue else {})
    )
    compact = lambda track: {
        "title": _text(track.get("title")),
        "artist": _text(track.get("artist") or track.get("album_artist")),
        "album": _text(track.get("album")),
        "genres": [_text(value) for value in track.get("genres") or [] if _text(value)][:8],
        "year": _text(track.get("year")),
    }
    history = [
        {
            "title": _text(event.get("title")),
            "artist": _text(event.get("artist") or event.get("album_artist")),
            "album": _text(event.get("album")),
            "genres": list(event.get("genres") or [])[:6],
        }
        for event in reversed(
            _listening_history(store, _queue_id_for_person(player.get("queue_id")))[-30:]
        )
    ]
    result = _music_llm_json(
        loop,
        llm_client,
        (
            "You are Tater's continuous-radio music programmer. The currently playing song is the strongest "
            "signal. Choose a smooth sequence of similar songs from only the supplied catalog IDs, using artist, "
            "genre, album context, era, and the nearby queue to preserve the current musical direction. Listening "
            "history is secondary context. Avoid abrupt genre changes and unnecessary repeats, but allow adjacent "
            "artists and discoveries that genuinely fit. Give the station a short creative name. Return JSON only "
            "in this exact shape: "
            '{"station_name":"short name","items":[{"track_id":"exact catalog id"}]}. '
            f"Return up to {CONTINUATION_BATCH_TRACKS} unique tracks in playback order."
        ),
        {
            "currently_playing": compact(current),
            "recent_queue": [compact(track) for track in queue[max(0, index - 2) : index]],
            "up_next": [compact(track) for track in queue[index + 1 : index + 4]],
            "recent_listening": history,
            "catalog_candidates": candidates,
        },
    )
    selections: List[Dict[str, Any]] = []
    seen = set()
    for row in result.get("items") or []:
        if isinstance(row, str):
            row = {"track_id": row}
        if not isinstance(row, dict) or len(selections) >= CONTINUATION_BATCH_TRACKS:
            continue
        track_id = _text(row.get("track_id") or row.get("candidate_id"))
        track = candidate_map.get(track_id)
        if not track or track_id in seen:
            continue
        seen.add(track_id)
        selections.append(dict(track))
    for track in ordered:
        track_id = _text(track.get("id"))
        if len(selections) >= CONTINUATION_BATCH_TRACKS:
            break
        if track_id and track_id not in seen:
            seen.add(track_id)
            selections.append(dict(track))
    if not selections:
        raise RuntimeError("The continuous-radio model did not select any playable tracks.")
    return (
        selections,
        _text(result.get("station_name")) or "Tater Continuous Radio",
    )


def _generate_continuation_impl(
    loop: asyncio.AbstractEventLoop,
    llm_client: Any,
    player: Dict[str, Any],
    session_token: str,
    client: Any = None,
) -> int:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(player.get("queue_id"))
    mode = _endless_mode_for_queue(player, store)

    # Smart Shuffle first: an active pool always feeds the queue before any
    # Endless Playback mode, so queued sources keep mixing as they drain.
    batch, remaining_pool = _smart_pool_batch(player, store)
    if batch:
        added = _append_continuation_tracks(
            session_token,
            batch,
            station_name=_text(player.get("radio_name")) or "Smart Shuffle",
            source="smart_shuffle_pool",
            person_id=queue_id,
            client=store,
        )
        if added:
            _save_smart_pool(queue_id, remaining_pool, store)
        return added

    if mode == "playlist_loop":
        batch, playlist, next_offset = _playlist_loop_tracks(player, store)
        if batch:
            added = _append_continuation_tracks(
                session_token,
                batch,
                station_name=f"Playlist: {_text(playlist.get('name')) or 'Chosen Playlist'}",
                source="playlist_loop",
                allow_repeats=True,
                person_id=queue_id,
                client=store,
            )
            if added:
                _save_playlist_loop_offset(queue_id, next_offset, store)
            return added
        # No usable playlist (unset, renamed, or empty): keep music playing
        # with the library mix rather than letting the queue die.
        batch = _library_mix_tracks(player, store)
        if batch:
            return _append_continuation_tracks(
                session_token,
                batch,
                station_name="Infinite Mix",
                source="library_mix",
                person_id=queue_id,
                client=store,
            )
        raise ValueError("No endless playlist was found to keep this queue playing.")

    if mode in {"automatic", "similar_played"}:
        queue = [dict(row) for row in player.get("queue") or [] if isinstance(row, dict)]
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        seed = (
            dict(player.get("current"))
            if isinstance(player.get("current"), dict)
            else (queue[index] if queue else {})
        )
        provider_tracks = _streaming_similar_tracks(
            [seed] if seed else [],
            person_id=queue_id,
            client=store,
        )
        if provider_tracks:
            return _append_continuation_tracks(
                session_token,
                provider_tracks,
                station_name="Similar to what you played",
                source="streaming_provider",
                person_id=queue_id,
                client=store,
            )
        if mode == "similar_played":
            # No streaming provider is connected (yet); fall back to the
            # library mix so the music never stops.
            batch = _library_mix_tracks(player, store)
            if batch:
                return _append_continuation_tracks(
                    session_token,
                    batch,
                    station_name="Infinite Mix",
                    source="library_mix",
                    person_id=queue_id,
                    client=store,
                )
            raise ValueError("No streaming provider is connected for similar tracks.")
        # "automatic": stream providers had nothing, so fall through to the
        # library Infinite Mix below.

    if mode == "library_mix":
        batch = _library_mix_tracks(player, store)
        if batch:
            return _append_continuation_tracks(
                session_token,
                batch,
                station_name="Infinite Mix",
                source="library_mix",
                person_id=queue_id,
                client=store,
            )
        raise ValueError("The active music library has no tracks for an Infinite Mix.")

    selections, station_name = _select_continuation_tracks(
        loop,
        llm_client,
        player,
        client,
    )
    return _append_continuation_tracks(
        session_token,
        selections,
        station_name=station_name,
        source="ai",
        person_id=player.get("queue_id"),
        client=client,
    )


def _generate_continuation(
    player: Dict[str, Any],
    session_token: str,
    client: Any = None,
    *,
    llm_client: Any = None,
) -> int:
    global _continuation_started_at
    if not _continuation_lock.acquire(blocking=False):
        return 0
    _continuation_started_at = time.time()
    store = client or globals().get("redis_client")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        model = llm_client if llm_client is not None else _get_primary_llm_client_from_env()
        added = _generate_continuation_impl(loop, model, player, session_token, store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_continuation_at": time.time(),
                "last_continuation_error": "",
                "last_continuation_error_at": "",
            },
        )
        return added
    except Exception as exc:
        fallback = _fallback_continuation_tracks(
            player,
            store,
            count=CONTINUATION_BATCH_TRACKS,
        )
        added = _append_continuation_tracks(
            session_token,
            fallback,
            station_name="Tater Continuous Radio",
            source="smart_fallback",
            allow_repeats=True,
            person_id=player.get("queue_id"),
            client=store,
        )
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_continuation_at": time.time() if added else "",
                "last_continuation_error": _text(exc)[:500],
                "last_continuation_error_at": time.time(),
            },
        )
        logger.warning("[Music] continuous-radio AI refill failed; added %s fallback tracks: %s", added, exc)
        return added
    finally:
        with _state_lock:
            latest = _player(store, _queue_id_for_person(player.get("queue_id")))
            if (
                _radio_session_token(latest) == session_token
                and latest.get("continuation_pending")
            ):
                latest["continuation_pending"] = False
                _save_player(latest, store, latest.get("queue_id"))
        finished_at = time.time()
        runtime = _runtime(store)
        _save_hash(
            store,
            RUNTIME_KEY,
            {
                "last_continuation_finished_at": finished_at,
                "last_continuation_duration_ms": max(
                    0.0,
                    (finished_at - _continuation_started_at) * 1000.0,
                ),
                "continuation_run_count": _as_int(
                    runtime.get("continuation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                )
                + 1,
            },
        )
        loop.close()
        asyncio.set_event_loop(None)
        _continuation_started_at = 0.0
        _continuation_lock.release()


def _schedule_continuation_refresh(
    player: Optional[Dict[str, Any]] = None,
    client: Any = None,
    person_id: Any = None,
) -> bool:
    store = client or globals().get("redis_client")
    with _state_lock:
        queue_id = _queue_id_for_person(
            person_id
            if person_id is not None
            else (player.get("queue_id") if isinstance(player, dict) else "")
        )
        current_player = (
            dict(player) if isinstance(player, dict) else _player(store, queue_id)
        )
        queue = [dict(row) for row in current_player.get("queue") or [] if isinstance(row, dict)]
        if (
            _text(current_player.get("status")).lower() != "playing"
            or _provider_id(current_player.get("provider")) not in CATALOG_PROVIDER_IDS
            or _text(current_player.get("repeat")).lower() == "one"
            or not queue
        ):
            return False
        index = _as_int(current_player.get("index"), 0, 0, max(0, len(queue) - 1))
        if len(queue) - index - 1 > CONTINUATION_TRIGGER_REMAINING_TRACKS:
            return False
        maximum = max(
            2,
            _as_int(_settings(store).get("maximum_queue_tracks"), 200, 1, 1000),
        )
        if len(queue) >= maximum and index == 0:
            return False
        worker_thread = _continuation_threads.get(queue_id)
        if worker_thread is not None and worker_thread.is_alive():
            return False
        session_token = _radio_session_token(current_player) or uuid.uuid4().hex
        current_player["queue_session_id"] = session_token
        current_player["continuous_radio"] = True
        current_player["continuation_pending"] = True
        _save_player(current_player, store, queue_id)
        snapshot = json.loads(json.dumps(current_player))

        def worker() -> None:
            _generate_continuation(snapshot, session_token, store)

        worker_thread = threading.Thread(
            target=worker,
            name=f"music-continuous-radio:{queue_id or 'shared'}",
            daemon=True,
        )
        _continuation_threads[queue_id] = worker_thread
        worker_thread.start()
        return True


def _append_end_of_queue_fallback(player: Dict[str, Any], client: Any = None) -> int:
    store = client or globals().get("redis_client")
    session_token = _radio_session_token(player)
    if not session_token:
        return 0
    tracks = _fallback_continuation_tracks(player, store, count=4)
    return _append_continuation_tracks(
        session_token,
        tracks,
        station_name=_text(player.get("radio_name")) or "Tater Continuous Radio",
        source="end_of_queue_fallback",
        allow_repeats=True,
        person_id=player.get("queue_id"),
        client=store,
    )


def _origin_value(origin: Dict[str, Any], *keys: str) -> str:
    nested = origin.get("origin") if isinstance(origin.get("origin"), dict) else {}
    for key in keys:
        value = _text(origin.get(key) if origin.get(key) not in (None, "") else nested.get(key))
        if value:
            return value
    return ""


def _stereo_member_target_map() -> Dict[str, str]:
    """Map each configured stereo member to its pair playback target."""
    try:
        from tater_voice import stereo_pairs

        pairs = stereo_pairs.list_pairs()
    except Exception:
        return {}

    routes: Dict[str, str] = {}
    for pair in pairs if isinstance(pairs, list) else []:
        if not isinstance(pair, dict):
            continue
        pair_selector = _text(pair.get("selector"))
        if not pair_selector:
            pair_id = _text(pair.get("id"))
            pair_selector = f"stereo:{pair_id}" if pair_id else ""
        if not pair_selector:
            continue
        pair_target = (
            pair_selector
            if pair_selector.lower().startswith("voice_core:")
            else f"voice_core:{pair_selector}"
        )
        for key in ("left_selector", "right_selector"):
            member_selector = _text(pair.get(key))
            if not member_selector:
                continue
            member_target = (
                member_selector
                if member_selector.lower().startswith("voice_core:")
                else f"voice_core:{member_selector}"
            )
            routes[member_selector.casefold()] = pair_target
            routes[member_target.casefold()] = pair_target
    return routes


def _normalize_stereo_targets(value: Any) -> List[str]:
    """Replace paired satellite selections with one deduplicated stereo target."""
    routes = _stereo_member_target_map()
    return _list(
        [
            routes.get(target.casefold(), target)
            for target in _list(value)
            if not target.casefold().startswith("integration:roon:")
        ]
    )


def _compact_target_option(row: Dict[str, Any]) -> Dict[str, Any]:
    """Use shorter satellite labels in Music Core's space-limited pickers."""
    option = dict(row)
    label = _text(option.get("label"))
    replacements = {
        "Tater Satellite:": "Tater Sat:",
        "AirPlay Bridge:": "AirPlay:",
    }
    for prefix, replacement in replacements.items():
        if label.casefold().startswith(prefix.casefold()):
            option["label"] = f"{replacement}{label[len(prefix):]}"
            break
    return option


def _settings_target_option(row: Dict[str, Any]) -> Dict[str, Any]:
    """Turn a playback target into a compact, readable settings choice."""
    option = dict(row)
    target = _text(option.get("value"))
    label = _text(option.get("label")) or target
    lower_target = target.casefold()
    if lower_target.startswith(("voice_core:stereo:", "stereo:")):
        kind = "Tater stereo pair"
        icon = "T²"
    elif lower_target.startswith(("voice_core:", "native:")):
        kind = "Tater native satellite"
        icon = "T"
    elif lower_target.startswith("airplay:"):
        kind = "AirPlay device"
        icon = "△"
    elif lower_target.startswith("sonos:"):
        kind = "Sonos player"
        icon = "S"
    elif lower_target.startswith("ha:"):
        kind = "Home Assistant player"
        icon = "H"
    else:
        kind = "Music player"
        icon = "♪"

    status = ""
    lower_label = label.casefold()
    for suffix in (
        " • offline or firmware update required",
        " • offline",
        " • online",
    ):
        if lower_label.endswith(suffix):
            status = suffix.removeprefix(" • ").capitalize()
            label = label[: -len(suffix)].strip()
            break

    for prefix in (
        "Tater Satellite:",
        "Tater Sat:",
        "Tater Stereo:",
        "AirPlay Bridge:",
        "AirPlay:",
        "Sonos:",
        "Home Assistant:",
        "Saved player:",
    ):
        if label.casefold().startswith(prefix.casefold()):
            label = label[len(prefix) :].strip()
            break

    detail = ""
    detail_start = label.rfind(" (")
    if detail_start >= 0 and label.endswith(")"):
        detail = label[detail_start + 2 : -1].strip()
        label = label[:detail_start].strip()

    description = " · ".join(
        value for value in (kind, detail, status) if _text(value)
    )
    option["label"] = label or target or "Unnamed player"
    option["description"] = description
    option["icon"] = _text(option.get("icon")) or icon
    return option


def _split_local_airplay_receiver_options(
    options: List[Dict[str, Any]],
    cfg: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], set[str]]:
    """Keep this Tater's receiver out of its own outbound AirPlay player list."""
    receiver_name = _text(cfg.get("airplay_receiver_name")) or "Tater Music"
    wanted_name = receiver_name.casefold()
    local_targets: set[str] = set()
    outbound: List[Dict[str, Any]] = []
    for row in options:
        target = _text(row.get("value")) if isinstance(row, dict) else ""
        label = _text(row.get("label")) if isinstance(row, dict) else ""
        display_name = label
        for prefix in ("AirPlay:", "AirPlay Bridge:"):
            if display_name.casefold().startswith(prefix.casefold()):
                display_name = display_name[len(prefix) :].strip()
                break
        display_name = display_name.split("(", 1)[0].strip()
        if (
            target.casefold().startswith("airplay:")
            and display_name.casefold() == wanted_name
        ):
            local_targets.add(target.casefold())
            continue
        outbound.append(row)
    return outbound, local_targets


def _target_options(
    current_values: Any = None,
    provider_id: Any = "",
    *,
    include_stereo_members: bool = False,
) -> List[Dict[str, str]]:
    try:
        from announcement_targets import build_announcement_target_options

        rows = build_announcement_target_options(
            homeassistant_base_url="",
            homeassistant_token="",
            include_homeassistant=True,
            include_sonos=True,
            include_airplay=True,
            include_voice_core=True,
            include_integrations=True,
            current_values=current_values,
        )
        options = [
            _compact_target_option(row)
            for row in rows
            if (
                isinstance(row, dict)
                and _text(row.get("value"))
                and not _text(row.get("value")).casefold().startswith("integration:roon:")
            )
        ]
        if not include_stereo_members:
            paired_members = _stereo_member_target_map()
            options = [
                row
                for row in options
                if _text(row.get("value")).casefold() not in paired_members
            ]
        return sorted(options, key=lambda row: _text(row.get("label")).casefold())
    except Exception as exc:
        logger.debug("[Music] target discovery unavailable: %s", exc)
        return []


def _target_alias_map(options: List[Dict[str, Any]]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for row in options:
        if not isinstance(row, dict):
            continue
        target = _text(row.get("value"))
        if not target:
            continue
        aliases[target.casefold()] = target
        for alias in _list(row.get("target_aliases")):
            aliases[alias.casefold()] = target
    return aliases


def _canonical_option_targets(targets: Any, options: List[Dict[str, Any]]) -> List[str]:
    aliases = _target_alias_map(options)
    return _list([aliases.get(target.casefold(), target) for target in _list(targets)])


def _target_from_query(value: Any, options: Optional[List[Dict[str, str]]] = None) -> str:
    token = _text(value)
    if not token:
        return ""
    lower = token.casefold()
    candidates = options if isinstance(options, list) else _target_options()
    if lower.startswith(("voice_core:", "ha:", "sonos:", "airplay:", "integration:")):
        return _target_alias_map(candidates).get(lower, token)
    exact = [
        _text(row.get("value"))
        for row in candidates
        if lower in {_text(row.get("value")).casefold(), _text(row.get("label")).casefold()}
    ]
    if exact:
        return exact[0]
    partial = [
        _text(row.get("value"))
        for row in candidates
        if lower in _text(row.get("label")).casefold()
    ]
    return partial[0] if len(partial) == 1 else ""


def _room_target_from_query(value: Any, options: List[Dict[str, str]]) -> str:
    """Resolve an automatic room destination, preferring Sonos over room satellites."""
    room_name = _text(value).casefold()
    if not room_name:
        return ""
    if room_name.startswith(("voice_core:", "ha:", "sonos:", "airplay:", "integration:")):
        return _text(value)
    matches = [
        row
        for row in options
        if isinstance(row, dict)
        and _text(row.get("value"))
        and room_name in f"{_text(row.get('label'))} {_text(row.get('value'))}".casefold()
    ]
    if not matches:
        return ""

    def rank(row: Dict[str, Any]) -> tuple[int, str]:
        target = _text(row.get("value")).casefold()
        label = _text(row.get("label")).casefold()
        if target.startswith(("sonos:", "integration:sonos:")) or "sonos" in label:
            priority = 0
        elif target.startswith("integration:"):
            priority = 1
        elif target.startswith("voice_core:"):
            priority = 2
        else:
            priority = 3
        return priority, label

    return _text(sorted(matches, key=rank)[0].get("value"))


def _preferred_room_target(room_names: Iterable[str], client: Any = None) -> str:
    names = [_text(value) for value in room_names if _text(value)]
    if not names:
        return ""
    try:
        from integration_registry import get_integration_room_preferred_media_player

        result = get_integration_room_preferred_media_player(names, client or redis_client)
        return _text(result.get("target")) if isinstance(result, dict) else ""
    except Exception:
        return ""


def _is_screen_target(value: Any) -> bool:
    return _text(value).casefold().startswith(SCREEN_TARGET_PREFIX)


def _screen_profiles(store: Any = None) -> Dict[str, Dict[str, Any]]:
    """Jarvis Screen profiles read-only: ``screen_key (casefolded) -> profile``.

    The hash lives in the screen core's Redis namespace; it is only ever read
    here, never written.
    """
    store = store if store is not None else globals().get("redis_client")
    rows: Dict[Any, Any] = {}
    try:
        rows = store.hgetall(SCREEN_PROFILES_KEY) if store else {}
    except Exception:
        rows = {}
    profiles: Dict[str, Dict[str, Any]] = {}
    for screen_key, value in (rows or {}).items():
        key = _text(screen_key).casefold()
        if not key:
            continue
        try:
            profile = json.loads(value) if isinstance(value, (str, bytes, bytearray)) else {}
        except Exception:
            profile = {}
        if isinstance(profile, dict):
            profiles[key] = profile
    return profiles


def _screen_target_label(screen_key: Any, *, store: Any = None) -> str:
    """Human label for one screen key, from the Jarvis Screen profiles."""
    profile = _screen_profiles(store).get(_text(screen_key).casefold()) or {}
    return _text(profile.get("label")) or _text(screen_key)


def _screen_target_summary_label(screen_key: Any, *, store: Any = None) -> str:
    """Label shaped for a spoken summary, e.g. "the Office screen"."""
    label = _screen_target_label(screen_key, store=store)
    if any(word in label.casefold() for word in ("screen", "display", "monitor", "panel", "tablet")):
        return f"the {label}"
    return f"the {label} screen"


def _screen_targets_from_words(values: Any, store: Any = None) -> Dict[str, str]:
    """Map user-named screens to ``screen:<key>`` playback targets.

    Reads the Jarvis Screen profiles hash (read-only) and matches each word
    against a profile's ``label`` — exact or case-insensitively contained —
    and the bare screen key; a ``screen:<key>`` value is accepted verbatim.
    Returns ``word (casefolded) -> screen target`` for the words that resolve;
    everything else is left to the hardware-target resolution path.
    """
    profiles = _screen_profiles(store)
    matches: Dict[str, str] = {}
    for value in _list(values):
        word = _text(value)
        lowered = word.casefold()
        if not word or lowered in matches:
            continue
        if lowered.startswith(SCREEN_TARGET_PREFIX):
            key = lowered[len(SCREEN_TARGET_PREFIX) :]
            if key:
                # The literal form is already resolved; pass it through even
                # when the profiles hash is unavailable (the screen core
                # validates the key when the card opens).
                matches[lowered] = lowered
            continue
        for profile_key, profile in profiles.items():
            label = _text(profile.get("label")).casefold()
            if (
                lowered == label
                or lowered == profile_key
                or (len(lowered) >= 3 and lowered in label)
            ):
                matches[lowered] = f"{SCREEN_TARGET_PREFIX}{profile_key}"
                break
    return matches


def _resolve_targets(
    requested: Any = "",
    *,
    room: Any = "",
    origin: Optional[Dict[str, Any]] = None,
    client: Any = None,
    provider_id: Any = "",
    person_id: Any = "",
) -> List[str]:
    store = client or globals().get("redis_client")
    requested_values = _list(requested)
    context = origin if isinstance(origin, dict) else {}
    explicit_room_names = _list(room)
    origin_room_names = (
        []
        if explicit_room_names or requested_values
        else _list(_origin_value(context, "room_name", "area_name", "room_id", "area_id"))
    )
    room_names = explicit_room_names or origin_room_names
    preferred_by_room = {
        room_name: _preferred_room_target([room_name], store)
        for room_name in room_names
    }
    options = _target_options(
        current_values=[*requested_values, *preferred_by_room.values()],
        provider_id=provider_id,
        include_stereo_members=True,
    )
    options, local_airplay_targets = _split_local_airplay_receiver_options(
        options,
        _settings(store),
    )
    if explicit_room_names:
        resolved_rooms = [
            preferred_by_room.get(room_name) or _room_target_from_query(room_name, options)
            for room_name in explicit_room_names
        ]
        if any(not target for target in resolved_rooms):
            return []
        return _normalize_stereo_targets(resolved_rooms)

    if requested_values:
        # Jarvis Screen destinations resolve first: user-named screens ("on the
        # office screen") and literal screen:<key> values map before the
        # hardware-target lookups, which would not know them.
        screen_matches = _screen_targets_from_words(requested_values, store)
        explicit = []
        for value in requested_values:
            direct_value = _text(value)
            if direct_value.casefold() in local_airplay_targets:
                explicit.append("")
                continue
            screen_target = screen_matches.get(direct_value.casefold())
            if screen_target:
                explicit.append(screen_target)
                continue
            if direct_value.casefold().startswith(("voice_core:", "ha:", "sonos:", "airplay:", "integration:", "screen:")):
                target = _target_alias_map(options).get(direct_value.casefold(), direct_value)
            else:
                target = (
                    _preferred_room_target([direct_value], store)
                    or _target_from_query(direct_value, options)
                    or _room_target_from_query(direct_value, options)
                )
            explicit.append(target)
        if any(not target for target in explicit):
            return []
        return _normalize_stereo_targets(explicit)

    if room_names:
        resolved_rooms = [
            preferred_by_room.get(room_name) or _room_target_from_query(room_name, options)
            for room_name in room_names
        ]
        if any(not target for target in resolved_rooms):
            return []
        return _normalize_stereo_targets(resolved_rooms)

    selector = _origin_value(
        context,
        "satellite_selector",
        "voice_core_selector",
        "device_selector",
    )
    if selector:
        return _normalize_stereo_targets(
            [selector if selector.startswith("voice_core:") else f"voice_core:{selector}"]
        )
    # A Person's bound rooms ("the Kitchen plays my music") win over the
    # household default destinations when nothing more specific was said.
    bound_targets = [
        _target_from_query(value, options)
        for value in _bound_targets_for_person(person_id, store)
    ]
    resolved_bound = [target for target in bound_targets if target]
    if resolved_bound:
        return _normalize_stereo_targets(resolved_bound)
    cfg = _settings(store)
    defaults = _list(cfg.get("default_targets") or cfg.get("default_target"))
    resolved_defaults = [_target_from_query(value, options) for value in defaults]
    return _normalize_stereo_targets([target for target in resolved_defaults if target])


def _resolve_target(
    requested: Any = "",
    *,
    room: Any = "",
    origin: Optional[Dict[str, Any]] = None,
    client: Any = None,
    provider_id: Any = "",
) -> str:
    targets = _resolve_targets(
        requested,
        room=room,
        origin=origin,
        client=client,
        provider_id=provider_id,
    )
    return targets[0] if targets else ""


def _target_summary(targets: Any) -> str:
    values = _list(targets)
    if not values:
        return "no players"
    screens = [value for value in values if _is_screen_target(value)]
    if screens and len(screens) == len(values):
        return _screen_target_summary_label(screens[0].casefold()[len(SCREEN_TARGET_PREFIX) :])
    if len(values) == 1:
        return values[0]
    return f"{len(values)} destinations"


def _track_label(track: Dict[str, Any]) -> str:
    title = _text(track.get("title")) or "Untitled"
    artist = _text(track.get("artist") or track.get("album_artist"))
    return f"{title} by {artist}" if artist else title


def _track_media_type(track: Dict[str, Any]) -> str:
    declared = _text(track.get("media_type")).split(";", 1)[0].strip().lower()
    if declared.startswith("audio/"):
        return declared
    extension = Path(_text(track.get("path"))).suffix.lower()
    if not extension and _text(track.get("container")):
        extension = "." + _text(track.get("container")).lower().lstrip(".")
    return {
        ".aac": "audio/aac",
        ".aiff": "audio/aiff",
        ".alac": "audio/x-alac",
        ".flac": "audio/flac",
        ".m4a": "audio/mp4",
        ".m4b": "audio/mp4",
        ".mp3": "audio/mpeg",
        ".ogg": "audio/ogg",
        ".opus": "audio/ogg",
        ".wav": "audio/wav",
        ".wma": "audio/x-ms-wma",
    }.get(extension, "application/octet-stream")


def _play_track(
    track: Dict[str, Any],
    targets: Any,
    *,
    volume_percent: int,
    start_position_seconds: float = 0.0,
    mixed_sync_adjustment_ms: int = 0,
    player_settings: Optional[Dict[str, Dict[str, Any]]] = None,
    airplay_group_id: str = "",
    client: Any = None,
) -> Dict[str, Any]:
    provider = _provider(client, track.get("provider"), track.get("person_scope"))
    target_ids = _list(targets)
    hardware_targets = [
        target for target in target_ids if not _is_screen_target(target)
    ]
    screen_targets = [target for target in target_ids if _is_screen_target(target)]
    selected_player_settings = (
        player_settings if isinstance(player_settings, dict) else {}
    )
    audio_sync_transcode = _uses_audio_sync_transcode(hardware_targets)
    source_url = provider.stream_url(track, audio_sync=audio_sync_transcode)
    if not source_url:
        raise RuntimeError(f"No stream is available for {_track_label(track)}.")
    if not hardware_targets:
        # Screen-only playback: the Jarvis Screen browser fetches the stream
        # URL itself (no audio_sync transcode — browsers decode original
        # containers), so this core only keeps the queue timeline and never
        # drives hardware for it.
        return {
            "ok": True,
            "target_count": len(screen_targets),
            "screen_targets": list(screen_targets),
        }
    from media_playback import play_media_url_targets

    duration = max(0.0, _as_float(track.get("duration_seconds")))
    source_path = Path(_text(track.get("path")) or "music-track")
    playback_media_type = "audio/wav" if audio_sync_transcode else _track_media_type(track)
    playback_filename = (
        f"{source_path.stem}.sync.wav"
        if audio_sync_transcode
        else source_path.name
    )
    result = play_media_url_targets(
        hardware_targets,
        source_url,
        media_type=playback_media_type,
        media_content_type="music",
        filename=playback_filename,
        text=f"Playing {_track_label(track)}.",
        title=_text(track.get("title")) or Path(_text(track.get("path")) or "music-track").stem,
        artist=_text(track.get("artist") or track.get("album_artist")),
        album=_text(track.get("album")),
        duration_seconds=duration,
        volume_percent=volume_percent,
        start_position_seconds=max(0.0, _as_float(start_position_seconds)),
        mixed_sync_adjustment_ms=_as_int(mixed_sync_adjustment_ms, 0, -750, 3000),
        target_volume_percent={
            target: _as_int(values.get("volume_percent"), volume_percent, 0, 100)
            for target, values in dict(player_settings or {}).items()
            if _text(target) and not _is_screen_target(target) and isinstance(values, dict)
        },
        target_sync_offset_ms={
            target: _as_int(values.get("sync_offset_ms"), 0, -1000, 1000)
            for target, values in dict(player_settings or {}).items()
            if _text(target) and not _is_screen_target(target) and isinstance(values, dict)
        },
        target_transport_mode={
            target: _player_transport_mode(values.get("transport_mode"))
            for target, values in dict(player_settings or {}).items()
            if _text(target)
            and isinstance(values, dict)
            and target.casefold().startswith(("sonos:", "integration:sonos:"))
        },
        airplay_group_id=_text(airplay_group_id),
        timeout_s=max(180.0, duration + 120.0),
        respect_reply_playback=False,
    )
    if not isinstance(result, dict) or result.get("ok") is False:
        raise RuntimeError(_text((result or {}).get("error")) or "Music playback failed.")
    # The screen mirrors in parallel when a queue mixes screens with speakers;
    # mixed sync is not guaranteed.
    if screen_targets:
        result["screen_targets"] = list(screen_targets)
    result["audio_sync_transcode_used"] = audio_sync_transcode
    if audio_sync_transcode:
        result["audio_sync_transcode_profile"] = "audio_sync"
    return result


def get_media_urls(track: Dict[str, Any], client: Any = None) -> Dict[str, str]:
    """Public stream/artwork URLs for one catalog/queue track row.

    Used by the Jarvis Screen music card. Returns {"stream": url, "art": url}
    with "" for missing entries. No audio_sync transcode: browser targets
    decode original containers.
    """
    if not isinstance(track, dict):
        return {"stream": "", "art": ""}
    provider = None
    try:
        provider = _provider(client, track.get("provider"), track.get("person_scope"))
        stream = _text(provider.stream_url(track)) if provider else ""
    except Exception:
        stream = ""
    art = ""
    if provider is not None:
        try:
            art = _text(provider.artwork_url(track))
        except Exception:
            art = ""
    return {"stream": stream, "art": art}


def _playback_voice_core_sessions(player: Dict[str, Any]) -> List[Dict[str, Any]]:
    playback_result = (
        player.get("playback_result")
        if isinstance(player.get("playback_result"), dict)
        else {}
    )
    return [
        dict(row)
        for row in list(playback_result.get("voice_core_sessions") or [])
        if isinstance(row, dict) and _text(row.get("session_id"))
    ]


def _stop_target(
    targets: Any,
    *,
    expected_voice_core_sessions: Any = None,
) -> List[str]:
    warnings: List[str] = []
    # Screen destinations are browser-side only: there is no session to stop
    # and split_announcement_targets would treat the unknown id as a
    # voice_core selector, so screens never reach the host target split.
    hardware_targets = [
        target for target in _list(targets) if not _is_screen_target(target)
    ]
    if not hardware_targets:
        return warnings
    try:
        from announcement_targets import split_announcement_targets

        grouped = split_announcement_targets(hardware_targets)
    except Exception as exc:
        return [_text(exc)]

    selectors = list(grouped.get("voice_core_selectors") or [])
    if selectors:
        try:
            sessions = [
                dict(row)
                for row in list(expected_voice_core_sessions or [])
                if isinstance(row, dict) and _text(row.get("session_id"))
            ]
            if sessions:
                from media_playback import _voice_core_stop_media_sync

                warnings.extend(
                    _voice_core_stop_media_sync(
                        [],
                        expected_sessions=sessions,
                        reason="music_core_stop",
                    )
                )
            else:
                from tater_voice import native_satellite, stereo_pairs

                for selector in selectors:
                    members = [selector]
                    pair = stereo_pairs.get_pair(selector) if stereo_pairs.is_stereo_selector(selector) else {}
                    if isinstance(pair, dict) and pair:
                        members = [
                            _text(pair.get("left_selector")),
                            _text(pair.get("right_selector")),
                        ]
                    for member in members:
                        if not member:
                            continue
                        try:
                            native_satellite.run_on_runtime_loop(
                                native_satellite.send_command(
                                    member,
                                    "media.session.stop",
                                    {"reason": "music_core_stop"},
                                ),
                                timeout=8.0,
                            )
                        except Exception as exc:
                            warnings.append(f"{member}: {exc}")
        except Exception as exc:
            warnings.append(_text(exc))

    airplay_players = list(grouped.get("airplay_players") or [])
    try:
        from announcement_targets import resolve_sonos_airplay_target

        for speaker in grouped.get("sonos_speakers") or []:
            bridge_target = _text(resolve_sonos_airplay_target(f"sonos:{speaker}"))
            bridge_id = bridge_target.removeprefix("airplay:")
            if bridge_id and bridge_id not in airplay_players:
                airplay_players.append(bridge_id)
    except Exception:
        pass
    if airplay_players:
        try:
            from airplay_bridge import stop_airplay_targets

            result = stop_airplay_targets(airplay_players)
            warnings.extend(
                _text(value)
                for value in list(result.get("warnings") or [])
                if _text(value)
            )
        except Exception as exc:
            warnings.append(f"AirPlay Bridge: {exc}")

    integration_targets = list(grouped.get("integration_devices") or [])
    integration_targets.extend(
        {"integration_id": "homeassistant", "device_id": entity_id}
        for entity_id in grouped.get("homeassistant_media_players") or []
    )
    integration_targets.extend(
        {"integration_id": "sonos", "device_id": speaker}
        for speaker in grouped.get("sonos_speakers") or []
    )
    if integration_targets:
        try:
            from integration_registry import run_integration_device_action

            for row in integration_targets:
                try:
                    run_integration_device_action(
                        _text(row.get("integration_id")),
                        "stop",
                        _text(row.get("device_id")),
                        {},
                    )
                except Exception as exc:
                    warnings.append(
                        f"{_text(row.get('integration_id'))}:{_text(row.get('device_id'))}: {exc}"
                    )
        except Exception as exc:
            warnings.append(_text(exc))
    return warnings


def _player_position_seconds(player: Dict[str, Any], *, now: Optional[float] = None) -> float:
    position = max(0.0, _as_float(player.get("position_offset_seconds")))
    started_at = _as_float(player.get("started_at"))
    if _text(player.get("status")).lower() == "playing" and started_at > 0:
        position += max(0.0, (time.time() if now is None else float(now)) - started_at)
    return position


def _native_session_members(player: Dict[str, Any]) -> List[Dict[str, Any]]:
    playback_result = (
        player.get("playback_result")
        if isinstance(player.get("playback_result"), dict)
        else {}
    )
    members: List[Dict[str, Any]] = []
    for session in list(playback_result.get("voice_core_sessions") or []):
        if not isinstance(session, dict) or not _text(session.get("session_id")):
            continue
        selectors = _list(session.get("selectors") or session.get("target"))
        for selector in selectors:
            if selector:
                members.append(
                    {
                        "selector": selector,
                        "session_id": _text(session.get("session_id")),
                        "target": _text(session.get("target")),
                    }
                )
    return members


def _require_native_seek_support(targets: Any) -> None:
    try:
        from announcement_targets import split_announcement_targets
        from tater_voice import native_satellite, stereo_pairs

        grouped = split_announcement_targets(_list(targets))
        selectors = list(grouped.get("voice_core_selectors") or [])
        members: List[str] = []
        for selector in selectors:
            pair = stereo_pairs.get_pair(selector) if stereo_pairs.is_stereo_selector(selector) else {}
            if isinstance(pair, dict) and pair:
                members.extend(
                    member
                    for member in (
                        _text(pair.get("left_selector")),
                        _text(pair.get("right_selector")),
                    )
                    if member
                )
            elif selector:
                members.append(selector)
        unsupported = []
        for member in members:
            supported = native_satellite.run_on_runtime_loop(
                native_satellite.client_has_capability(
                    member,
                    "media_session_start_position",
                ),
                timeout=4.0,
            )
            if not supported:
                unsupported.append(member)
        if unsupported:
            raise ValueError(
                "Seeking needs the latest satellite firmware on "
                + ", ".join(unsupported)
                + "."
            )
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(f"Could not confirm satellite seek support: {exc}") from exc


def _set_target_volume(player: Dict[str, Any], volume_percent: int) -> Dict[str, Any]:
    # Screen destinations play at the browser's own volume; there is nothing
    # to send, and the host target split would read an unknown id as a
    # voice_core selector.
    targets = [
        target
        for target in _list(player.get("targets") or player.get("target"))
        if not _is_screen_target(target)
    ]
    if not targets:
        return {"sent_count": 0, "warnings": ["No playback destinations are selected."]}
    try:
        from announcement_targets import split_announcement_targets

        grouped = split_announcement_targets(targets)
    except Exception as exc:
        return {"sent_count": 0, "warnings": [_text(exc)]}

    sent_count = 0
    warnings: List[str] = []
    airplay_players = list(grouped.get("airplay_players") or [])
    if airplay_players:
        try:
            from airplay_bridge import set_airplay_target_volumes

            result = set_airplay_target_volumes(
                {f"airplay:{player}": volume_percent for player in airplay_players}
            )
            sent_count += _as_int(result.get("sent_count"), 0, 0, len(airplay_players))
            warnings.extend(
                _text(value)
                for value in list(result.get("warnings") or [])
                if _text(value)
            )
        except Exception as exc:
            warnings.append(f"AirPlay Bridge: {exc}")
    bridged_sonos_speakers = set()
    try:
        from airplay_bridge import set_airplay_target_volumes
        from announcement_targets import resolve_sonos_airplay_target

        for speaker in grouped.get("sonos_speakers") or []:
            bridge_target = _text(resolve_sonos_airplay_target(f"sonos:{speaker}"))
            if not bridge_target:
                continue
            result = set_airplay_target_volumes({bridge_target: volume_percent})
            if _as_int(result.get("sent_count"), 0, 0, 1) > 0:
                sent_count += 1
                bridged_sonos_speakers.add(_text(speaker))
    except Exception:
        pass
    native_members = _native_session_members(player)
    if grouped.get("voice_core_selectors") and not native_members:
        warnings.append("The current satellite playback session is unavailable; start the track again.")
    if native_members:
        try:
            from tater_voice import native_satellite, stereo_pairs

            pair_scales: Dict[str, int] = {}
            for target in grouped.get("voice_core_selectors") or []:
                pair = stereo_pairs.get_pair(target) if stereo_pairs.is_stereo_selector(target) else {}
                if not isinstance(pair, dict) or not pair:
                    continue
                pair_scales[_text(pair.get("left_selector"))] = _as_int(
                    pair.get("left_volume_percent"), 100, 0, 100
                )
                pair_scales[_text(pair.get("right_selector"))] = _as_int(
                    pair.get("right_volume_percent"), 100, 0, 100
                )
            for member in native_members:
                selector = _text(member.get("selector"))
                try:
                    supported = native_satellite.run_on_runtime_loop(
                        native_satellite.client_has_capability(selector, "media_session_volume"),
                        timeout=4.0,
                    )
                    if not supported:
                        raise RuntimeError("update satellite firmware to enable live music volume")
                    member_volume = round(volume_percent * pair_scales.get(selector, 100) / 100)
                    native_satellite.run_on_runtime_loop(
                        native_satellite.send_request(
                            selector,
                            "media.session.volume",
                            {
                                "session_id": _text(member.get("session_id")),
                                "volume_percent": max(0, min(100, member_volume)),
                            },
                            timeout_s=4.0,
                        ),
                        timeout=6.0,
                    )
                    sent_count += 1
                except Exception as exc:
                    warnings.append(f"{selector}: {exc}")
        except Exception as exc:
            warnings.append(_text(exc))

    integration_targets = list(grouped.get("integration_devices") or [])
    integration_targets.extend(
        {"integration_id": "homeassistant", "device_id": entity_id}
        for entity_id in grouped.get("homeassistant_media_players") or []
    )
    integration_targets.extend(
        {"integration_id": "sonos", "device_id": speaker}
        for speaker in grouped.get("sonos_speakers") or []
        if _text(speaker) not in bridged_sonos_speakers
    )
    if integration_targets:
        try:
            from integration_registry import run_integration_device_action

            for row in integration_targets:
                integration_id = _text(row.get("integration_id"))
                device_id = _text(row.get("device_id"))
                try:
                    result = run_integration_device_action(
                        integration_id,
                        "set_volume",
                        device_id,
                        {
                            "volume_percent": volume_percent,
                            "volume": volume_percent / 100.0,
                        },
                    )
                    if isinstance(result, dict) and result.get("ok") is False:
                        raise RuntimeError(_text(result.get("error")) or "volume change failed")
                    sent_count += 1
                except Exception as exc:
                    warnings.append(f"{integration_id}:{device_id}: {exc}")
        except Exception as exc:
            warnings.append(_text(exc))
    return {"sent_count": sent_count, "warnings": warnings}


def _start_player_index(
    index: int,
    *,
    start_position_seconds: float = 0.0,
    record_history: bool = True,
    person_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    with _state_lock:
        player = _player(store, queue_id)
        # Explicit playback supersedes any pending gaining-room resume delay.
        _clear_delayed_resume(player)
        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if not queue:
            raise ValueError("The music queue is empty.")
        if index < 0 or index >= len(queue):
            raise ValueError("The requested queue position is unavailable.")
        targets = _list(player.get("targets") or player.get("target"))
        if not targets:
            raise ValueError("Choose at least one satellite or media player before playing music.")
        track = queue[index]
        cfg = _settings(store)
        saved_volume = player.get("volume_percent")
        if saved_volume in (None, ""):
            saved_volume = cfg.get("default_volume_percent")
        volume = _as_int(
            saved_volume,
            75,
            0,
            100,
        )
        playback_result = (
            player.get("playback_result")
            if isinstance(player.get("playback_result"), dict)
            else {}
        )
        reusable_airplay_group_id = (
            _text(playback_result.get("airplay_bridge_group_id"))
            if _text(player.get("status")).lower() == "playing"
            else ""
        )
        if _text(player.get("status")).lower() == "playing" and not reusable_airplay_group_id:
            _stop_target(
                targets,
                expected_voice_core_sessions=_playback_voice_core_sessions(player),
            )
        duration = max(0.0, _as_float(track.get("duration_seconds")))
        start_position = max(0.0, _as_float(start_position_seconds))
        if duration > 0:
            start_position = min(duration, start_position)
        result = _play_track(
            track,
            targets,
            volume_percent=volume,
            start_position_seconds=start_position,
            mixed_sync_adjustment_ms=_mixed_sync_from_player_settings(
                targets,
                _selected_player_settings(targets, cfg, default_volume=volume),
                _mixed_sync_adjustment(targets, cfg),
            ),
            player_settings=_selected_player_settings(
                targets,
                cfg,
                default_volume=volume,
            ),
            airplay_group_id=reusable_airplay_group_id,
            client=store,
        )
        playback_result = {
            key: result.get(key)
            for key in (
                "target_count",
                "sent_count",
                "homeassistant_target_count",
                "voice_core_sent_count",
                "sonos_sent_count",
                "sonos_airplay_target_count",
                "sonos_airplay_routes",
                "airplay_bridge_target_count",
                "airplay_bridge_prepared_count",
                "airplay_bridge_primed_count",
                "airplay_bridge_sent_count",
                "airplay_bridge_group_id",
                "airplay_bridge_start_unix_ms",
                "airplay_native_start_lead_ms",
                "airplay_minimum_start_unix_ms",
                "airplay_bridge_reused",
                "airplay_bridge_reuse_fallback",
                "airplay_prepare_retried",
                "resume_fallback_used",
                "integration_sent_count",
                "media_session_sent_count",
                "media_session_fallback_count",
                "mixed_sync_adjustment_ms",
                "mixed_native_start_lead_ms",
                "sonos_proxy_used",
                "audio_sync_transcode_used",
                "audio_sync_transcode_profile",
            )
            if result.get(key) is not None
        }
        if isinstance(result.get("sonos_group"), dict):
            playback_result["sonos_group"] = dict(result["sonos_group"])
        voice_core_sessions = [
            dict(row)
            for row in list(result.get("voice_core_sessions") or [])
            if isinstance(row, dict) and _text(row.get("session_id"))
        ]
        if voice_core_sessions:
            playback_result["voice_core_sessions"] = voice_core_sessions
        player.update(
            {
                "status": "playing",
                "index": index,
                "current": track,
                "started_at": time.time(),
                "position_offset_seconds": start_position,
                "duration_seconds": duration,
                "volume_percent": volume,
                "mixed_sync_adjustment_ms": _mixed_sync_from_player_settings(
                    targets,
                    _selected_player_settings(targets, cfg, default_volume=volume),
                    _mixed_sync_adjustment(targets, cfg),
                ),
                "last_error": "",
                "seek_position_pending": False,
                "playback_result": playback_result,
                "warnings": [
                    _text(value)
                    for value in list(result.get("warnings") or [])
                    if _text(value)
                ],
            }
        )
        _save_player(player, store, queue_id)
        if record_history:
            _record_listening_history(
                track,
                targets,
                person_id=player.get("person_id"),
                client=store,
            )
        return player


def _route_player_targets(
    targets: Any,
    *,
    restart_playing: bool = True,
    force_restart: bool = False,
    resume_delay: Any = 0,
    person_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    """Move a queue's session to new destinations without replacing its queue.

    This is the follow-me handoff primitive: the current track and position are
    preserved while playback hands off to the new rooms. With `resume_delay`
    above zero (a real room change only), the gaining room waits that many
    seconds before the music resumes there — `_delayed_resume_tick` starts it.
    """
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    next_targets = _normalize_stereo_targets(targets)
    if not next_targets:
        raise ValueError("Choose one or more valid music destinations.")
    delay = min(600.0, max(0.0, _as_float(resume_delay, 0.0)))
    with _state_lock:
        player = _player(store, queue_id)
        old_targets = _list(player.get("targets") or player.get("target"))
        targets_changed = old_targets != next_targets
        was_playing = _text(player.get("status")).lower() == "playing"
        restart_required = was_playing and (targets_changed or force_restart)
        if not targets_changed and not restart_required:
            return player

        position = _player_position_seconds(player) if was_playing else max(
            0.0,
            _as_float(player.get("position_offset_seconds")),
        )
        warnings: List[str] = []
        if restart_required and old_targets:
            warnings = _stop_target(
                old_targets,
                expected_voice_core_sessions=_playback_voice_core_sessions(player),
            )
        player["targets"] = next_targets
        # A delayed resume is only a real "walk to the other room" — a forced
        # restart on unchanged targets (sync calibration) must not stall.
        pending_delay = delay if (restart_required and targets_changed) else 0.0
        if restart_required:
            player.update(
                {
                    "status": "paused" if pending_delay > 0 else "stopped",
                    "started_at": 0.0,
                    "position_offset_seconds": position,
                }
            )
        if pending_delay > 0:
            player["resume_delay_until"] = time.time() + pending_delay
            player["resume_delay_position"] = position
        elif restart_required:
            player.pop("resume_delay_until", None)
            player.pop("resume_delay_position", None)
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store, queue_id)

        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if restart_required and restart_playing and queue:
            if pending_delay > 0:
                return player
            return _start_player_index(
                _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1)),
                start_position_seconds=position,
                record_history=False,
                person_id=queue_id,
                client=store,
            )
        return player


def _delayed_resume_tick(store: Any, queue_id: Any) -> None:
    """Resume a moved queue in its gaining room once the resume delay elapsed.

    Called from the run loop for every queue slot; manual play/resume/stop
    clears the pending resume instead.
    """
    store = store or globals().get("redis_client")
    if _as_float(_player(store, queue_id).get("resume_delay_until"), 0.0) <= 0:
        return
    with _state_lock:
        player = _player(store, queue_id)
        if _as_float(player.get("resume_delay_until"), 0.0) <= 0:
            return
        if time.time() < _as_float(player.get("resume_delay_until"), 0.0):
            return
        player.pop("resume_delay_until", None)
        player.pop("resume_delay_position", None)
        _save_player(player, store, queue_id)
        if _text(player.get("status")).lower() == "playing":
            # Something already started playback during the wait.
            return
        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if not queue:
            return
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        position = max(0.0, _as_float(player.get("position_offset_seconds")))
    _start_player_index(
        index,
        start_position_seconds=position,
        record_history=False,
        person_id=queue_id,
        client=store,
    )


def _clear_delayed_resume(player: Dict[str, Any]) -> None:
    """Drop a pending delayed resume (explicit play, pause, stop, or new queue)."""
    if player.pop("resume_delay_until", None) is not None:
        player.pop("resume_delay_position", None)


def _seek_player(
    position_seconds: float,
    *,
    person_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    player = _player(store, queue_id)
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    if not queue:
        raise ValueError("The music queue is empty.")
    duration = max(0.0, _as_float(player.get("duration_seconds")))
    if duration <= 0:
        raise ValueError("This track does not report a duration, so it cannot be seeked.")
    position = max(0.0, min(max(0.0, duration - 1.0), _as_float(position_seconds)))
    if position > 0:
        _require_native_seek_support(player.get("targets") or player.get("target"))
    if _text(player.get("status")).lower() == "playing":
        return _start_player_index(
            _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1)),
            start_position_seconds=position,
            record_history=False,
            person_id=queue_id,
            client=store,
        )

    # A timeline adjustment must not become an implicit play command. Store the
    # requested position and let the next explicit play/resume action start from
    # it, while preserving the player's current non-playing status.
    player.update(
        {
            "started_at": 0.0,
            "position_offset_seconds": position,
            "seek_position_pending": True,
        }
    )
    _save_player(player, store, queue_id)
    return player


def _create_and_start_queue(
    tracks: List[Dict[str, Any]],
    *,
    targets: List[str],
    shuffle: bool,
    volume_percent: int,
    person_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    if not tracks:
        raise ValueError("No matching music was found.")
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    queue_id = _queue_id_for_person(person_id)
    selected_person_id = _text(person_id) or _text(cfg.get("prompt_person_id"))
    maximum = _as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000)
    original_queue = [dict(track) for track in tracks[:maximum]]
    queue = [dict(track) for track in original_queue]
    if shuffle and len(queue) > 1:
        random.SystemRandom().shuffle(queue)
    # Smart Shuffle: keep only a rolling window in the queue and hold the rest
    # in a pool that feeds it round-robin across sources as it drains.
    smart_pool: List[Dict[str, Any]] = []
    smart_shuffle = _person_smart_shuffle_enabled(selected_person_id, cfg, store)
    if smart_shuffle:
        recent_ids = _smart_recent_ids(store, selected_person_id)
        window, smart_pool = _smart_window_tracks(queue, shuffle=shuffle, recent_ids=recent_ids)
        if smart_pool:
            original_queue = [dict(track) for track in window]
            queue = [dict(track) for track in window]
    with _state_lock:
        if queue_id:
            _register_queue(queue_id, store)
        previous = _player(store, queue_id)
        old_targets = _list(previous.get("targets") or previous.get("target"))
        if previous.get("status") == "playing" and old_targets:
            _stop_target(
                old_targets,
                expected_voice_core_sessions=_playback_voice_core_sessions(previous),
            )
        player = {
            "status": "queued",
            "provider": _provider_id(queue[0].get("provider"), _provider_id(cfg.get("provider"))),
            "queue": queue,
            "queue_original": original_queue,
            "index": 0,
            "current": queue[0],
            "targets": _list(targets),
            "person_id": selected_person_id,
            "shuffle": bool(shuffle),
            "smart_shuffle": smart_shuffle,
            "smart_pool": smart_pool,
            "repeat": _text(previous.get("repeat") or "off"),
            "volume_percent": volume_percent,
            "mixed_sync_adjustment_ms": _mixed_sync_adjustment(targets, cfg),
            "created_at": time.time(),
            "queue_session_id": uuid.uuid4().hex,
            "continuous_radio": True,
            "continuation_pending": False,
            "radio_name": "Tater Continuous Radio",
            "started_at": 0.0,
            "position_offset_seconds": 0.0,
            "duration_seconds": 0.0,
            "last_error": "",
        }
        _save_player(player, store, queue_id)
    return _start_player_index(0, person_id=queue_id, client=store)


def _advance_player(direction: int, *, person_id: Any = "", client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    with _state_lock:
        player = _player(store, queue_id)
        queue = player.get("queue") if isinstance(player.get("queue"), list) else []
        if not queue:
            raise ValueError("The music queue is empty.")
        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        if direction < 0 and time.time() - _as_float(player.get("started_at")) > 8:
            next_index = index
        else:
            next_index = index + (1 if direction >= 0 else -1)
        repeat = _text(player.get("repeat") or "off").lower()
        if next_index >= len(queue):
            if repeat == "all":
                next_index = 0
            else:
                if _provider_id(player.get("provider")) in CATALOG_PROVIDER_IDS:
                    _append_end_of_queue_fallback(player, store)
                    player = _player(store, queue_id)
                    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
                    index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
                    next_index = index + 1
                if next_index >= len(queue):
                    _stop_target(
                        player.get("targets") or player.get("target"),
                        expected_voice_core_sessions=_playback_voice_core_sessions(player),
                    )
                    player.update(
                        {
                            "status": "finished",
                            "index": len(queue) - 1,
                            "started_at": 0.0,
                            "position_offset_seconds": max(
                                0.0, _as_float(player.get("duration_seconds"))
                            ),
                        }
                    )
                    _save_player(player, store, queue_id)
                    return player
        if next_index < 0:
            next_index = len(queue) - 1 if repeat == "all" else 0
    return _start_player_index(next_index, person_id=queue_id, client=store)


def _set_player_shuffle(
    enabled: bool,
    *,
    person_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    with _state_lock:
        player = _player(store, queue_id)
        queue = [dict(track) for track in list(player.get("queue") or []) if isinstance(track, dict)]
        if not queue:
            player["shuffle"] = bool(enabled)
            _save_player(player, store)
            return player

        index = _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1))
        original = [
            dict(track)
            for track in list(player.get("queue_original") or queue)
            if isinstance(track, dict)
        ]
        player["queue_original"] = original
        player["smart_shuffle"] = _person_smart_shuffle_enabled(queue_id, _settings(store), store)
        if enabled:
            remaining = queue[index + 1 :]
            if player["smart_shuffle"]:
                # Smart Shuffle: recently played tracks go to the back instead
                # of a purely random order.
                remaining = _smart_shuffle_order(remaining, _smart_recent_ids(store, queue_id))
            else:
                random.SystemRandom().shuffle(remaining)
        else:
            used = queue[: index + 1]

            def track_token(track: Dict[str, Any]) -> str:
                return _text(track.get("id") or track.get("url") or track.get("stream_url")) or json.dumps(
                    track,
                    sort_keys=True,
                    default=str,
                )

            used_counts: Dict[str, int] = {}
            for track in used:
                token = track_token(track)
                used_counts[token] = used_counts.get(token, 0) + 1
            remaining = []
            for track in original:
                token = track_token(track)
                if used_counts.get(token, 0) > 0:
                    used_counts[token] -= 1
                    continue
                remaining.append(dict(track))
        player["queue"] = [*queue[: index + 1], *remaining]
        player["shuffle"] = bool(enabled)
        _save_player(player, store, queue_id)
        return player


def _pause_player(*, person_id: Any = "", client: Any = None) -> Dict[str, Any]:
    """Stop active transports while preserving the current track position."""
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    with _state_lock:
        player = _player(store, queue_id)
        # Pausing during a resume-delay window cancels the pending resume too —
        # the queue is not playing, so this is the only feedback channel.
        _clear_delayed_resume(player)
        if _text(player.get("status")).lower() != "playing":
            _save_player(player, store, queue_id)
            return player
        position = _player_position_seconds(player)
        duration = max(0.0, _as_float(player.get("duration_seconds")))
        if duration > 0:
            position = min(duration, position)
        targets = _list(player.get("targets") or player.get("target"))
        warnings = (
            _stop_target(
                targets,
                expected_voice_core_sessions=_playback_voice_core_sessions(player),
            )
            if targets
            else []
        )
        player.update(
            {
                "status": "paused",
                "started_at": 0.0,
                "position_offset_seconds": position,
            }
        )
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store, queue_id)
        return player


def _resume_player(*, person_id: Any = "", client: Any = None) -> Dict[str, Any]:
    """Resume a paused queue from its persisted position, or start it normally."""
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    player = _player(store, queue_id)
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    if not queue:
        raise ValueError("The music queue is empty.")
    status = _text(player.get("status")).lower()
    if status == "playing":
        return player
    resume_from_saved_position = status == "paused" or bool(
        player.get("seek_position_pending")
    )
    return _start_player_index(
        _as_int(player.get("index"), 0, 0, max(0, len(queue) - 1)),
        start_position_seconds=(
            _player_position_seconds(player)
            if resume_from_saved_position
            else 0.0
        ),
        record_history=not resume_from_saved_position,
        person_id=queue_id,
        client=store,
    )


def _stop_player(*, person_id: Any = "", client: Any = None) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    with _state_lock:
        player = _player(store, queue_id)
        targets = _list(player.get("targets") or player.get("target"))
        warnings = (
            _stop_target(
                targets,
                expected_voice_core_sessions=_playback_voice_core_sessions(player),
            )
            if targets
            else []
        )
        player.update(
            {
                "status": "stopped",
                "started_at": 0.0,
                "position_offset_seconds": 0.0,
                "seek_position_pending": False,
            }
        )
        _clear_delayed_resume(player)
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store, queue_id)
        return player


SLEEP_TIMER_MAX_MINUTES = 720


def _sleep_timer_state(player: Dict[str, Any]) -> Dict[str, Any]:
    """One queue's sleep-timer countdown, tracked against the active player."""
    ends_at = _as_float(player.get("sleep_timer_ends_at"))
    if ends_at <= 0:
        return {"active": False, "expired": bool(player.get("sleep_timer_expired_at"))}
    remaining = ends_at - time.time()
    return {
        "active": remaining > 0,
        "expired": remaining <= 0,
        "ends_at": ends_at,
        "remaining_seconds": max(0.0, remaining),
        "minutes": _as_int(player.get("sleep_timer_minutes"), 0, 0, SLEEP_TIMER_MAX_MINUTES),
    }


def _set_sleep_timer(
    minutes: Any,
    *,
    person_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    """Arm (minutes > 0) or cancel (0) one queue's sleep timer."""
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    wanted = _as_int(minutes, 0, 0, SLEEP_TIMER_MAX_MINUTES)
    with _state_lock:
        player = _player(store, queue_id)
        if wanted <= 0:
            player.update({"sleep_timer_ends_at": 0.0, "sleep_timer_minutes": 0})
        else:
            player.update(
                {
                    "sleep_timer_ends_at": time.time() + wanted * 60.0,
                    "sleep_timer_minutes": wanted,
                    "sleep_timer_expired_at": 0.0,
                }
            )
        _save_player(player, store, queue_id)
        return player


def _sleep_timer_tick(store: Any, queue_id: Any) -> Optional[Dict[str, Any]]:
    """Force-stop a queue whose sleep timer just hit zero.

    The timer is a hard override: Endless Playback, Smart Shuffle pools, and
    any in-flight continuous-radio refill are all cancelled so nothing keeps
    streaming after the countdown.
    """
    with _state_lock:
        player = _player(store, queue_id)
        ends_at = _as_float(player.get("sleep_timer_ends_at"))
        if ends_at <= 0 or time.time() < ends_at:
            return None
        targets = _list(player.get("targets") or player.get("target"))
        warnings = (
            _stop_target(
                targets,
                expected_voice_core_sessions=_playback_voice_core_sessions(player),
            )
            if targets
            else []
        )
        player.update(
            {
                "status": "stopped",
                "started_at": 0.0,
                "position_offset_seconds": 0.0,
                "seek_position_pending": False,
                "sleep_timer_ends_at": 0.0,
                "sleep_timer_expired_at": time.time(),
                "continuation_pending": False,
                "continuous_radio": False,
                "smart_pool": [],
                # Invalidate any continuation worker still holding the old
                # session token so a late refill can never append.
                "queue_session_id": uuid.uuid4().hex,
            }
        )
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store, queue_id)
        return player


def _advance_finished_player(client: Any = None, person_id: Any = "") -> None:
    store = client or globals().get("redis_client")
    queue_id = _queue_id_for_person(person_id)
    player = _reconcile_native_playback(_player(store, queue_id), store, queue_id)
    if _text(player.get("status")).lower() != "playing":
        return
    duration = _as_float(player.get("duration_seconds"))
    if duration <= 0 or _player_position_seconds(player) < duration + 0.15:
        return
    try:
        if _text(player.get("repeat")).lower() == "one":
            _start_player_index(
                _as_int(player.get("index"), 0, 0, 100000),
                person_id=queue_id,
                client=store,
            )
        else:
            _advance_player(1, person_id=queue_id, client=store)
    except Exception as exc:
        player = _player(store, queue_id)
        player.update({"status": "error", "last_error": _text(exc)[:500]})
        _save_player(player, store, queue_id)


def _reconcile_native_playback(
    player: Dict[str, Any],
    client: Any = None,
    person_id: Any = "",
) -> Dict[str, Any]:
    if _text(player.get("status")).lower() != "playing":
        return player
    playback_result = (
        player.get("playback_result")
        if isinstance(player.get("playback_result"), dict)
        else {}
    )
    sessions = [
        row
        for row in list(playback_result.get("voice_core_sessions") or [])
        if isinstance(row, dict) and _text(row.get("session_id"))
    ]
    if not sessions:
        return player
    try:
        from tater_voice import native_satellite

        snapshot = native_satellite.status_snapshot_sync()
    except Exception:
        return player
    clients = snapshot.get("clients") if isinstance(snapshot, dict) else {}
    if not isinstance(clients, dict):
        return player

    failed: List[str] = []
    for session in sessions:
        session_id = _text(session.get("session_id"))
        selectors = _list(session.get("selectors") or session.get("target"))
        states = []
        for selector in selectors:
            client_row = clients.get(selector)
            media_session = (
                client_row.get("media_session")
                if isinstance(client_row, dict) and isinstance(client_row.get("media_session"), dict)
                else {}
            )
            if _text(media_session.get("session_id")) == session_id:
                states.append(media_session)
        if not states or any(bool(state.get("active")) for state in states):
            continue
        finished_states = [state for state in states if _as_float(state.get("finished_ts")) > 0]
        if finished_states and any(state.get("ok") is False for state in finished_states):
            failed.append(_text(session.get("target")) or ", ".join(selectors))

    if not failed:
        return player
    warning = "Playback failed on " + ", ".join(failed) + "."
    warnings = [_text(value) for value in list(player.get("warnings") or []) if _text(value)]
    if warning not in warnings:
        warnings.append(warning)
    player["warnings"] = warnings
    sent_count = _as_int(playback_result.get("sent_count"), len(sessions), 0, 10000)
    voice_core_sent_count = _as_int(
        playback_result.get("voice_core_sent_count"),
        len(sessions),
        0,
        10000,
    )
    all_dispatched_targets_are_tracked_native_sessions = (
        len(sessions) >= voice_core_sent_count and sent_count <= voice_core_sent_count
    )
    if len(failed) == len(sessions) and all_dispatched_targets_are_tracked_native_sessions:
        player["status"] = "error"
        player["last_error"] = warning
        player["started_at"] = 0.0
    _save_player(
        player,
        client,
        person_id if person_id else player.get("queue_id"),
    )
    return player


def _validate_catalog_provider_targets(targets: Any) -> None:
    roon_targets = [
        target
        for target in _list(targets)
        if target.lower().startswith("integration:roon:")
    ]
    if roon_targets:
        raise ValueError(
            "Roon zones cannot receive Personal Music Core streams. Choose satellites, stereo pairs, "
            "or another supported media player."
        )


def _queue_owner_label(queue_id: Any, client: Any = None) -> str:
    """Human name for whoever owns a queue slot."""
    wanted = _text(queue_id)
    if not wanted:
        return "the household music"
    return (
        _people_person_name(wanted, client)
        or wanted
    ) + "'s music"


def _play_request(
    args: Dict[str, Any],
    origin: Optional[Dict[str, Any]],
    client: Any,
    *,
    force: bool = False,
) -> Dict[str, Any]:
    cfg = _settings(client)
    speaking_person_id = _context_person_id(origin)
    person_id = speaking_person_id or _text(cfg.get("prompt_person_id"))
    selected_provider = _person_source_id(person_id, client)
    catalog = _person_catalog(client, selected_provider, person_id)
    if not isinstance(catalog.get("tracks"), list) or not catalog.get("tracks"):
        catalog = _sync_catalog(client, selected_provider, person_id)
    query = _text(args.get("query") or args.get("music"))
    title = _text(args.get("title") or args.get("track") or args.get("song"))
    artist = _text(args.get("artist"))
    album = _text(args.get("album"))
    genre = _text(args.get("genre"))
    matches = _search_tracks(
        query=query,
        title=title,
        artist=artist,
        album=album,
        genre=genre,
        limit=_as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000),
        client=client,
        provider_id=selected_provider,
        person_id=person_id,
    )
    requested_targets = (
        args.get("targets")
        or args.get("target")
        or args.get("destinations")
        or args.get("destination")
        or args.get("players")
        or args.get("player")
    )
    targets = _resolve_targets(
        requested_targets,
        room=args.get("rooms") or args.get("room"),
        origin=origin,
        client=client,
        provider_id=selected_provider,
        person_id=speaking_person_id,
    )
    if not targets:
        raise ValueError("Choose one or more satellites, stereo pairs, or media players for this music.")
    _validate_catalog_provider_targets(targets)
    broad_request = bool(genre or artist or (query and not title and not album))
    shuffle = _as_bool(
        args.get("shuffle"),
        _as_bool(cfg.get("default_shuffle"), True) if broad_request else False,
    )
    requested_volume = args.get("volume_percent")
    if requested_volume in (None, ""):
        requested_volume = args.get("volume")
    if requested_volume in (None, ""):
        requested_volume = cfg.get("default_volume_percent")
    volume = _as_int(requested_volume, 75, 0, 100)

    # Queue identity follows the speaking Person; personless requests share the
    # household queue (with history attributed to the configured prompt Person).
    queue_id = _queue_id_for_person(speaking_person_id)
    conflicts = _queue_conflicts(client, queue_id, targets)
    if (conflicts["foreign_targets"] or conflicts["own_playing_elsewhere"]) and not force:
        mode = _queue_conflict_mode(queue_id, client)
        if mode == "ask":
            return _queue_conflict_prompt(args, origin, client, queue_id, targets, conflicts)
        if conflicts["foreign_targets"]:
            # auto_move: free the requested rooms from any other queue without
            # asking; that queue keeps its position on any rooms it has left.
            _release_targets_to(client, targets, except_queue_id=queue_id)
    player = _create_and_start_queue(
        matches,
        targets=targets,
        shuffle=shuffle,
        volume_percent=volume,
        person_id=person_id,
        client=client,
    )
    sleep_note = ""
    sleep_raw = args.get("sleep_minutes")
    if sleep_raw in (None, ""):
        sleep_raw = args.get("sleep_timer_minutes")
    if sleep_raw in (None, ""):
        sleep_raw = args.get("sleep_timer")
    if sleep_raw not in (None, ""):
        sleep_minutes = _as_int(sleep_raw, 0, 0, SLEEP_TIMER_MAX_MINUTES)
        if sleep_minutes > 0:
            _set_sleep_timer(sleep_minutes, person_id=speaking_person_id, client=client)
            sleep_note = f" The sleep timer stops it in {sleep_minutes} minute"
            sleep_note += "" if sleep_minutes == 1 else "s"
    return {
        "ok": True,
        "provider": selected_provider,
        "target": targets[0],
        "targets": targets,
        "target_count": len(targets),
        "queue_count": len(player.get("queue") or []),
        "shuffle": bool(player.get("shuffle")),
        "sleep_timer_minutes": _as_int(sleep_raw, 0, 0, SLEEP_TIMER_MAX_MINUTES)
        if sleep_raw not in (None, "")
        else 0,
        "warnings": list(player.get("warnings") or []),
        "now_playing": _public_track(player.get("current") or {}),
        "summary_for_user": (
            f"Playing {_track_label(player.get('current') or {})} on {_target_summary(targets)}. "
            f"The queue has {len(player.get('queue') or [])} track"
            f"{'' if len(player.get('queue') or []) == 1 else 's'}, and continuous radio will keep it playing."
            + sleep_note
        ),
    }


def _queue_conflict_prompt(
    args: Dict[str, Any],
    origin: Optional[Dict[str, Any]],
    client: Any,
    queue_id: Any,
    targets: List[str],
    conflicts: Dict[str, Any],
) -> Dict[str, Any]:
    """Ask before touching another Person's stream (or relocating their own)."""
    store = client or globals().get("redis_client")
    if conflicts.get("foreign_targets"):
        pending_type = "takeover"
        owners = sorted(
            {_queue_owner_label(qid, store) for qid in conflicts["foreign_queues"]}
        )
        question = (
            f"{' and '.join(owners)} still playing on {_target_summary(targets)}. Take over?"
        )
    else:
        pending_type = "relocate"
        own_targets = _list(conflicts.get("own_queue", {}).get("targets"))
        question = (
            f"Your music is still playing on {_target_summary(own_targets)}. "
            f"Say yes to move it to {_target_summary(targets)} instead, or ask me to start the new music."
        )
    person_for_pending = _text(queue_id)
    _save_pending_confirmation(
        store,
        person_for_pending,
        {
            "type": pending_type,
            "args": args if isinstance(args, dict) else {},
            "origin": dict(origin) if isinstance(origin, dict) else {},
            "targets": list(targets),
            "queue_id": person_for_pending,
        },
    )
    return {
        "ok": False,
        "needs_confirmation": True,
        "pending": pending_type,
        "question": question,
        "summary_for_user": question,
        "say_hint": question,
        "confirm_tool": "personal_music_confirm",
    }


def get_hydra_kernel_tools(*, platform: str = "", **_kwargs) -> List[Dict[str, Any]]:
    # Tool ids are globally namespaced across cores and first-declared wins, so
    # every id here carries the personal_music_ prefix and never collides with the
    # upstream Music Core's music_* tools when both cores run side by side.
    return [
        {
            "id": "personal_music_play",
            "description": (
                "Use when the user asks to play music from their personal Emby or network-share library by "
                "song, artist, album, genre, or description. Put user-named rooms in rooms, specific "
                "user-named speakers in targets, and leave both empty when playback should follow the "
                "speaking room. User-named screens (\"play on the office screen\") are also valid "
                "destinations and can be mixed with speakers — screen playback renders in the screen's "
                "browser. When the user asks for music for a set amount of time (\"play my music "
                "for an hour\"), also pass sleep_minutes; a sleep timer then stops playback and "
                "overrides endless playback when it hits zero."
            ),
            "usage": (
                '{"function":"personal_music_play","arguments":{"query":"reggae music","genre":"reggae",'
                '"artist":"","album":"","title":"","targets":[],'
                '"rooms":["Family Room"],"shuffle":true,"volume_percent":75,"sleep_minutes":60}}'
            ),
        },
        {
            "id": "personal_music_search",
            "description": "Search the personal Personal Music library without starting playback.",
            "usage": (
                '{"function":"personal_music_search","arguments":{"query":"","genre":"","artist":"","album":"","title":"",'
                '"limit":10}}'
            ),
        },
        {
            "id": "personal_music_control",
            "description": (
                "Control the Personal Music queue: next, previous, stop, replay, shuffle, repeat, "
                "add more music to the queue, set a sleep timer, set one or more playback "
                "destinations, or bind rooms to a Person. Each Person has their own queue, and "
                "transport actions act on the music playing in the speaking room first, then that "
                "Person's own queue. User-named screens (\"set targets to the office screen\") are "
                "valid destinations and can be mixed with speakers. Use the add action to queue an "
                "extra album, playlist, artist, "
                "or genre on top of what is playing — with Smart Shuffle on, several sources mix "
                "together on the fly. Use the volume action when the user asks to set the volume "
                "across the whole speaker group (\"set all speakers to 70 percent\") — it sets every "
                "destination in the group to the same absolute level. Use mute_all / unmute_all when "
                "the user asks to mute or unmute every speaker at once. Use the sleep_timer action "
                "(minutes, 0 cancels) when the user asks for music to stop after a while, e.g. "
                "\"play my music for an hour\"; the timer force-stops playback and overrides endless "
                "playback when it hits zero."
            ),
            "usage": (
                '{"function":"personal_music_control","arguments":'
                '{"action":"next|previous|stop|replay|pause|resume|shuffle|repeat|add|sleep_timer|move|set_targets|'
                'bind_room|unbind_room|volume|mute_all|unmute_all",'
                '"targets":["Kitchen","Living Room"],"enabled":true,"mode":"off|all|one",'
                '"minutes":60,"volume_percent":70,"album":"","playlist":"","artist":"","genre":"","query":"",'
                '"person":"person_id"}}'
            ),
        },
        {
            "id": "personal_music_now_playing",
            "description": "Read the current Personal Music track, queue, target, and playback state.",
            "usage": '{"function":"personal_music_now_playing","arguments":{}}',
        },
        {
            "id": "personal_music_move",
            "description": (
                "Follow-me handoff: move or transfer the user's currently playing music to another "
                "room or speaker (\"move/transfer my music to the Master Bedroom\"), keeping the "
                "same track, position, and full queue. User-named screens (\"move my music to the "
                "office screen\") are valid destinations too, though mixing a screen with speakers "
                "does not guarantee tight sync. Only use when music is already playing; "
                "start a new queue with personal_music_play instead."
            ),
            "usage": (
                '{"function":"personal_music_move","arguments":{"rooms":["Kitchen"],"targets":[]}}'
            ),
        },
        {
            "id": "personal_music_confirm",
            "description": (
                "Confirm or cancel a music action Tater asked about, such as taking over a room "
                "another Person is listening in, or moving your music from another room. Call with "
                "choice yes|no after the user answers; choice start_new starts the requested new "
                "music instead of moving the current stream. For a resume-room question (music "
                "paused elsewhere, asked where to resume), pass choice here or there."
            ),
            "usage": '{"function":"personal_music_confirm","arguments":{"choice":"yes|no|start_new|here|there"}}',
        },
        {
            "id": "personal_music_browse",
            "description": "Browse artists, albums, genres, or tracks from the linked Emby or network-share library.",
            "usage": (
                '{"function":"personal_music_browse","arguments":{"category":"artists|albums|genres|tracks",'
                '"limit":50}}'
            ),
        },
    ]


def _origin_room_targets(origin: Optional[Dict[str, Any]], client: Any = None) -> List[str]:
    """Destinations for the room/satellite the request came from, else []."""
    store = client or globals().get("redis_client")
    context = origin if isinstance(origin, dict) else {}
    candidates = set()
    selector = _origin_value(
        context,
        "satellite_selector",
        "voice_core_selector",
        "device_selector",
    )
    if selector:
        candidates.add(selector if selector.startswith("voice_core:") else f"voice_core:{selector}")
    for room_name in _list(
        _origin_value(context, "room_name", "area_name", "room_id", "area_id")
    ):
        preferred = _preferred_room_target([room_name], store)
        if preferred:
            candidates.add(preferred)
    return _normalize_stereo_targets(sorted(candidates))


def _queue_playing_near(origin: Optional[Dict[str, Any]], client: Any = None) -> str:
    """Queue id playing on the speaker's current room or satellite, else ""."""
    store = client or globals().get("redis_client")
    candidates = set(_origin_room_targets(origin, store))
    if not candidates:
        return ""
    for target, queue_id in _occupied_targets(store).items():
        if target in candidates:
            return queue_id
    return ""


def _control_queue_id(origin: Optional[Dict[str, Any]], client: Any = None) -> str:
    """Transport actions hit the music playing nearby first, then the Person's own."""
    room_queue_id = _queue_playing_near(origin, client)
    if room_queue_id:
        return room_queue_id
    return _context_person_id(origin)


def _resume_queue_id(origin: Optional[Dict[str, Any]], control_queue_id: str, client: Any = None) -> str:
    """Which queue "resume my music" acts on.

    Stay mode keeps the nearby-first rule. Follow/ask modes read the possessive
    literally: the speaker's own paused queue wins over someone else's queue in
    the room, so the busy-room rule can keep their music where it was instead
    of restarting the other Person's queue.
    """
    store = client or globals().get("redis_client")
    own_queue_id = _context_person_id(origin)
    if not own_queue_id or own_queue_id == control_queue_id:
        return control_queue_id
    own = _player(store, own_queue_id)
    if _text(own.get("status")).lower() != "paused" or not (own.get("queue") or []):
        return control_queue_id
    if _person_resume_room_mode(own_queue_id, store) == "stay":
        return control_queue_id
    return own_queue_id


def _resume_player_for_room(
    origin: Optional[Dict[str, Any]],
    queue_id: str,
    client: Any = None,
    *,
    mode_override: str = "",
    room_targets: Optional[List[str]] = None,
) -> Tuple[Dict[str, Any], str]:
    """Resume a paused queue, honoring the Person's resume-room mode.

    Returns (player, note) where `note` explains a follow/abandon outcome and
    is appended to the voice summary. A paused queue in the speaker's own room,
    or with no resolvable speaking room, resumes in place.
    """
    store = client or globals().get("redis_client")
    player = _player(store, queue_id)
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    if not queue:
        raise ValueError("The music queue is empty.")
    mode = mode_override if mode_override in RESUME_ROOM_MODES else _person_resume_room_mode(
        queue_id, store
    )
    own_targets = _normalize_stereo_targets(_list(player.get("targets") or player.get("target")))
    if mode == "stay" or _text(player.get("status")).lower() != "paused":
        return _resume_player(person_id=queue_id, client=store), ""
    wanted = _normalize_stereo_targets(
        room_targets
        if room_targets is not None
        else _origin_room_targets(origin, store)
    )
    if not wanted or set(wanted) & set(own_targets):
        return _resume_player(person_id=queue_id, client=store), ""
    # Nearby room wins: someone else's queue (even just paused) keeps the room,
    # so a follow move is abandoned and the music resumes where it was.
    conflicts = _queue_conflicts(store, queue_id, wanted)
    if conflicts["foreign_targets"]:
        foreign_label = _queue_owner_label(
            next(iter(conflicts["foreign_queues"]), queue_id), store
        )
        note = (
            f" {_target_summary(wanted)} was still playing {foreign_label}'s music, so "
            f"yours resumed on {_target_summary(own_targets)}."
        )
        return _resume_player(person_id=queue_id, client=store), note
    if mode == "ask":
        _save_pending_confirmation(
            store,
            queue_id,
            {
                "type": "resume_room",
                "args": {},
                "origin": {},
                "targets": list(wanted),
                "queue_id": queue_id,
            },
        )
        _speak_follow_me_prompt(
            wanted,
            f"Your music is paused on {_target_summary(own_targets)}. Say 'resume it "
            "here' to play it in this room, or 'resume it there' to leave it on "
            f"{_target_summary(own_targets)}.",
        )
        return player, ""
    # follow: hand the paused queue to the speaking room (the Transfer Resume
    # Delay decides when the music actually starts there).
    resume_delay = _transfer_resume_delay(queue_id, store)
    player = _route_player_targets(
        wanted,
        restart_playing=False,
        person_id=queue_id,
        client=store,
    )
    if resume_delay > 0:
        player["resume_delay_until"] = time.time() + resume_delay
        player["resume_delay_position"] = _player_position_seconds(player)
        _save_player(player, store, queue_id)
        return player, f" It resumes there in {max(1, round(resume_delay))} seconds."
    return (
        _resume_player(person_id=queue_id, client=store),
        " Resumed at the same spot in the room you asked from.",
    )


async def run_hydra_kernel_tool(
    *,
    tool_id: str,
    args: Optional[Dict[str, Any]] = None,
    origin: Optional[Dict[str, Any]] = None,
    redis_client: Any = None,
    **_kwargs,
) -> Optional[Dict[str, Any]]:
    store = redis_client or globals().get("redis_client")
    values = args if isinstance(args, dict) else {}
    # Catalog/history/recommendation scope follows the speaking Person so a
    # linked Person browses and plays their own source by default.
    active_person_id = _context_person_id(origin)
    if tool_id == "personal_music_play":
        try:
            return await asyncio.to_thread(_play_request, values, origin, store)
        except Exception as exc:
            return {
                "ok": False,
                "error": {"code": "personal_music_play_failed", "message": _text(exc)},
                "say_hint": "Explain the music playback problem and ask for any missing song or destination detail.",
            }
    if tool_id == "personal_music_search":
        try:
            selected_provider = _person_source_id(active_person_id, store)
            if not (_person_catalog(store, selected_provider, active_person_id).get("tracks") or []):
                await asyncio.to_thread(_sync_catalog, store, selected_provider, active_person_id)
            matches = _search_tracks(
                query=values.get("query"),
                title=values.get("title") or values.get("track") or values.get("song"),
                artist=values.get("artist"),
                album=values.get("album"),
                genre=values.get("genre"),
                limit=_as_int(values.get("limit"), 10, 1, 50),
                client=store,
                provider_id=selected_provider,
                person_id=active_person_id,
            )
            public = [_public_track(track) for track in matches]
            return {
                "ok": True,
                "provider": selected_provider,
                "count": len(public),
                "tracks": public,
                "summary_for_user": f"Found {len(public)} matching track{'' if len(public) == 1 else 's'}.",
            }
        except Exception as exc:
            return {"ok": False, "error": {"code": "personal_music_search_failed", "message": _text(exc)}}
    if tool_id == "personal_music_move":
        try:
            move_queue_id = _context_person_id(origin)
            existing = _player(store, move_queue_id)
            if not (existing.get("queue") or []):
                raise ValueError("There is no music playing to move. Ask for some music first.")
            targets = _resolve_targets(
                values.get("targets") or values.get("target"),
                room=values.get("rooms") or values.get("room"),
                origin=origin,
                client=store,
                person_id=active_person_id,
            )
            if not targets:
                raise ValueError("Choose one or more valid music destinations.")
            _validate_catalog_provider_targets(targets)
            player = await asyncio.to_thread(
                _route_player_targets,
                targets,
                resume_delay=_transfer_resume_delay(move_queue_id, store),
                person_id=move_queue_id,
                client=store,
            )
            delay_note = ""
            if _as_float(player.get("resume_delay_until"), 0.0) > time.time():
                delay_note = (
                    f" It resumes there in "
                    f"{max(1, round(_as_float(player.get('resume_delay_until')) - time.time()))} "
                    "seconds."
                )
            return {
                "ok": True,
                "targets": targets,
                "status": _text(player.get("status")),
                "now_playing": _public_track(player.get("current") or {}),
                "summary_for_user": (
                    f"Moved the music to {_target_summary(targets)} at the same spot in the "
                    f"track.{delay_note}"
                ),
            }
        except Exception as exc:
            return {"ok": False, "error": {"code": "personal_music_move_failed", "message": _text(exc)}}
    if tool_id == "personal_music_confirm":
        try:
            pending = _load_pending_confirmation(store, active_person_id)
            if not pending:
                return {
                    "ok": False,
                    "error": {"code": "personal_music_confirm_empty", "message": "No music action is waiting for confirmation."},
                    "say_hint": "Mention that there is no music request waiting for an answer.",
                }
            choice = _text(values.get("choice") or values.get("confirm")).casefold() or "yes"
            pending_type = _text(pending.get("type"))
            if pending_type == "resume_room":
                _clear_pending_confirmation(store, active_person_id)
                queue_id = _text(pending.get("queue_id")) or active_person_id
                if choice in {
                    "no",
                    "there",
                    "keep",
                    "leave",
                    "old",
                    "cancel",
                    "stop",
                    "where it was",
                    "where they were",
                }:
                    player = await asyncio.to_thread(_resume_player, person_id=queue_id, client=store)
                    return {
                        "ok": True,
                        "status": _text(player.get("status")),
                        "now_playing": _public_track(player.get("current") or {}),
                        "summary_for_user": (
                            f"Resumed your music on {_target_summary(_list(player.get('targets') or player.get('target')))}."
                        ),
                    }
                # "here" (the default): resume in the room they asked from. The
                # busy-room rule still applies at confirm time — if someone
                # started playing there in the meantime, it stays where it was.
                player, note = await asyncio.to_thread(
                    _resume_player_for_room,
                    origin,
                    queue_id,
                    store,
                    mode_override="follow",
                    room_targets=_list(pending.get("targets")),
                )
                targets = _list(player.get("targets") or player.get("target"))
                return {
                    "ok": True,
                    "status": _text(player.get("status")),
                    "now_playing": _public_track(player.get("current") or {}),
                    "summary_for_user": (
                        f"Music is {_text(player.get('status')) or 'idle'} on "
                        f"{_target_summary(targets)}.{note}"
                    ),
                }
            if choice in {"no", "cancel", "stop", "leave"}:
                _clear_pending_confirmation(store, active_person_id)
                return {
                    "ok": True,
                    "summary_for_user": "Okay, I left the music as it was.",
                }
            if choice in {"start_new", "new", "fresh"} and pending_type == "relocate":
                _clear_pending_confirmation(store, active_person_id)
                return await asyncio.to_thread(
                    _play_request,
                    pending.get("args") or {},
                    pending.get("origin") or origin,
                    store,
                    force=True,
                )
            if choice not in {"yes", "ok", "confirm", "takeover", "move"}:
                return {
                    "ok": False,
                    "error": {"code": "personal_music_confirm_choice", "message": "Confirm with yes, no, or start_new."},
                    "say_hint": "Ask whether to go ahead, cancel, or start the new music instead.",
                }
            _clear_pending_confirmation(store, active_person_id)
            if pending_type == "relocate":
                player = await asyncio.to_thread(
                    _route_player_targets,
                    pending.get("targets") or [],
                    resume_delay=_transfer_resume_delay(
                        pending.get("queue_id") or active_person_id, store
                    ),
                    person_id=pending.get("queue_id") or "",
                    client=store,
                )
                return {
                    "ok": True,
                    "targets": _list(player.get("targets")),
                    "status": _text(player.get("status")),
                    "now_playing": _public_track(player.get("current") or {}),
                    "summary_for_user": (
                        f"Moved the music to {_target_summary(pending.get('targets') or [])} "
                        "at the same spot in the track."
                    ),
                }
            if pending_type == "follow_takeover":
                targets = _list(pending.get("targets"))
                queue_id = _text(pending.get("queue_id")) or active_person_id
                # The Person confirmed; take the room over regardless of mode.
                _release_targets_to(store, targets, except_queue_id=queue_id)
                player = await asyncio.to_thread(
                    _route_player_targets,
                    targets,
                    resume_delay=_transfer_resume_delay(queue_id, store),
                    person_id=queue_id,
                    client=store,
                )
                return {
                    "ok": True,
                    "targets": _list(player.get("targets")),
                    "status": _text(player.get("status")),
                    "now_playing": _public_track(player.get("current") or {}),
                    "summary_for_user": (
                        f"Moved the music to {_target_summary(targets)} "
                        "at the same spot in the track."
                    ),
                }
            return await asyncio.to_thread(
                _play_request,
                pending.get("args") or {},
                pending.get("origin") or origin,
                store,
                force=True,
            )
        except Exception as exc:
            return {"ok": False, "error": {"code": "personal_music_confirm_failed", "message": _text(exc)}}
    if tool_id == "personal_music_control":
        action = _text(values.get("action")).lower()
        resume_note = ""
        try:
            control_queue_id = _control_queue_id(origin, store)
            if action == "next":
                player = await asyncio.to_thread(_advance_player, 1, person_id=control_queue_id, client=store)
            elif action == "previous":
                player = await asyncio.to_thread(_advance_player, -1, person_id=control_queue_id, client=store)
            elif action == "stop":
                player = await asyncio.to_thread(_stop_player, person_id=control_queue_id, client=store)
            elif action == "replay":
                current = _player(store, control_queue_id)
                player = await asyncio.to_thread(
                    _start_player_index,
                    _as_int(current.get("index"), 0, 0, 100000),
                    person_id=control_queue_id,
                    client=store,
                )
            elif action in {"play", "resume"}:
                resume_queue_id = _resume_queue_id(origin, control_queue_id, store)
                player, resume_note = await asyncio.to_thread(
                    _resume_player_for_room, origin, resume_queue_id, store
                )
            elif action == "pause":
                player = await asyncio.to_thread(_pause_player, person_id=control_queue_id, client=store)
            elif action == "shuffle":
                player = _player(store, control_queue_id)
                queue = player.get("queue") if isinstance(player.get("queue"), list) else []
                current = player.get("current") if isinstance(player.get("current"), dict) else {}
                remaining = [row for row in queue if _text(row.get("id")) != _text(current.get("id"))]
                if _as_bool(values.get("enabled"), True):
                    if _person_smart_shuffle_enabled(control_queue_id, _settings(store), store):
                        remaining = _smart_shuffle_order(
                            remaining,
                            _smart_recent_ids(store, control_queue_id),
                        )
                    else:
                        random.SystemRandom().shuffle(remaining)
                player["queue"] = ([current] if current else []) + remaining
                player["index"] = 0 if current else -1
                player["shuffle"] = _as_bool(values.get("enabled"), True)
                _save_player(player, store, control_queue_id)
            elif action == "repeat":
                mode = _text(values.get("mode") or "off").lower()
                if mode not in {"off", "all", "one"}:
                    raise ValueError("Repeat mode must be off, all, or one.")
                player = _player(store, control_queue_id)
                player["repeat"] = mode
                _save_player(player, store, control_queue_id)
            elif action in {"bind_room", "unbind_room"}:
                targets = _resolve_targets(
                    values.get("targets") or values.get("target"),
                    room=values.get("rooms") or values.get("room"),
                    origin=origin,
                    client=store,
                    person_id=active_person_id,
                )
                if not targets:
                    raise ValueError("Choose one or more valid rooms to bind.")
                bind_person = _text(values.get("person")) or active_person_id
                if action == "bind_room" and not bind_person:
                    raise ValueError("Say which Person this room should follow.")
                changed = await asyncio.to_thread(
                    _set_room_bindings,
                    targets,
                    bind_person if action == "bind_room" else "",
                    store,
                )
                if not changed:
                    raise ValueError("No matching rooms were found to bind.")
                if action == "bind_room":
                    summary = (
                        f"{', '.join(changed)} will play "
                        f"{_queue_owner_label(bind_person, store)} from now on."
                    )
                else:
                    summary = f"Room binding removed for {', '.join(changed)}."
                return {
                    "ok": True,
                    "summary_for_user": summary,
                }
            elif action in {"set_target", "set_targets", "move"}:
                move_queue_id = _context_person_id(origin)
                player = _player(store, move_queue_id)
                player_provider = _provider_id(player.get("provider"))
                targets = _resolve_targets(
                    values.get("targets") or values.get("target"),
                    room=values.get("rooms") or values.get("room"),
                    origin=origin,
                    client=store,
                    provider_id=player_provider,
                    person_id=active_person_id,
                )
                if not targets:
                    raise ValueError("Choose one or more valid music destinations.")
                _validate_catalog_provider_targets(targets)
                player = await asyncio.to_thread(
                    _route_player_targets,
                    targets,
                    resume_delay=_transfer_resume_delay(move_queue_id, store),
                    person_id=move_queue_id,
                    client=store,
                )
            elif action == "sleep_timer":
                raw_minutes = values.get("minutes")
                if raw_minutes in (None, ""):
                    raw_minutes = values.get("sleep_minutes")
                if _text(raw_minutes).casefold() in {"off", "cancel", "stop", "clear"}:
                    raw_minutes = 0
                sleep_minutes = _as_int(raw_minutes, 0, 0, SLEEP_TIMER_MAX_MINUTES)
                player = await asyncio.to_thread(
                    _set_sleep_timer,
                    sleep_minutes,
                    person_id=control_queue_id,
                    client=store,
                )
                targets = _list(player.get("targets") or player.get("target"))
                return {
                    "ok": True,
                    "status": _text(player.get("status")),
                    "sleep_timer_minutes": sleep_minutes,
                    "sleep_timer_active": sleep_minutes > 0,
                    "now_playing": _public_track(player.get("current") or {}),
                    "queue_count": len(player.get("queue") or []),
                    "summary_for_user": (
                        f"Sleep timer set: the music on {_target_summary(targets)} stops in "
                        f"{sleep_minutes} minute{'' if sleep_minutes == 1 else 's'} — endless "
                        "playback will not keep it going."
                        if sleep_minutes > 0
                        else "Sleep timer cancelled; endless playback runs as usual."
                    ),
                }
            elif action == "add":
                add_result = await asyncio.to_thread(
                    _add_queue_tracks,
                    values,
                    origin=origin,
                    client=store,
                )
                player = add_result.get("player") if isinstance(add_result, dict) else None
                if not isinstance(player, dict):
                    player = _player(store, control_queue_id)
                targets = _list(player.get("targets") or player.get("target"))
                return {
                    "ok": True,
                    "status": _text(player.get("status")),
                    "target": targets[0] if targets else "",
                    "targets": targets,
                    "target_count": len(targets),
                    "added_count": _as_int(add_result.get("added"), 0, 0, 100000),
                    "now_playing": _public_track(player.get("current") or {}),
                    "queue_count": len(player.get("queue") or []),
                    "smart_pool_count": len(player.get("smart_pool") or []),
                    "summary_for_user": (
                        f"Added {_as_int(add_result.get('added'), 0, 0, 100000)} tracks to "
                        f"{_target_summary(targets)}"
                        + (
                            "; Smart Shuffle will mix them in as the queue plays."
                            if add_result.get("queued_directly") is False
                            else "."
                        )
                    ),
                }
            elif action == "volume":
                player = _player(store, control_queue_id)
                requested = values.get("volume_percent")
                if requested in (None, ""):
                    requested = values.get("volume")
                if requested in (None, ""):
                    raise ValueError(
                        'Say a percentage, e.g. "set all speakers to 70 percent".'
                    )
                volume = _as_int(requested, 75, 0, 100)
                live_result = await asyncio.to_thread(
                    _set_player_volume, player, volume, store=store
                )
                _apply_mute_warnings(player, live_result)
                _save_player(player, store, control_queue_id)
                targets = _list(player.get("targets") or player.get("target"))
                # Screen playback plays at each screen's own browser volume;
                # the requested level stays stored and applies when speakers
                # are (or become) part of the destination group.
                screen_only = bool(targets) and all(
                    _is_screen_target(target) for target in targets
                )
                summary = (
                    f"Every speaker in the group is now at {volume}%."
                    if len(targets) > 1
                    else f"Volume set to {volume}%."
                )
                if screen_only:
                    summary = (
                        f"Queued {volume}% for the group — screen playback uses each "
                        "screen's own volume control until speakers are targeted too."
                    )
                return {
                    "ok": True,
                    "status": _text(player.get("status")),
                    "volume_percent": volume,
                    "target_count": len(targets),
                    "summary_for_user": summary,
                }
            elif action in {"mute_all", "unmute_all"}:
                player = _player(store, control_queue_id)
                live_result = await asyncio.to_thread(
                    _apply_player_mute,
                    player,
                    mute=action == "mute_all",
                    client=store,
                )
                _apply_mute_warnings(player, live_result)
                _save_player(player, store, control_queue_id)
                if action == "mute_all":
                    return {
                        "ok": True,
                        "muted": True,
                        "summary_for_user": "All speakers in the group are muted.",
                    }
                restored = _as_int(player.get("volume_percent"), 75, 0, 100)
                return {
                    "ok": True,
                    "muted": False,
                    "summary_for_user": (
                        f"All speakers unmuted — volume restored to {restored}%."
                    ),
                }
            else:
                raise ValueError(
                    "Music control action must be next, previous, stop, replay, shuffle, repeat, "
                    "add, sleep_timer, set_targets, bind_room, unbind_room, volume, mute_all, "
                    "or unmute_all."
                )
            targets = _list(player.get("targets") or player.get("target"))
            return {
                "ok": True,
                "status": _text(player.get("status")),
                "target": targets[0] if targets else "",
                "targets": targets,
                "target_count": len(targets),
                "now_playing": _public_track(player.get("current") or {}),
                "queue_count": len(player.get("queue") or []),
                "continuous_radio": bool(player.get("continuous_radio")),
                "radio_name": _text(player.get("radio_name")),
                "summary_for_user": (
                    f"Music is {_text(player.get('status')) or 'idle'} on {_target_summary(targets)}."
                    f"{resume_note}"
                ),
            }
        except Exception as exc:
            return {"ok": False, "error": {"code": "personal_music_control_failed", "message": _text(exc)}}
    if tool_id == "personal_music_now_playing":
        player = _player(store, _control_queue_id(origin, store))
        targets = _list(player.get("targets") or player.get("target"))
        return {
            "ok": True,
            "queue_id": _text(player.get("queue_id")),
            "status": _text(player.get("status")),
            "target": targets[0] if targets else "",
            "targets": targets,
            "target_count": len(targets),
            "now_playing": _public_track(player.get("current") or {}),
            "queue_count": len(player.get("queue") or []),
            "queue_index": _as_int(player.get("index"), -1, -1, 100000),
            "shuffle": bool(player.get("shuffle")),
            "repeat": _text(player.get("repeat") or "off"),
            "continuous_radio": bool(player.get("continuous_radio")),
            "radio_name": _text(player.get("radio_name")),
            "summary_for_user": (
                f"{_track_label(player.get('current') or {})} is {_text(player.get('status'))} "
                f"on {_target_summary(targets)}."
                if player.get("current")
                else "Personal Music Core is idle."
            ),
        }
    if tool_id == "personal_music_browse":
        selected_provider = _person_source_id(active_person_id, store)
        catalog = _person_catalog(store, selected_provider, active_person_id)
        if not (catalog.get("tracks") or []):
            catalog = await asyncio.to_thread(
                _sync_catalog, store, selected_provider, active_person_id
            )
        category = _text(values.get("category") or "genres").lower()
        limit = _as_int(values.get("limit"), 50, 1, 200)
        if category == "tracks":
            items: Any = [
                _public_track(row)
                for row in (catalog.get("tracks") or [])[:limit]
            ]
        elif category in {"artists", "albums", "genres"}:
            items = list(catalog.get(category) or [])[:limit]
        else:
            return {
                "ok": False,
                "error": {
                    "code": "personal_music_browse_category",
                    "message": "Choose artists, albums, genres, or tracks.",
                },
            }
        return {
            "ok": True,
            "provider": selected_provider,
            "category": category,
            "items": items,
            "count": len(items),
            "summary_for_user": f"The Personal Music library has {len(items)} {category} in this result.",
        }
    return None


def _format_time(timestamp: Any) -> str:
    value = _as_float(timestamp)
    if value <= 0:
        return "never"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(value))


def _art_data_uri(track: Dict[str, Any]) -> str:
    label = _text(track.get("album") or track.get("artist") or track.get("title") or "Music")
    digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
    color_a = f"#{digest[:6]}"
    color_b = f"#{digest[6:12]}"
    safe_label = (
        label.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )[:30]
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="360" height="360" viewBox="0 0 360 360">
<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="{color_a}"/><stop offset="1" stop-color="{color_b}"/></linearGradient></defs>
<rect width="360" height="360" rx="28" fill="#15111f"/><circle cx="180" cy="165" r="118" fill="url(#g)" opacity=".92"/>
<circle cx="180" cy="165" r="48" fill="#15111f"/><circle cx="180" cy="165" r="13" fill="#ffbd59"/>
<path d="M254 77v142c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V96l-88 19v124c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V95z" fill="#fff" opacity=".9"/>
<rect x="24" y="304" width="312" height="34" rx="17" fill="#15111f" opacity=".86"/><text x="180" y="327" fill="#fff" font-family="system-ui,sans-serif" font-size="17" text-anchor="middle">{safe_label}</text>
</svg>"""
    return "data:image/svg+xml;charset=utf-8," + quote(svg, safe="")


def _artwork_proxy_url(track: Dict[str, Any]) -> str:
    track_id = _text(track.get("id"))
    if not track_id or not _as_bool(track.get("has_artwork"), False):
        return ""
    query = {
        "track_id": track_id,
        "provider": _provider_id(track.get("provider")),
    }
    version = _text(track.get("artwork_version")) or _text(
        _as_int(track.get("modified_unix"), 0, 0, 10**12)
    )
    if version and version != "0":
        query["v"] = version[:128]
    scope = _text(track.get("person_scope"))
    if scope:
        query["person"] = scope
    return f"/api/cores/personal_music_core/webhook/artwork?{urlencode(query)}"


def _artwork_display_url(track: Dict[str, Any]) -> str:
    return _artwork_proxy_url(track) or _art_data_uri(track)


def _facet_art_track(catalog: Dict[str, Any], singular: str, value: Any) -> Dict[str, Any]:
    wanted = _text(value).casefold()
    fallback: Dict[str, Any] = {}
    for track in catalog.get("tracks") or []:
        if not isinstance(track, dict):
            continue
        if singular == "album":
            matches = _text(track.get("album")).casefold() == wanted
        elif singular == "artist":
            matches = wanted in {
                _text(track.get("artist")).casefold(),
                _text(track.get("album_artist")).casefold(),
            }
        else:
            matches = wanted in {
                _text(genre).casefold() for genre in track.get("genres") or []
            }
        if not matches:
            continue
        fallback = fallback or track
        if _as_bool(track.get("has_artwork"), False):
            return track
    return fallback


def _player_item(
    player: Dict[str, Any],
    target_options: List[Dict[str, str]],
    active_provider: str,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    current = player.get("current") if isinstance(player.get("current"), dict) else {}
    status = _text(player.get("status") or "idle").upper()
    targets = _list(player.get("targets") or player.get("target"))
    target_summary = _target_summary(targets)
    player_warnings = [_text(value) for value in list(player.get("warnings") or []) if _text(value)]
    player_provider = _provider_id(player.get("provider"), active_provider)
    queue = player.get("queue") if isinstance(player.get("queue"), list) else []
    queue_count = len(queue)
    current_index = _as_int(player.get("index"), -1, -1, max(0, queue_count - 1))
    track_list = [
        {
            "id": f"queue:{index}",
            "position": index + 1,
            "title": _text(track.get("title")) or "Untitled",
            "artist": _text(track.get("artist") or track.get("album_artist")),
            "album": _text(track.get("album")),
            "duration": _text(track.get("duration_display")),
            "active": index == current_index,
            "image_src": _artwork_proxy_url(track),
            "image_alt": f"{_track_label(track)} artwork",
        }
        for index, track in enumerate(queue[:200])
        if isinstance(track, dict)
    ]
    targets_field = {
        "key": "targets",
        "label": "Play On",
        "type": "multiselect",
        "value": targets,
        "size": max(4, min(8, len(target_options))),
        "options": target_options or [{"value": "", "label": "No players discovered"}],
        "description": "Choose any mix of satellites, stereo pairs, and supported media players.",
    }
    default_volume = _as_int(player.get("volume_percent"), 75, 0, 100)
    player_rows = []
    for option in target_options:
        if not isinstance(option, dict):
            continue
        target = _text(option.get("value"))
        if not target:
            continue
        calibration = _target_calibration(target, cfg, default_volume=default_volume)
        transport_options = [
            dict(value)
            for value in list(option.get("transport_options") or [])
            if isinstance(value, dict) and _text(value.get("value"))
        ]
        player_rows.append(
            {
                "target": target,
                "label": _text(option.get("label")) or target,
                "meta": _text(
                    option.get("description")
                    or option.get("meta")
                    or option.get("room")
                    or option.get("area")
                ),
                "selected": target in targets,
                "kind": _client_target_kind(target),
                "sync_quality": (
                    "precise"
                    if _is_native_target(target)
                    else "automatic"
                    if transport_options
                    else "bridge"
                    if target.casefold().startswith("airplay:")
                    else "best_effort"
                ),
                "transport_options": transport_options,
                "airplay_bridge_target": _text(option.get("airplay_bridge_target")),
                **calibration,
            }
        )
    if status == "PLAYING":
        transport_toggle = {
            "action": "music_ui_pause",
            "label": "⏸",
            "aria_label": "Pause music",
            "tooltip": "Pause music",
            "working_text": "Pausing music...",
            "success_text": "Music paused.",
        }
    else:
        resume = status == "PAUSED" and bool(current)
        transport_toggle = {
            "action": "music_ui_play",
            "label": "▶",
            "aria_label": "Resume music" if resume else "Play music",
            "tooltip": "Resume music" if resume else "Play music",
            "working_text": "Resuming music..." if resume else "Finding and starting music...",
            "success_text": "Music resumed." if resume else "Music started.",
        }
    return {
        "id": "player:main",
        "group": "player",
        "card_variant": "player_bar",
        "title": _track_label(current) if current else "Music Player",
        "subtitle": f"{status} · {_text(current.get('album')) or 'No album selected'}",
        "detail": (
            _text(player.get("last_error"))
            if status == "ERROR" and _text(player.get("last_error"))
            else "Playback warning: " + player_warnings[0]
            if player_warnings
            else (
                f"Playing on {target_summary}. Queue position "
                f"{current_index + 1} of {queue_count}. Continuous radio keeps adding similar tracks."
            )
            if current
            else "Search your connected music library and choose where it should play."
        ),
        "hero_image_src": _artwork_display_url(current),
        "hero_image_alt": f"{_track_label(current) if current else 'Music'} artwork",
        "hero_badges": [
            {
                "label": status,
                "tone": "good" if status == "PLAYING" else ("warn" if status == "ERROR" else "muted"),
            },
            *([{"label": "MUTED", "tone": "warn"}] if _as_bool(player.get("muted")) else []),
            {
                "label": (
                    "RADIO MIXING"
                    if player.get("continuation_pending")
                    else _text(player.get("radio_name") or "Continuous Radio").upper()
                ),
                "tone": "good",
            },
            {"label": PROVIDER_LABELS[player_provider].upper(), "tone": "muted"},
            {"label": f"{queue_count} TRACKS", "tone": "muted"},
            {"label": "SHUFFLE" if player.get("shuffle") else "IN ORDER", "tone": "muted"},
            {"label": f"REPEAT {_text(player.get('repeat') or 'off').upper()}", "tone": "muted"},
            {"label": f"{len(targets)} PLAYER{'' if len(targets) == 1 else 'S'}", "tone": "muted"},
            *([{"label": "PARTIAL PLAYBACK", "tone": "warn"}] if player_warnings else []),
        ],
        "summary_rows": [
            {"label": "Artist", "value": _text(current.get("artist") or current.get("album_artist")) or "—"},
            {"label": "Album", "value": _text(current.get("album")) or "—"},
            {"label": "Genre", "value": _text(current.get("genre")) or "—"},
            {"label": "Destinations", "value": target_summary if targets else "Choose below"},
        ],
        "fields_popup": False,
        "fields_dropdown": False,
        "popup_fields": [dict(targets_field)],
        "player_rows": player_rows,
        "test_sync_action": "music_ui_test_sync",
        "settings_title": "Choose Speakers & Players",
        "settings_label": "🔊",
        "settings_aria_label": "Choose speakers and players",
        "settings_tooltip": "Choose speakers and players",
        "show_save_button": False,
        "fields": [
            {
                "key": "volume_percent",
                "label": "Volume",
                "type": "range",
                "value": _as_int(player.get("volume_percent"), 75, 0, 100),
                "min": 0,
                "max": 100,
                "step": 1,
                "suffix": "%",
                "action": "music_ui_set_volume",
            },
            {
                "key": "sleep_timer_minutes",
                "label": "Sleep Timer (minutes)",
                "type": "number",
                "value": (
                    round(_as_float(_sleep_timer_state(player).get("remaining_seconds")) / 60.0)
                    if _sleep_timer_state(player).get("active")
                    else ""
                ),
                "min": 0,
                "max": SLEEP_TIMER_MAX_MINUTES,
                "step": 1,
                "description": (
                    "Stop the music automatically after this many minutes (blank = off). At "
                    "zero the timer force-stops playback — endless playback and Smart Shuffle "
                    "cannot keep it going."
                ),
            },
        ],
        "track_list": track_list,
        "track_list_label": "Playlist",
        "track_list_action": "music_ui_queue_play",
        "track_list_shuffle": bool(player.get("shuffle")),
        "track_list_shuffle_action": "music_ui_set_shuffle",
        "save_action": "music_ui_save_player",
        "save_label": "Set Player",
        "actions": [
            {
                "action": "music_ui_previous",
                "label": "⏮",
                "aria_label": "Previous track",
                "tooltip": "Previous track",
                "working_text": "Loading previous track...",
                "success_text": "Previous track started.",
            },
            transport_toggle,
            {
                "action": "music_ui_stop",
                "label": "■",
                "aria_label": "Stop music",
                "tooltip": "Stop music",
                "tone": "danger",
                "working_text": "Stopping music...",
                "success_text": "Music stopped.",
            },
            {
                "action": "music_ui_next",
                "label": "⏭",
                "aria_label": "Next track",
                "tooltip": "Next track",
                "working_text": "Loading next track...",
                "success_text": "Next track started.",
            },
            {
                "action": "music_ui_mute_all",
                "label": "🔇 All",
                "aria_label": "Mute all speakers",
                "tooltip": "Mute every selected speaker",
                "working_text": "Muting all speakers...",
                "success_text": "All speakers muted.",
            },
            {
                "action": "music_ui_unmute_all",
                "label": "🔊 All",
                "aria_label": "Unmute all speakers",
                "tooltip": "Unmute every selected speaker",
                "working_text": "Unmuting all speakers...",
                "success_text": "All speakers unmuted.",
            },
        ],
    }


def _search_item(catalog: Dict[str, Any]) -> Dict[str, Any]:
    track_count = len(catalog.get("tracks") or [])
    searchable = (
        f"{track_count} tracks"
        f" · {len(catalog.get('artists') or [])} artists"
        f" · {len(catalog.get('albums') or [])} albums"
        f" · {len(catalog.get('genres') or [])} genres"
        if track_count
        else "0 tracks — connect a source and sync the library first"
    )
    return {
        "id": "search:music",
        "group": "search",
        "card_variant": "music_search",
        "title": "Search Your Library",
        "subtitle": "Find a genre, artist, album, or song and start a fresh track list.",
        "summary_rows": [{"label": "Searchable Library", "value": searchable}],
        "fields_popup": False,
        "fields_dropdown": False,
        "fields": [
            {
                "key": "query",
                "label": "Find Music",
                "type": "text",
                "value": "",
                "placeholder": "Reggae, Bob Marley, Exodus, or a song title",
            },
        ],
        "run_action": "music_ui_play",
        "run_label": "Play Search",
    }


def _facet_items(catalog: Dict[str, Any], category: str, label: str) -> List[Dict[str, Any]]:
    items = []
    singular = category[:-1] if category.endswith("s") else category
    for value in list(catalog.get(category) or [])[:500]:
        artwork_track = _facet_art_track(catalog, singular, value)
        items.append(
            {
                "id": f"{singular}:{value}",
                "group": category,
                "card_variant": "library_tile",
                "title": _text(value),
                "subtitle": f"Browse and play this {singular}.",
                "hero_image_src": (
                    _artwork_display_url(artwork_track)
                    if artwork_track
                    else _art_data_uri({singular: value, "title": value})
                ),
                "hero_badges": [{"label": label.upper(), "tone": "muted"}],
                "run_action": "music_ui_facet_play",
                "run_label": f"Play {label}",
            }
        )
    return items


def _client_target_kind(value: Any) -> str:
    token = _text(value).lower()
    if token.startswith("voice_core:"):
        return "satellite"
    if token.startswith("airplay:"):
        return "airplay_bridge"
    if token.startswith(("ha:", "sonos:", "integration:")):
        return "media_player"
    return "player"


def _personalized_client_tracks(
    catalog: Dict[str, Any],
    *,
    provider_id: str,
    limit: int,
    client: Any = None,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Build a fast For You feed from AI picks and recent listening affinity."""
    store = client or globals().get("redis_client")
    tracks = [dict(row) for row in catalog.get("tracks") or [] if isinstance(row, dict)]
    clean_limit = max(1, min(200, int(limit)))
    history = [
        row
        for row in _listening_history(store)
        if _provider_id(row.get("provider")) == provider_id
    ][-MAX_HISTORY_EVENTS:]
    if not history:
        return tracks[:clean_limit], {
            "kind": "library",
            "title": "Library",
            "summary": "Play some music and Tater will personalize this list for you.",
            "history_event_count": 0,
            "ai_seed_count": 0,
        }

    artist_affinity: Dict[str, float] = {}
    album_affinity: Dict[str, float] = {}
    genre_affinity: Dict[str, float] = {}
    track_plays: Dict[str, int] = {}
    history_count = len(history)
    for position, event in enumerate(history):
        # Recent plays carry more weight, while older plays still preserve long-term taste.
        recency = 1.0 + (4.0 * (position + 1) / history_count)
        artist = _text(event.get("album_artist") or event.get("artist")).casefold()
        album = _text(event.get("album")).casefold()
        track_id = _text(event.get("track_id"))
        if artist:
            artist_affinity[artist] = artist_affinity.get(artist, 0.0) + (12.0 * recency)
        if album:
            album_affinity[album] = album_affinity.get(album, 0.0) + (8.0 * recency)
        for raw_genre in event.get("genres") or []:
            genre = _text(raw_genre).casefold()
            if genre:
                genre_affinity[genre] = genre_affinity.get(genre, 0.0) + (5.0 * recency)
        if track_id:
            track_plays[track_id] = track_plays.get(track_id, 0) + 1

    catalog_track_ids = {
        _text(track.get("id"))
        for track in tracks
        if _text(track.get("id"))
    }
    published = _recommendations(store)
    ai_seed_ids: List[str] = []
    seen_ai_ids = set()
    if _provider_id(published.get("provider"), "") == provider_id:
        for playlist in published.get("playlists") or []:
            if not isinstance(playlist, dict):
                continue
            for raw_track_id in playlist.get("track_ids") or []:
                track_id = _text(raw_track_id)
                if track_id in catalog_track_ids and track_id not in seen_ai_ids:
                    seen_ai_ids.add(track_id)
                    ai_seed_ids.append(track_id)
    ai_rank = {track_id: position for position, track_id in enumerate(ai_seed_ids)}

    recent_window = min(32, max(8, history_count // 3))
    recently_played_ids = {
        _text(event.get("track_id"))
        for event in history[-recent_window:]
        if _text(event.get("track_id"))
    }
    mix_token = _text(published.get("generated_at")) or _text(history[-1].get("played_at"))

    def rank(track: Dict[str, Any]) -> tuple[Any, ...]:
        track_id = _text(track.get("id"))
        artist = _text(track.get("album_artist") or track.get("artist")).casefold()
        album = _text(track.get("album")).casefold()
        genres = {
            _text(value).casefold()
            for value in track.get("genres") or []
            if _text(value)
        }
        score = (
            artist_affinity.get(artist, 0.0)
            + album_affinity.get(album, 0.0)
            + sum(genre_affinity.get(genre, 0.0) for genre in genres)
            + min(60.0, track_plays.get(track_id, 0) * 6.0)
        )
        if track_id in ai_rank:
            score += max(1000.0, 10000.0 - (ai_rank[track_id] * 40.0))
        if track_id in recently_played_ids:
            score -= 12000.0
        dispersion = hashlib.sha256(
            f"{provider_id}\x00{mix_token}\x00{track_id}".encode("utf-8")
        ).hexdigest()
        return (
            -score,
            dispersion,
            _text(track.get("title")).casefold(),
        )

    ranked = sorted(tracks, key=rank)

    # Reorder only within small relevance bands so the feed stays varied without
    # allowing low-affinity catalog tracks to jump ahead of strong matches.
    selected: List[Dict[str, Any]] = []
    artist_counts: Dict[str, int] = {}
    album_counts: Dict[tuple[str, str], int] = {}
    band_size = 24
    for offset in range(0, len(ranked), band_size):
        band = list(enumerate(ranked[offset : offset + band_size]))
        while band and len(selected) < clean_limit:
            position, track = min(
                band,
                key=lambda item: (
                    artist_counts.get(
                        _text(item[1].get("album_artist") or item[1].get("artist")).casefold(),
                        0,
                    ),
                    album_counts.get(
                        (
                            _text(item[1].get("album_artist") or item[1].get("artist")).casefold(),
                            _text(item[1].get("album")).casefold(),
                        ),
                        0,
                    ),
                    item[0],
                ),
            )
            band.remove((position, track))
            selected.append(track)
            artist = _text(track.get("album_artist") or track.get("artist")).casefold()
            album_key = (artist, _text(track.get("album")).casefold())
            artist_counts[artist] = artist_counts.get(artist, 0) + 1
            album_counts[album_key] = album_counts.get(album_key, 0) + 1
        if len(selected) >= clean_limit:
            break

    ai_seed_count = len(ai_seed_ids)
    summary = (
        "Tater blended your AI picks with the artists, albums, and genres you play."
        if ai_seed_count
        else "Tater shaped these songs from the artists, albums, and genres you play."
    )
    return selected, {
        "kind": "personalized",
        "title": "For You",
        "summary": summary,
        "history_event_count": history_count,
        "ai_seed_count": ai_seed_count,
    }


def get_client_music_state(
    *,
    query: Any = "",
    limit: int = 60,
    refresh: bool = False,
    client: Any = None,
) -> Dict[str, Any]:
    """Return the credential-free Music Core state used by trusted Tater clients."""
    store = client or globals().get("redis_client")
    cfg = _settings(store)
    active_provider = _provider_id(cfg.get("provider"))
    connected = _paired(cfg, active_provider)
    catalog: Dict[str, Any] = {}
    if active_provider in CATALOG_PROVIDER_IDS:
        if refresh and connected:
            catalog = _sync_catalog(store, active_provider)
        else:
            catalog = _catalog(store, active_provider)
    player = _reconcile_native_playback(_player(store), store)
    clean_limit = _as_int(limit, 60, 1, 200)
    clean_query = _text(query)
    if clean_query and active_provider in CATALOG_PROVIDER_IDS:
        tracks = _search_tracks(
            query=clean_query,
            limit=clean_limit,
            client=store,
            provider_id=active_provider,
        )
        track_feed = {
            "kind": "search",
            "title": "Results",
            "summary": "",
            "history_event_count": 0,
            "ai_seed_count": 0,
        }
    else:
        tracks, track_feed = _personalized_client_tracks(
            catalog,
            provider_id=active_provider,
            limit=clean_limit,
            client=store,
        )
    target_options = _target_options(
        current_values=player.get("targets"),
        provider_id=active_provider,
    )
    target_options, local_airplay_targets = _split_local_airplay_receiver_options(
        target_options,
        cfg,
    )
    saved_player_targets = _list(player.get("targets") or player.get("target"))
    saved_player_targets = [
        target
        for target in saved_player_targets
        if target.casefold() not in local_airplay_targets
    ]
    player_targets = _canonical_option_targets(saved_player_targets, target_options)
    if player_targets != saved_player_targets:
        player["targets"] = player_targets
        _save_player(player, store)
    targets: List[Dict[str, Any]] = []
    for row in target_options:
        if not isinstance(row, dict):
            continue
        target_id = _text(row.get("value"))
        if not target_id:
            continue
        transport_options = [
            {
                "value": _text(option.get("value")),
                "label": _text(option.get("label")) or _text(option.get("value")),
            }
            for option in list(row.get("transport_options") or [])
            if isinstance(option, dict) and _text(option.get("value"))
        ]
        calibration = _target_calibration(
            target_id,
            cfg,
            default_volume=_as_int(player.get("volume_percent"), 75, 0, 100),
        )
        targets.append(
            {
                "id": target_id,
                "label": _text(row.get("label")) or target_id,
                "kind": _client_target_kind(target_id),
                "description": _text(row.get("description") or row.get("meta")),
                "airplay_bridge_target": _text(row.get("airplay_bridge_target")),
                "transport_options": transport_options,
                "transport_mode": (
                    _player_transport_mode(calibration.get("transport_mode"))
                    if transport_options
                    else ""
                ),
            }
        )
    providers = [
        {
            "id": provider_id,
            "label": label,
            "connected": _paired(cfg, provider_id),
            "active": provider_id == active_provider,
            "local_playback": provider_id in CATALOG_PROVIDER_IDS,
        }
        for provider_id, label in PROVIDER_LABELS.items()
    ]
    queue = [
        _public_track(track)
        for track in list(player.get("queue") or [])[:200]
        if isinstance(track, dict)
    ]
    duration_seconds = max(
        0.0,
        _as_float(
            player.get("duration_seconds")
            or (player.get("current") or {}).get("duration_seconds")
        ),
    )
    position_seconds = _player_position_seconds(player)
    if duration_seconds > 0:
        position_seconds = min(duration_seconds, position_seconds)

    published = _recommendations(store)
    recommendations: List[Dict[str, Any]] = []
    if _provider_id(published.get("provider"), "") == active_provider:
        catalog_tracks = {
            _text(track.get("id")): track
            for track in catalog.get("tracks") or []
            if isinstance(track, dict) and _text(track.get("id"))
        }
        for playlist in published.get("playlists") or []:
            if not isinstance(playlist, dict) or not _text(playlist.get("id")):
                continue
            playlist_tracks = [
                _public_track(catalog_tracks[track_id])
                for track_id in (_text(value) for value in playlist.get("track_ids") or [])
                if track_id in catalog_tracks
            ]
            if not playlist_tracks:
                continue
            recommendations.append(
                {
                    "id": _text(playlist.get("id")),
                    "name": _text(playlist.get("name")) or "Tater Mix",
                    "description": _text(playlist.get("description")),
                    "tracks": playlist_tracks,
                    "track_count": len(playlist_tracks),
                    "artwork_url": _text(playlist_tracks[0].get("artwork_url")),
                }
            )
    return {
        "ok": True,
        "available": True,
        "version": __version__,
        "provider": {
            "id": active_provider,
            "label": PROVIDER_LABELS[active_provider],
            "connected": connected,
            "local_playback": active_provider in CATALOG_PROVIDER_IDS,
        },
        "providers": providers,
        "tracks": [_public_track(track) for track in tracks],
        "track_feed": track_feed,
        "track_count": len(catalog.get("tracks") or []),
        "artists": list(catalog.get("artists") or [])[:200],
        "albums": list(catalog.get("albums") or [])[:200],
        "genres": list(catalog.get("genres") or [])[:200],
        "recommendations": recommendations,
        "recommendation_summary": _text(published.get("summary")),
        "recommendation_generated_at": _as_float(published.get("generated_at")),
        "targets": targets,
        "player": {
            "status": _text(player.get("status") or "idle"),
            "provider": _provider_id(player.get("provider"), active_provider),
            "current": _public_track(player.get("current") or {}),
            "targets": player_targets,
            "target": player_targets[0] if player_targets else "",
            "queue_count": len(player.get("queue") or []),
            "queue_index": _as_int(player.get("index"), -1, -1, 100000),
            "queue": queue,
            "shuffle": bool(player.get("shuffle")),
            "repeat": _text(player.get("repeat") or "off"),
            "continuous_radio": bool(player.get("continuous_radio")),
            "radio_name": _text(player.get("radio_name")),
            "continuation_pending": bool(player.get("continuation_pending")),
            "position_seconds": position_seconds,
            "duration_seconds": duration_seconds,
            "seekable": bool(player.get("current") and duration_seconds > 0),
            "volume_percent": _as_int(
                player.get("volume_percent"),
                _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                0,
                100,
            ),
        },
        "synced_at": _as_float(catalog.get("synced_at")),
    }


def _find_track_in_catalog(catalog: Dict[str, Any], wanted_id: str) -> Optional[Dict[str, Any]]:
    for track in catalog.get("tracks") or []:
        if isinstance(track, dict) and _text(track.get("id")) == wanted_id:
            return dict(track)
    return None


def _client_track(
    track_id: Any,
    provider_id: str,
    client: Any,
    person_id: Any = "",
) -> Dict[str, Any]:
    wanted = _text(track_id)
    if not wanted:
        raise ValueError("Choose a track first.")
    wanted_provider = _provider_id(provider_id)
    scoped = _text(person_id)
    if scoped:
        # Person-scoped artwork/stream requests resolve against that Person's
        # own catalog and fetch with their own source credentials.
        catalog = _catalog(client, wanted_provider, scoped)
        track = _find_track_in_catalog(catalog, wanted)
        if track is None:
            raise ValueError("That track is no longer in the active music library.")
        track["person_scope"] = scoped
        return track
    catalog = _catalog(client, wanted_provider)
    track = _find_track_in_catalog(catalog, wanted)
    if track is not None:
        return track
    # Artwork URLs built from a viewed Person's catalog carry no scope, so
    # search the linked Persons' catalogs next and remember whose it was.
    # This runs before any household sync: a Person's track must resolve even
    # when the household source is disconnected or a different provider.
    for linked_id in sorted(_person_links(client)):
        if _person_source_id(linked_id, client) != wanted_provider:
            continue
        person_catalog = _catalog(client, wanted_provider, linked_id)
        track = _find_track_in_catalog(person_catalog, wanted)
        if track is not None:
            track["person_scope"] = linked_id
            return track
    if not (catalog.get("tracks") or []):
        # Still unknown: refresh the household catalog once (best effort — a
        # disconnected source must not break Person-scoped artwork).
        try:
            catalog = _sync_catalog(client, wanted_provider)
        except Exception:
            catalog = {}
        track = _find_track_in_catalog(catalog, wanted)
        if track is not None:
            return track
    raise ValueError("That track is no longer in the active music library.")


def _client_tracks(track_ids: Any, provider_id: str, client: Any) -> List[Dict[str, Any]]:
    requested_ids = _list(track_ids)[:200]
    if not requested_ids:
        raise ValueError("Choose at least one track first.")
    catalog = _catalog(client, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(client, provider_id)
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    tracks: List[Dict[str, Any]] = []
    seen = set()
    for track_id in requested_ids:
        if track_id in seen:
            continue
        track = track_by_id.get(track_id)
        if track is None:
            raise ValueError("One or more tracks are no longer in the active music library.")
        seen.add(track_id)
        tracks.append(dict(track))
    return tracks


def _client_local_continuation(
    values: Dict[str, Any],
    provider_id: str,
    client: Any,
) -> Dict[str, Any]:
    """Choose the next continuous-radio batch without starting a Tater player."""
    catalog = _catalog(client, provider_id)
    if not (catalog.get("tracks") or []):
        catalog = _sync_catalog(client, provider_id)
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    requested_ids = _list(values.get("track_ids"))[:200]
    current_track_id = _text(values.get("track_id"))
    queue = [
        dict(track_by_id[track_id])
        for track_id in requested_ids
        if track_id in track_by_id
    ]
    if not queue and current_track_id in track_by_id:
        queue = [dict(track_by_id[current_track_id])]
    if not queue:
        raise ValueError("Little Spud's current music queue is no longer in the active library.")
    index = _as_int(
        values.get("queue_index"),
        next(
            (
                position
                for position, track in enumerate(queue)
                if _text(track.get("id")) == current_track_id
            ),
            0,
        ),
        0,
        max(0, len(queue) - 1),
    )
    current = queue[index]
    _record_listening_history(
        current,
        ["little_spud:local"],
        client=client,
    )
    player = {
        "status": "playing",
        "provider": provider_id,
        "queue": queue,
        "queue_original": queue,
        "index": index,
        "current": current,
        "targets": ["little_spud:local"],
        "queue_session_id": _text(values.get("queue_session_id")) or uuid.uuid4().hex,
        "continuous_radio": True,
    }
    selections: List[Dict[str, Any]] = []
    station_name = "Little Spud Continuous Radio"
    source = "smart_fallback"
    acquired = _client_continuation_lock.acquire(blocking=False)
    loop: Optional[asyncio.AbstractEventLoop] = None
    try:
        if acquired:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            model = _get_primary_llm_client_from_env()
            selections, station_name = _select_continuation_tracks(
                loop,
                model,
                player,
                client,
            )
            source = "ai"
    except Exception as exc:
        logger.warning("[Music] Little Spud AI continuation failed; using smart fallback: %s", exc)
    finally:
        if loop is not None:
            loop.close()
            asyncio.set_event_loop(None)
        if acquired:
            _client_continuation_lock.release()
    if not selections:
        selections = _fallback_continuation_tracks(
            player,
            client,
            count=CONTINUATION_BATCH_TRACKS,
        )
    if not selections:
        raise ValueError("The active music library has no tracks for continuous radio.")
    return {
        "ok": True,
        "tracks": [_public_track(track) for track in selections[:CONTINUATION_BATCH_TRACKS]],
        "station_name": station_name,
        "source": source,
        "continuous_radio": True,
    }


def run_client_music_action(
    action: Any,
    payload: Optional[Dict[str, Any]] = None,
    *,
    client: Any = None,
) -> Dict[str, Any]:
    """Run a bounded Music Core action for native Tater clients."""
    store = client or globals().get("redis_client")
    values = payload if isinstance(payload, dict) else {}
    command = _text(action).lower()
    cfg = _settings(store)
    selected_provider = _provider_id(
        values.get("provider"),
        _provider_id(cfg.get("provider")),
    )
    if command in {"refresh", "sync"}:
        _sync_catalog(store, selected_provider)
        return get_client_music_state(client=store)
    if command == "local_play_started":
        track = _client_track(values.get("track_id"), selected_provider, store)
        _record_listening_history(
            track,
            ["little_spud:local"],
            client=store,
        )
        return {"ok": True}
    if command in {"continue_local", "local_continuation"}:
        return _client_local_continuation(values, selected_provider, store)
    if command in {"play_recommendation", "recommendation"}:
        player = _play_recommendation(
            values.get("recommendation_id"),
            store,
            requested_targets=values.get("targets") or values.get("target"),
            volume_percent=values.get("volume_percent"),
        )
        return {
            "ok": True,
            "summary_for_user": (
                f"Playing a Tater recommendation on "
                f"{_target_summary(player.get('targets'))}."
            ),
            "state": get_client_music_state(client=store),
        }
    if command in {"play_queue", "replace_queue", "play_album"}:
        tracks = _client_tracks(values.get("track_ids"), selected_provider, store)
        targets = _resolve_targets(
            values.get("targets") or values.get("target"),
            client=store,
            provider_id=selected_provider,
        )
        if not targets:
            raise ValueError("Choose a satellite or media player.")
        _validate_catalog_provider_targets(targets)
        player = _create_and_start_queue(
            tracks,
            targets=targets,
            shuffle=False,
            volume_percent=_as_int(
                values.get("volume_percent"),
                _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                0,
                100,
            ),
            client=store,
        )
        return {
            "ok": True,
            "summary_for_user": (
                f"Playing {len(tracks)} tracks on {_target_summary(targets)}."
            ),
            "now_playing": _public_track(player.get("current") or {}),
            "state": get_client_music_state(client=store),
        }
    if command == "play":
        track_id = _text(values.get("track_id"))
        if track_id:
            track = _client_track(track_id, selected_provider, store)
            targets = _resolve_targets(
                values.get("targets") or values.get("target"),
                client=store,
                provider_id=selected_provider,
            )
            if not targets:
                raise ValueError("Choose a satellite or media player.")
            _validate_catalog_provider_targets(targets)
            player = _create_and_start_queue(
                [track],
                targets=targets,
                shuffle=False,
                volume_percent=_as_int(
                    values.get("volume_percent"),
                    _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                    0,
                    100,
                ),
                client=store,
            )
            result = {
                "ok": True,
                "summary_for_user": (
                    f"Playing {_track_label(track)} on {_target_summary(targets)}."
                ),
                "now_playing": _public_track(player.get("current") or {}),
            }
        else:
            # Client playback is deliberate: take over busy rooms without the
            # TTS confirmation used for voice requests.
            result = _play_request(values, {}, store, force=True)
        return {
            **result,
            "state": get_client_music_state(client=store),
        }

    player = _reconcile_native_playback(_player(store), store)
    requested_targets = values.get("targets") or values.get("target")
    routed_targets: List[str] = []
    if _list(requested_targets):
        routed_targets = _resolve_targets(
            requested_targets,
            client=store,
            provider_id=_provider_id(player.get("provider"), selected_provider),
        )
        if not routed_targets:
            raise ValueError("Choose one or more valid music destinations.")
        _validate_catalog_provider_targets(routed_targets)
    if command in {"set_target", "set_targets"}:
        updated = _route_player_targets(routed_targets, client=store)
        return {
            "ok": True,
            "player": {
                "status": _text(updated.get("status")),
                "current": _public_track(updated.get("current") or {}),
            },
            "state": get_client_music_state(client=store),
        }
    if command == "set_volume":
        if routed_targets:
            player = _route_player_targets(routed_targets, client=store)
        volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        live_result = {"sent_count": 0, "warnings": []}
        if _text(player.get("status")).lower() == "playing":
            live_result = _set_target_volume(player, volume)
            if _as_int(live_result.get("sent_count"), 0, 0, 10000) <= 0:
                warning = "; ".join(
                    _text(value)
                    for value in list(live_result.get("warnings") or [])
                    if _text(value)
                )
                raise ValueError(warning or "The active players could not change volume.")
        _persist_shared_player_volume(player, volume, client=store)
        player["warnings"] = [
            _text(value)
            for value in list(live_result.get("warnings") or [])
            if _text(value)
        ]
        _save_player(player, store)
        return {
            "ok": True,
            "state": get_client_music_state(client=store),
        }
    if command == "seek":
        if routed_targets:
            player = _route_player_targets(
                routed_targets,
                restart_playing=False,
                client=store,
            )
        updated = _seek_player(
            _as_float(values.get("position_seconds")),
            client=store,
        )
        return {
            "ok": True,
            "player": {
                "status": _text(updated.get("status")),
                "current": _public_track(updated.get("current") or {}),
            },
            "state": get_client_music_state(client=store),
        }
    if command in {"next", "previous", "stop", "replay", "play", "resume", "pause"}:
        route_before_action = command in {"next", "previous", "replay", "play", "resume"}
        if routed_targets and route_before_action:
            player = _route_player_targets(
                routed_targets,
                restart_playing=command in {"play", "resume"},
                client=store,
            )
        if command == "next":
            updated = _advance_player(1, client=store)
        elif command == "previous":
            updated = _advance_player(-1, client=store)
        elif command == "stop":
            updated = _stop_player(client=store)
        elif command == "replay":
            updated = _start_player_index(
                _as_int(player.get("index"), 0, 0, 100000),
                client=store,
            )
        elif command in {"play", "resume"}:
            updated = _resume_player(client=store)
        else:
            updated = _pause_player(client=store)
        if routed_targets and command in {"stop", "pause"}:
            updated = _route_player_targets(routed_targets, client=store)
        return {
            "ok": True,
            "player": {
                "status": _text(updated.get("status")),
                "current": _public_track(updated.get("current") or {}),
            },
            "state": get_client_music_state(client=store),
        }
    raise ValueError(
        "Music action must be play, play_queue, play_recommendation, next, previous, stop, "
        "pause, resume, replay, seek, set_volume, continue_local, or refresh."
    )


def get_client_music_stream_source(
    track_id: Any,
    *,
    provider_id: Any = "",
    client: Any = None,
) -> Dict[str, Any]:
    """Resolve one Tater Tube stream for Tater's authenticated client proxy."""
    store = client or globals().get("redis_client")
    selected_provider = _provider_id(
        provider_id,
        _provider_id(_settings(store).get("provider")),
    )
    track = _client_track(track_id, selected_provider, store)
    # A track from a Person's own catalog streams with their credentials
    # (person_scope is stamped by _client_track; household tracks carry none).
    source_url = _provider(store, selected_provider, track.get("person_scope")).stream_url(track)
    if not source_url:
        raise ValueError("That track does not have a playable stream.")
    return {
        "source_url": source_url,
        "media_type": _track_media_type(track),
        "filename": Path(_text(track.get("path")) or "music-track").name,
        "track": _public_track(track),
        "provider": selected_provider,
    }


def _provider_connection_detail(
    cfg: Dict[str, Any],
    provider_id: str,
) -> str:
    spec = PROVIDER_FIELD_SPECS.get(_provider_id(provider_id, ""))
    if spec is None:
        return "This music source is not enabled in this build."
    return spec.connection_detail(cfg)


def _provider_fields(cfg: Dict[str, Any], provider_id: str) -> List[Dict[str, Any]]:
    spec = PROVIDER_FIELD_SPECS.get(_provider_id(provider_id, ""))
    if spec is None:
        return []
    global_fields = getattr(spec, "global_fields", None)
    return global_fields(cfg) if callable(global_fields) else []


def _provider_cards(
    cfg: Dict[str, Any],
    active_provider: str,
    client: Any = None,
) -> List[Dict[str, Any]]:
    """Global source cards under Sources.

    These describe the household's shared source, so the track badge reads the
    global catalog stats — never a linked Person's personal library counts.
    """
    global_stats = _catalog_stats("", client)
    cards: List[Dict[str, Any]] = []
    for provider_id in CATALOG_PROVIDER_ORDER:
        label = PROVIDER_LABELS[provider_id]
        connected = _paired(cfg, provider_id)
        actions: List[Dict[str, Any]] = [
            {
                "action": "music_provider_connect",
                "label": "Connect / Test",
                "working_text": f"Connecting to {label}...",
                "success_text": f"{label} connected.",
            }
        ]
        if connected:
            actions.extend(
                [
                    {
                        "action": "music_provider_activate",
                        "label": "Rescan Library",
                        "working_text": f"Loading the {label} library...",
                        "success_text": f"{label} library loaded.",
                    },
                    {
                        "action": "music_provider_disconnect",
                        "label": "Disconnect",
                        "tone": "danger",
                        "confirm": f"Disconnect Personal Music Core from {label}?",
                    },
                ]
            )
        cards.append(
            {
                "id": f"provider:{provider_id}",
                "group": "providers",
                "title": label,
                "subtitle": "Connected music source" if connected else "Not connected",
                "detail": _provider_connection_detail(cfg, provider_id),
                "hero_badges": [
                    {
                        "label": "CONNECTED" if connected else "SETUP NEEDED",
                        "tone": "good" if connected else "warn",
                    },
                    {
                        "label": (
                            f"{_as_int(global_stats.get('track_count'), 0, 0, 10**9)} TRACKS"
                            if _text(global_stats.get("status")) == "ok"
                            and _provider_id(global_stats.get("provider")) == provider_id
                            else "0 TRACKS"
                        ),
                        "tone": "muted",
                    },
                ],
                "fields": _provider_fields(cfg, provider_id),
                "fields_popup": False,
                "fields_dropdown": True,
                "actions": actions,
            }
        )
    return cards


def _recommendation_ui_items(
    cfg: Dict[str, Any],
    catalog: Dict[str, Any],
    runtime: Dict[str, Any],
    active_provider: str,
    client: Any = None,
    person_id: Any = "",
) -> List[Dict[str, Any]]:
    assistant_name = _assistant_first_name(client)
    recommendations_label = _recommendations_label(client)
    default_mix_title = f"{assistant_name} Mix"
    history = [
        row
        for row in _listening_history(client, person_id)
        if _provider_id(row.get("provider")) == active_provider
    ]
    enabled = _person_recommendations_enabled(person_id, cfg, client)
    published = _recommendations(client, person_id)
    if _provider_id(published.get("provider"), "") != active_provider:
        published = {}
    generated_at = _as_float(published.get("generated_at"))
    last_error = _text(runtime.get("last_recommendation_error"))
    if not enabled:
        detail = f"Turn on {recommendations_label} in Settings to create AI-named music mixes."
    elif not history:
        detail = f"Start playing music and {assistant_name} will learn enough to prepare your first mixes."
    elif last_error and not published:
        detail = last_error
    elif generated_at:
        detail = (
            f"Built from {len(history)} listening event{'' if len(history) == 1 else 's'} · "
            f"updated {_format_time(generated_at)}"
        )
    else:
        detail = f"{assistant_name} has listening history and is ready to prepare your first mixes."

    items: List[Dict[str, Any]] = [
        {
            "id": "recommendations:overview",
            "group": "recommendations",
            "card_variant": "recommendations_intro",
            "title": recommendations_label,
            "assistant_name": assistant_name,
            "subtitle": _text(published.get("summary"))
            or "Named playlists made from what you actually listen to.",
            "detail": detail,
            "generated_at": generated_at,
            "history_event_count": len(history),
            "recommendations_enabled": enabled,
            "refresh_available": bool(enabled and history),
            "refresh_running": bool(
                _recommendation_lock.locked()
                or (_recommendation_thread is not None and _recommendation_thread.is_alive())
            ),
            "run_action": "music_recommendations_refresh",
            "run_label": "Refresh Recommendations",
        }
    ]
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    for playlist in published.get("playlists") or []:
        if not isinstance(playlist, dict) or not _text(playlist.get("id")):
            continue
        recommendation_items = []
        album_count = 0
        song_count = 0
        for selection in playlist.get("items") or []:
            if not isinstance(selection, dict):
                continue
            selection_type = _text(selection.get("type")) or "song"
            if selection_type == "album":
                album_count += 1
            else:
                song_count += 1
            art_track = track_by_id.get(_text(selection.get("image_track_id"))) or {}
            recommendation_items.append(
                {
                    "id": _text(selection.get("candidate_id")),
                    "type": selection_type,
                    "title": _text(selection.get("title")) or "Untitled",
                    "artist": _text(selection.get("artist")),
                    "album": _text(selection.get("album")),
                    "reason": _text(selection.get("reason")),
                    "track_count": _as_int(selection.get("track_count"), 1, 1, 10000),
                    "image_src": _artwork_display_url(art_track) if art_track else "",
                    "image_alt": f"{_text(selection.get('title')) or 'Music'} artwork",
                }
            )
        if not recommendation_items:
            continue
        hero_src = _text(recommendation_items[0].get("image_src"))
        items.append(
            {
                "id": f"recommendation:{_text(playlist.get('id'))}",
                "group": "recommendations",
                "card_variant": "recommendation_playlist",
                "title": _text(playlist.get("name")) or default_mix_title,
                "subtitle": _text(playlist.get("description")),
                "hero_image_src": hero_src,
                "hero_image_alt": f"{_text(playlist.get('name')) or default_mix_title} artwork",
                "hero_badges": [
                    {"label": "AI PLAYLIST", "tone": "good"},
                    {"label": f"{album_count} ALBUM{'' if album_count == 1 else 'S'}", "tone": "muted"},
                    {"label": f"{song_count} SONG{'' if song_count == 1 else 'S'}", "tone": "muted"},
                    {"label": f"{len(playlist.get('track_ids') or [])} TRACKS", "tone": "muted"},
                ],
                "recommendation_items": recommendation_items,
                "run_action": "music_recommendation_play",
                "run_label": "Play Playlist",
            }
        )
    return items


def _follow_me_link_fields(
    cfg: Dict[str, Any],
    link: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Follow-Me presence fields shared by the People section cards."""
    return [
        {
            "key": "person_link_follow_me_entity",
            "label": "Home Assistant Person",
            "type": "text",
            "value": _text(link.get("follow_me_person_entity")),
            "placeholder": "person.john",
            "description": (
                "The Home Assistant person entity for this Person (updated as they move, for "
                "example by your BLE trackers). With Follow-Me enabled, their music follows "
                "them to the room this entity reports."
            ),
        },
        {
            "key": "person_link_follow_me_room_overrides",
            "label": "Zone to Room Overrides (optional)",
            "type": "text",
            "value": _text(link.get("follow_me_room_overrides")),
            "placeholder": "The Kitchen=Kitchen, Guest Room=Beds",
            "description": (
                "Map Home Assistant zone names to Tater room names when they differ, so the "
                "right room is used automatically. Comma-separated Zone=Room pairs."
            ),
        },
        {
            "key": "person_link_follow_me_takeover_mode",
            "label": "Follow-Me Room Takeover",
            "type": "select",
            "value": _text(link.get("follow_me_takeover_mode"))
            or _text(cfg.get("follow_me_takeover_mode") or DEFAULT_FOLLOW_ME_TAKEOVER_MODE),
            "options": [
                {"value": "auto", "label": "Auto take over"},
                {"value": "ask", "label": "Ask before taking over"},
            ],
            "description": (
                "When the room they walk into is already playing someone else's music: take it "
                "over automatically (the other queue pauses in place), or ask first."
            ),
        },
        {
            "key": "person_link_follow_me_away_action",
            "label": "Follow-Me Away Behavior",
            "type": "select",
            "value": _text(link.get("follow_me_away_action"))
            or _text(cfg.get("follow_me_away_action") or DEFAULT_FOLLOW_ME_AWAY_ACTION),
            "options": [
                {"value": "keep_pause", "label": "Keep in dead rooms, pause when away"},
                {"value": "pause", "label": "Pause whenever they leave a speaker room"},
                {"value": "keep", "label": "Never pause; only move into rooms"},
            ],
            "description": (
                "What happens to their music in a zone with no speakers, or when they're "
                "outside the home. Follow-Me pausing resumes automatically when they reappear "
                "in a room with speakers."
            ),
        },
        {
            "key": "person_link_follow_me_move_resume_delay_seconds",
            "label": "Their Move Resume Delay (seconds)",
            "type": "number",
            "value": _as_int(link.get("follow_me_move_resume_delay_seconds"), 0, 0, 600)
            if _text(link.get("follow_me_move_resume_delay_seconds")) != ""
            else "",
            "min": 0,
            "max": 600,
            "step": 1,
            "description": (
                "When Follow-Me hands their music to the room they just walked into, that room "
                "waits this many seconds before the music resumes at the same spot — time to "
                "walk between rooms. Leave blank to use the global setting; 0 resumes "
                "immediately."
            ),
        },
        {
            "key": "person_link_follow_me_resume_delay_seconds",
            "label": "Their Away Resume Delay (seconds)",
            "type": "number",
            "value": _as_int(link.get("follow_me_resume_delay_seconds"), 0, 0, 600)
            if _text(link.get("follow_me_resume_delay_seconds")) != ""
            else "",
            "min": 0,
            "max": 600,
            "step": 1,
            "description": (
                "When a follow-me pause resumes automatically because they reappear in a room "
                "with speakers, that room waits this many seconds before the music resumes at "
                "the same spot. Leave blank to use the global setting; 0 resumes immediately."
            ),
        },
    ]


def _follow_me_card_status(
    person_id: Any,
    link: Dict[str, Any],
    cfg: Dict[str, Any],
    store: Any,
) -> str:
    """Short follow-me status for a Person card subtitle ("" when not shown)."""
    if not _follow_me_follow_enabled(cfg):
        return ""
    if not _text(link.get("follow_me_person_entity")):
        return ""
    state = _follow_me_state(person_id, store)
    status = _text(state.get("status"))
    zone = _text(state.get("zone"))
    room = _text(state.get("resolved_room"))
    if status == "error":
        error = _text(state.get("last_error"))
        labels = {
            "ha_not_configured": "Home Assistant not configured in Tater",
            "entity_not_found": "person entity not found in Home Assistant",
            "ha_unauthorized": "Home Assistant token was rejected",
        }
        return f"Follow-me: {labels.get(error, error) or 'unreachable'}"
    if status == "awaiting_confirmation":
        return (
            "Follow-me: asking before taking over "
            f"{_target_summary(_list(state.get('resolved_targets')))}"
        )
    if status == "following" and room:
        return f"Follow-me: in {zone} → {room}"
    if status == "tracking":
        return f"Follow-me: spotted in {zone}, holding…"
    if status == "paused_away":
        return "Follow-me: paused (away from home)"
    if status == "paused_dead_zone":
        return f"Follow-me: paused (in {zone}, no speakers)"
    if status == "away_kept":
        return "Follow-me: away, music kept playing"
    if status in {"dead_zone", "dead_zone_idle", "away_idle"}:
        return f"Follow-me: in {zone}, no move needed"
    if not status:
        return "Follow-me: waiting for first check"
    return f"Follow-me: {status}"


def _apply_person_link_test_state(card: Dict[str, Any], state: Dict[str, Any]) -> None:
    """Show the last connection-test outcome on a link card as a summary row.

    The tab UI toasts the test's success message (Tater v1.2.0+), but the row
    keeps the outcome visible on the card after the toast is gone.
    """
    message = _text(state.get("message"))
    if message:
        passed = _text(state.get("status")) == "ok"
        card.setdefault("summary_rows", []).append(
            {"label": f"Last connection test: {'passed' if passed else 'failed'}", "value": message}
        )


def _library_summary_row(
    person_id: Any,
    *,
    label: str,
    not_synced_hint: str,
    store: Any = None,
) -> Dict[str, Any]:
    """One summary row describing a catalog's sync state (see CATALOG_STATS_KEY)."""
    return {
        "label": label,
        "value": _library_summary_value(person_id, not_synced_hint=not_synced_hint, store=store),
    }


def _library_summary_value(
    person_id: Any,
    *,
    not_synced_hint: str,
    store: Any = None,
) -> str:
    """Full-text sync state for one catalog.

    Rendered on the card's detail line, whose host CSS wraps — the
    summary-row boxes ellipsis-truncate long values (e.g. sync errors).
    """
    stats = _catalog_stats(person_id, store)
    status = _text(stats.get("status"))
    # A Person with a second linked source sums both libraries into one line.
    extra_source = _person_link_extra_source(_person_link(person_id, store))
    if extra_source and status == "ok":
        extra_stats = _catalog_stats(_person_extra_slot(person_id, extra_source), store)
        if _text(extra_stats.get("status")) == "ok":
            return (
                f"{_as_int(stats.get('track_count'), 0, 0, 10**9) + _as_int(extra_stats.get('track_count'), 0, 0, 10**9)} tracks"
                f" · {_as_int(stats.get('artist_count'), 0, 0, 10**9) + _as_int(extra_stats.get('artist_count'), 0, 0, 10**9)} artists"
                f" · {_as_int(stats.get('album_count'), 0, 0, 10**9) + _as_int(extra_stats.get('album_count'), 0, 0, 10**9)} albums"
                f" · {_as_int(stats.get('genre_count'), 0, 0, 10**9) + _as_int(extra_stats.get('genre_count'), 0, 0, 10**9)} genres"
                f" · scanned {_format_time(max(_as_float(stats.get('synced_at')), _as_float(extra_stats.get('synced_at'))))}"
            )
        if _text(extra_stats.get("status")) == "error":
            return (
                f"{_library_summary_value(person_id, not_synced_hint=not_synced_hint, store=store)}"
                f" · second source failed: {_text(extra_stats.get('error')) or 'unknown error'}"
            )
    if status == "syncing":
        return "Syncing…"
    if status == "error":
        return f"Last sync failed: {_text(stats.get('error')) or 'unknown error'}"
    if status != "ok":
        return not_synced_hint
    return (
        f"{_as_int(stats.get('track_count'), 0, 0, 10**9)} tracks"
        f" · {_as_int(stats.get('artist_count'), 0, 0, 10**9)} artists"
        f" · {_as_int(stats.get('album_count'), 0, 0, 10**9)} albums"
        f" · {_as_int(stats.get('genre_count'), 0, 0, 10**9)} genres"
        f" · scanned {_format_time(stats.get('synced_at'))}"
    )


def _person_link_personalization_fields(
    cfg: Dict[str, Any],
    link: Dict[str, Any],
    client: Any = None,
    person_id: Any = "",
) -> List[Dict[str, Any]]:
    """Per-Person Personalization overrides; blank choices inherit the global settings."""
    assistant_name = _assistant_first_name(client)
    playlist_payload = _endless_playlist_payload(client, _text(person_id))
    playlist_options = [
        {"value": "", "label": "Use the global Endless Playback playlist (or the newest mix)"}
    ]
    seen_playlist_names = set()
    for row in playlist_payload.get("playlists") or []:
        name = _text(row.get("name")) if isinstance(row, dict) else ""
        if name and name.casefold() not in seen_playlist_names:
            seen_playlist_names.add(name.casefold())
            playlist_options.append({"value": name, "label": name})
    # Playlists the user made themselves (Emby playlists, share .m3u files).
    for row in _catalog_user_playlists(client, _text(person_id)):
        name = _text(row.get("name"))
        if name and name.casefold() not in seen_playlist_names:
            seen_playlist_names.add(name.casefold())
            playlist_options.append({"value": name, "label": name})
    return [
        {
            "key": "person_link_recommendations_enabled",
            "label": "Their Recommendations",
            "type": "select",
            "value": _person_tri_state(link, "recommendations_enabled"),
            "options": [
                {"value": "", "label": "Use the global Personalization setting"},
                {"value": "on", "label": "On for this Person"},
                {"value": "off", "label": "Off for this Person"},
            ],
            "description": (
                f"Whether {assistant_name} builds AI-named mixes from this Person's "
                "listening history."
            ),
        },
        {
            "key": "person_link_recommendation_interval_hours",
            "label": "Their Mix Refresh (hours)",
            "type": "number",
            "value": _as_int(link.get("recommendation_interval_hours"), 0, 0, 168) or "",
            "min": 1,
            "max": 168,
            "step": 1,
            "description": (
                "Their mixes normally refresh with the global cadence; a higher value "
                "here keeps their mixes between shared refreshes. Leave blank for the "
                "global setting."
            ),
        },
        {
            "key": "person_link_recommendation_playlist_count",
            "label": "Their Recommendation Playlists",
            "type": "number",
            "value": _as_int(link.get("recommendation_playlist_count"), 0, 0, 6) or "",
            "min": 1,
            "max": 6,
            "step": 1,
            "description": "Leave blank to use the global Recommendation Playlists count.",
        },
        {
            "key": "person_link_recommendation_items_per_playlist",
            "label": "Their Albums & Songs Per Playlist",
            "type": "number",
            "value": _as_int(link.get("recommendation_items_per_playlist"), 0, 0, 12) or "",
            "min": 3,
            "max": 12,
            "step": 1,
            "description": (
                "Leave blank to use the global Albums & Songs Per Playlist count."
            ),
        },
        {
            "key": "person_link_prompt_context_enabled",
            "label": "Their Music Prompt Context",
            "type": "select",
            "value": _person_tri_state(link, "prompt_context_enabled"),
            "options": [
                {"value": "", "label": "Use the global Personalization setting"},
                {"value": "on", "label": "On for this Person"},
                {"value": "off", "label": "Off for this Person"},
            ],
            "description": (
                f"Whether {assistant_name} gets this Person's music profile (tastes, "
                "recent tracks) when they are speaking."
            ),
        },
        {
            "key": "person_link_endless_playback_mode",
            "label": "Their Endless Playback",
            "type": "select",
            "value": _person_select_choice(
                person_id,
                "endless_playback_mode",
                cfg,
                ENDLESS_PLAYBACK_MODES,
                "",
                client,
            )
            if _text(person_id)
            else "",
            "options": [
                {"value": "", "label": "Use the global Endless Playback setting"},
                {"value": "automatic", "label": "Automatic"},
                {"value": "llm_auto", "label": "Basic Auto (LLM)"},
                {"value": "library_mix", "label": "Infinite Mix from your library"},
                {"value": "similar_played", "label": "Similar to what you played"},
                {"value": "playlist_loop", "label": "Tracks from a playlist"},
            ],
            "description": (
                "How their queue keeps playing after its final track. Automatic tries a "
                "connected streaming provider, then an Infinite Mix from the library; Basic "
                "Auto (LLM) is the original AI-picked radio; Infinite Mix favours least-played "
                "tracks in the genres just heard; Similar to what you played relies on "
                "streaming providers; Tracks from a playlist loops one chosen mix."
            ),
        },
        {
            "key": "person_link_endless_playback_playlist",
            "label": "Their Endless Playback Playlist",
            "type": "select",
            "value": _text(link.get("endless_playback_playlist")),
            "options": playlist_options,
            "description": (
                "The playlist looped by their \"Tracks from a playlist\" Endless Playback mode "
                "(one of their AI-named mixes, or the household's)."
            ),
        },
        {
            "key": "person_link_folder_playlists",
            "label": "Their Folder Playlists",
            "type": "text",
            "value": _text(link.get("folder_playlists")),
            "placeholder": "Workout=Tunes/Workout, Jazz=Tunes/Jazz",
            "description": (
                "Name=Folder pairs (comma-separated) that turn folders in their library "
                "into always-up-to-date playlists just for them, matched against their own "
                "library on every play. Leave blank to use the global Folder Playlists "
                "setting; when filled, it replaces that list for this Person."
            ),
        },
        {
            "key": "person_link_smart_shuffle_enabled",
            "label": "Their Smart Shuffle",
            "type": "select",
            "value": _person_tri_state(link, "smart_shuffle_enabled"),
            "options": [
                {"value": "", "label": "Use the global Smart Shuffle setting"},
                {"value": "on", "label": "On for this Person"},
                {"value": "off", "label": "Off for this Person"},
            ],
            "description": (
                "Smart Shuffle pushes recently played tracks to the back of their shuffled "
                "queues and mixes several queued sources (albums, playlists, genres) on the "
                "fly instead of building one enormous queue up front."
            ),
        },
        {
            "key": "person_link_transfer_resume_delay_seconds",
            "label": "Their Transfer Resume Delay (seconds)",
            "type": "number",
            "value": _as_int(link.get("transfer_resume_delay_seconds"), 0, 0, 600)
            if _text(link.get("transfer_resume_delay_seconds")) != ""
            else "",
            "min": 0,
            "max": 600,
            "step": 1,
            "description": (
                "When their music is moved to new rooms (\"transfer/move my music to the "
                "Kitchen\", the control tool's move/set player actions, or changing the Play "
                "On destinations while playing), the gaining room waits this many seconds "
                "before the music resumes at the same spot. Leave blank to use the global "
                "setting; 0 resumes immediately."
            ),
        },
        {
            "key": "person_link_resume_room_mode",
            "label": "Resume in Another Room",
            "type": "select",
            "value": _text(link.get("resume_room_mode"))
            or _text(cfg.get("resume_room_mode") or DEFAULT_RESUME_ROOM_MODE),
            "options": [
                {"value": "stay", "label": "Stay where it was"},
                {"value": "follow", "label": "Follow me to this room"},
                {"value": "ask", "label": "Ask me each time"},
            ],
            "description": (
                "When they say \"resume my music\" from a room other than the one their paused "
                "music is in: Stay where it was resumes it in the original room; Follow me to "
                "this room moves it to the room they're speaking in (unless another queue is "
                "already playing there — then it stays put); Ask me each time asks over TTS."
            ),
        },
    ]


def _person_link_extra_source_fields(
    link: Dict[str, Any],
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Fields for a Person's optional second linked music source."""
    extra_source = _person_link_extra_source(link)
    values = link.get("extra") if isinstance(link.get("extra"), dict) else {}
    options: List[Dict[str, str]] = [
        {"value": "", "label": "None — one source is enough"}
    ]
    for provider_id in CATALOG_PROVIDER_ORDER:
        spec = PROVIDER_FIELD_SPECS[provider_id]
        options.extend(spec.source_options("extra"))
    fields: List[Dict[str, Any]] = [
        {
            "key": "person_link_extra_source",
            "label": "Second Music Source (optional)",
            "type": "select",
            "value": extra_source,
            "options": options,
            "description": (
                "Also play from a second library — their own Emby/Jellyfin account, a Subsonic or "
                "Plex server, or another share folder — merged into one catalog. It can even be a "
                "second account on the same server (fill in that account's fields below)."
            ),
        },
    ]
    for provider_id in CATALOG_PROVIDER_ORDER:
        if provider_id == "network_share":
            fields.extend(
                [
                    {
                        "key": "person_link_extra_share_root_path",
                        "label": "Second Source — Mounted Share Folder",
                        "type": "text",
                        "value": _text(values.get("root_path")),
                        "placeholder": "/mnt/music/<person>-more",
                    }
                ]
            )
            continue
        fields.extend(
            PROVIDER_FIELD_SPECS[provider_id].person_fields(
                values,
                prefix="person_link_extra",
                label_prefix="Second Source — ",
                with_descriptions=False,
            )
        )
        # The keep-blank hint still shows on the extra source's secrets.
        for field in fields:
            if field["type"] == "password" and "description" not in field:
                field["description"] = "Leave blank to keep the saved value."
    return fields


def _person_link_source_options(
    kind: str,
    *,
    allow_blank: bool = True,
) -> List[Dict[str, str]]:
    """Picker rows for a Person link's Music Source (or second source)."""
    options: List[Dict[str, str]] = []
    if allow_blank:
        options.append(
            {"value": "", "label": "Use the global music source"}
            if kind == "primary"
            else {"value": "", "label": "None — one source is enough"}
        )
    for provider_id in CATALOG_PROVIDER_ORDER:
        options.extend(PROVIDER_FIELD_SPECS[provider_id].source_options(kind))
    return options


def _person_link_source_fields(
    link: Dict[str, Any],
    cfg: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Every provider's fields for a Person link form.

    The form shows all providers' fields at once (the source select cannot
    re-render the card in the host UI), with each provider's stored values —
    so switching a link between sources never loses what was already saved.
    """
    fields: List[Dict[str, Any]] = []
    link_values = link if isinstance(link, dict) else {}
    for provider_id in CATALOG_PROVIDER_ORDER:
        values = (
            link_values.get(provider_id)
            if isinstance(link_values.get(provider_id), dict)
            else {}
        )
        # Pre-fill the Emby server URL with the household's, exactly as this
        # editor always did.
        if provider_id == "emby" and not values.get("server_url"):
            values = {
                **values,
                "server_url": _text(cfg.get("emby_server_url") or cfg.get("server_url")),
            }
        spec = PROVIDER_FIELD_SPECS[provider_id]
        fields.extend(spec.person_fields(values, prefix="person_link"))
        if provider_id == "plex":
            # The resolved Plex token is a link value, never a hand-typed form
            # field, so the token box stays blank (keep-blank convention).
            for field in fields:
                if field["key"] == spec.form_key("person_link", "token"):
                    field["value"] = ""
    return fields


def _person_link_items(cfg: Dict[str, Any], store: Any) -> List[Dict[str, Any]]:
    """Per-Person source links shown in the core tab's People section.

    Linked People render as compact cards (name, library sync state, and their
    actions); Edit opens the card's full settings form in the host's modal
    (ui.item_fields_popup, Tater v1.2.0+), whose Save/Cancel round-trip straight
    to music_person_link_save needs no core-side editor state.
    """
    items: List[Dict[str, Any]] = []
    person_options = _people_person_options(store)
    if len(person_options) <= 1:
        return items
    linked_ids = set(_person_links(store))
    for person_id, link in sorted(_person_links(store).items()):
        name = _people_person_name(person_id, store) or person_id
        source = _person_link_source(link)
        values = link.get(source) if isinstance(link.get(source), dict) else {}
        person_queue = _player(store, person_id)
        person_queue_status = _text(person_queue.get("status")).lower()
        person_queue_targets = _list(person_queue.get("targets") or person_queue.get("target"))
        queue_state = ""
        if person_queue_status in {"playing", "paused"} and person_queue_targets:
            queue_state = (
                f" · Now {person_queue_status}: {_track_label(person_queue.get('current') or {})} "
                f"on {_target_summary(person_queue_targets)}"
            )
        follow_me_status = _follow_me_card_status(person_id, link, cfg, store)
        sleep_state = _sleep_timer_state(person_queue)
        if sleep_state.get("active"):
            sleep_status = (
                f"Sleep timer: {max(1, int(_as_float(sleep_state.get('remaining_seconds')) / 60.0))} min left"
            )
        elif sleep_state.get("expired"):
            sleep_status = "Sleep timer stopped the music"
        else:
            sleep_status = ""
        library_hint = (
            "Their library has not been synced yet — press Edit, then Save Person Link to load it."
            if source
            else "The shared library has not been synced yet — press Rescan Library on the source card."
        )
        stats_status = _text(_catalog_stats(person_id, store).get("status"))
        card: Dict[str, Any] = {
            "id": f"person:{person_id}",
            "group": "people",
            "title": name,
            "subtitle": (
                (
                    f"Plays from {PROVIDER_LABELS[source]}"
                    + (
                        f" + {PROVIDER_LABELS[extra]}"
                        if (extra := _person_link_extra_source(link))
                        else ""
                    )
                )
                if source
                else "Uses the global music source"
            )
            + queue_state
            + (f" · {follow_me_status}" if follow_me_status else "")
            + (f" · {sleep_status}" if sleep_status else ""),
            # The full sync state (and failure reason) lives on the detail
            # line: the host renders it full width, unlike summary rows.
            "detail": _library_summary_value(person_id, not_synced_hint=library_hint, store=store),
            "hero_badges": [
                {
                    "label": PROVIDER_LABELS.get(source, "GLOBAL").upper(),
                    "tone": "good" if source else "muted",
                }
            ]
            + (
                [{"label": "SYNC FAILED", "tone": "danger"}]
                if stats_status == "error"
                else [{"label": "SYNCING", "tone": "muted"}] if stats_status == "syncing" else []
            ),
        }
        card["fields"] = [
                    {
                        "key": "person_link_person_id",
                        "type": "text",
                        "value": person_id,
                        "hidden": True,
                    },
                    {
                        "key": "person_link_source",
                        "label": "Music Source",
                        "type": "select",
                        "value": source,
                        "options": _person_link_source_options("primary"),
                    },
                    {
                        "key": "person_link_queue_conflict_mode",
                        "label": "Playback Conflicts",
                        "type": "select",
                        "value": _text(link.get("queue_conflict_mode"))
                        or _text(cfg.get("queue_conflict_mode") or DEFAULT_QUEUE_CONFLICT_MODE),
                        "options": [
                            {"value": "ask", "label": "Ask before taking over"},
                            {"value": "auto_move", "label": "Auto-move / take over"},
                        ],
                        "description": (
                            "When their music is playing elsewhere, or someone else's music is on "
                            "the rooms they ask for: ask first, or move/take over automatically."
                        ),
                    },
                    *_follow_me_link_fields(cfg, link),
                    *_person_link_personalization_fields(cfg, link, store, person_id),
                    *_person_link_extra_source_fields(link, cfg),
                    *_person_link_source_fields(link, cfg),
        ]
        card["save_action"] = "music_person_link_save"
        card["save_label"] = "Save Person Link"
        card["fields_popup"] = True
        card["settings_label"] = "Edit"
        card["settings_title"] = f"Edit {name}'s music link"
        card["actions"] = [
            {
                "action": "music_view_as_switch",
                "label": "View Their Music",
            },
            {
                "action": "music_person_link_test",
                "label": "Test Connection",
            },
            {
                "action": "music_person_sleep_start_30",
                "label": "Sleep 30m",
            },
            {
                "action": "music_person_sleep_start_60",
                "label": "Sleep 60m",
            },
            {
                "action": "music_person_sleep_cancel",
                "label": "Cancel Timer",
            },
            {
                "action": "music_person_link_remove",
                "label": "Remove Link",
                "tone": "danger",
                "confirm": f"Remove {name}'s personal music link? Their library and history stay until removed.",
            },
        ]
        items.append(card)
    unlinked = [
        option
        for option in person_options
        if option.get("value") and option["value"] not in linked_ids
    ]
    if unlinked:
        new_card: Dict[str, Any] = {
            "id": "person:new",
            "group": "people",
            "title": "Link a Person",
            "subtitle": "Give one Person their own music source, library, and listening history.",
            "detail": "Choose a Person, pick a source, and fill in that source's details.",
            "hero_badges": [{"label": "NEW LINK", "tone": "muted"}],
            "fields": [
                {
                    "key": "person_link_person_id",
                    "label": "Person",
                    "type": "select",
                    "value": "",
                    "options": unlinked,
                },
                {
                    "key": "person_link_source",
                    "label": "Music Source",
                    "type": "select",
                    "value": "emby",
                    "options": _person_link_source_options("primary", allow_blank=False),
                },
                {
                    "key": "person_link_queue_conflict_mode",
                    "label": "Playback Conflicts",
                    "type": "select",
                    "value": _text(cfg.get("queue_conflict_mode") or DEFAULT_QUEUE_CONFLICT_MODE),
                    "options": [
                        {"value": "ask", "label": "Ask before taking over"},
                        {"value": "auto_move", "label": "Auto-move / take over"},
                    ],
                    "description": (
                        "When their music is playing elsewhere, or someone else's music is on "
                        "the rooms they ask for: ask first, or move/take over automatically."
                    ),
                },
                *_follow_me_link_fields(cfg, {}),
                *_person_link_personalization_fields(cfg, {}, store),
                *_person_link_extra_source_fields({}, cfg),
                *_person_link_source_fields({}, cfg),
            ],
        }
        new_card["save_action"] = "music_person_link_save"
        new_card["save_label"] = "Link Person"
        new_card["fields_popup"] = True
        new_card["settings_label"] = "Add Person Link"
        new_card["settings_title"] = "Link a Person to their own music"
        new_card["actions"] = [
            {
                "action": "music_person_link_test",
                "label": "Test Connection",
            },
        ]
        items.append(new_card)
    # Show the last connection-test outcome on the card it was run against, so
    # it stays visible on the card after the success toast is gone.
    state = _person_link_test_state(store)
    if state.get("person_id"):
        unlinked_ids = {option.get("value") for option in unlinked}
        for card in items:
            card_id = _text(card.get("id"))
            if card_id == "person:new":
                # The add-link card only exists for still-unlinked people.
                if state["person_id"] not in unlinked_ids:
                    continue
                _apply_person_link_test_state(card, state)
            elif card_id == f"person:{state['person_id']}":
                _apply_person_link_test_state(card, state)
    return items


def get_htmlui_tab_data(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    assistant_possessive = _assistant_possessive(store)
    recommendations_label = _recommendations_label(store)
    cfg = _settings(store)
    prompt_person_id = _text(cfg.get("prompt_person_id"))
    people_options = _people_person_options(store)
    if prompt_person_id and not any(
        _text(option.get("value")) == prompt_person_id for option in people_options
    ):
        people_options.append({"value": prompt_person_id, "label": f"Saved Person: {prompt_person_id}"})
    active_provider = _provider_id(cfg.get("provider"))
    # When viewing a linked Person's music, their own source decides which
    # catalog, queue, and recommendation set the tabs show.
    viewer_person_id = _webui_viewer_person_id(store)
    if viewer_person_id:
        active_provider = _person_source_id(viewer_person_id, store) or active_provider
        viewer_name = _people_person_name(viewer_person_id, store) or viewer_person_id
    else:
        viewer_name = ""
    runtime = _runtime(store)
    catalog = _person_catalog(store, active_provider, viewer_person_id)
    player = _reconcile_native_playback(_player(store, viewer_person_id), store)
    if viewer_person_id:
        viewer_provider = _person_link_provider(viewer_person_id, active_provider, store)
        connected = bool(viewer_provider and viewer_provider.connected)
    else:
        connected = _paired(cfg, active_provider)
    saved_player_targets = _normalize_stereo_targets(
        player.get("targets") or player.get("target")
    )
    saved_default_targets = _normalize_stereo_targets(
        cfg.get("default_targets") or cfg.get("default_target")
    )
    saved_targets = _list([*saved_player_targets, *saved_default_targets])
    target_options = _target_options(
        current_values=saved_targets,
        provider_id=active_provider,
    )
    target_options, local_airplay_targets = _split_local_airplay_receiver_options(
        target_options,
        cfg,
    )
    saved_player_targets = [
        target
        for target in saved_player_targets
        if target.casefold() not in local_airplay_targets
    ]
    saved_default_targets = [
        target
        for target in saved_default_targets
        if target.casefold() not in local_airplay_targets
    ]
    saved_player_targets = _canonical_option_targets(saved_player_targets, target_options)
    saved_default_targets = _canonical_option_targets(saved_default_targets, target_options)
    saved_targets = _list([*saved_player_targets, *saved_default_targets])
    player = dict(player)
    player["targets"] = saved_player_targets
    known_targets = set(_target_alias_map(target_options))
    for saved in saved_targets:
        if saved and saved.casefold() not in known_targets:
            target_options.append({"value": saved, "label": f"Saved player: {saved}"})
            known_targets.add(saved.casefold())

    airplay_targets = _canonical_option_targets(
        _airplay_receiver_targets(cfg, player),
        target_options,
    )
    airplay_targets = [target for target in airplay_targets if _is_external_audio_target(target)]
    receiver_target_options = [
        row
        for row in target_options
        if _is_external_audio_option(row)
    ]
    settings_target_options = [_settings_target_option(row) for row in target_options]
    receiver_settings_target_options = [
        _settings_target_option(row) for row in receiver_target_options
    ]
    external_audio = _external_audio_status(cfg, player)
    airplay_enabled = _as_bool(cfg.get("airplay_receiver_enabled"), False)
    external_status = _text(external_audio.get("status") or "disabled").lower()
    external_error = _text(
        external_audio.get("route_error") or external_audio.get("receiver_error")
    )
    external_status_labels = {
        "disabled": "OFF",
        "starting": "STARTING",
        "ready": "READY",
        "buffering": "BUFFERING",
        "receiving": "RECEIVING",
        "routing": "CONNECTING SATS",
        "playing": "PLAYING",
        "waiting_for_targets": "CHOOSE SATS",
        "dependency_missing": "SHAIRPORT NEEDED",
        "runtime_unavailable": "TATER UPDATE NEEDED",
        "stopped": "STOPPED",
        "error": "ERROR",
    }
    external_status_label = external_status_labels.get(
        external_status,
        external_status.replace("_", " ").upper() or "UNKNOWN",
    )

    item_forms = [
        _player_item(player, target_options, active_provider, cfg),
        _search_item(catalog),
    ]
    item_forms.extend(
        _recommendation_ui_items(cfg, catalog, runtime, active_provider, store, viewer_person_id)
    )
    item_forms.extend(_view_as_items(cfg, store))
    item_forms.extend(_facet_items(catalog, "genres", "Genre"))
    item_forms.extend(_facet_items(catalog, "artists", "Artist"))
    item_forms.extend(_facet_items(catalog, "albums", "Album"))
    item_forms.extend(_provider_cards(cfg, active_provider, store))
    item_forms.extend(_person_link_items(cfg, store))
    item_forms.extend(
        [
            {
                "id": "settings:airplay_receiver",
                "group": "airplay",
                "card_variant": "airplay_receiver",
                "title": _text(cfg.get("airplay_receiver_name")) or "Tater Music",
                "subtitle": (
                    "Your single AirPlay doorway into synchronized Tater Native and AirPlay speakers."
                ),
                "detail": (
                    external_error
                    if external_error
                    else f"Incoming AirPlay audio is playing on {_target_summary(airplay_targets)}."
                    if external_status == "playing"
                    else "Visible in the AirPlay speaker picker and waiting for audio."
                    if external_status == "ready"
                    else "Enable the receiver and choose at least one Native or AirPlay destination."
                    if not airplay_enabled
                    else "Choose at least one Native satellite or stereo pair."
                    if not airplay_targets
                    else "The receiver uses the same Shairport Sync adapter on Docker/Linux and macOS."
                ),
                "hero_badges": [
                    {
                        "label": external_status_label if airplay_enabled else "OFF",
                        "tone": (
                            "good"
                            if external_status in {"ready", "receiving", "routing", "playing"}
                            else "warn"
                            if airplay_enabled
                            else "muted"
                        ),
                    },
                    {"label": "SHAIRPORT SYNC", "tone": "muted"},
                    {
                        "label": f"{len(airplay_targets)} DESTINATION{'' if len(airplay_targets) == 1 else 'S'}",
                        "tone": "muted",
                    },
                    *(
                        [{"label": "LIVE INPUT", "tone": "good"}]
                        if external_audio.get("input_active")
                        else []
                    ),
                ],
                "summary_rows": [
                    {
                        "label": "Receiver",
                        "value": _text(cfg.get("airplay_receiver_name")) or "Tater Music",
                    },
                    {
                        "label": "Destinations",
                        "value": _target_summary(airplay_targets) if airplay_targets else "Choose below",
                    },
                    {
                        "label": "Adapter",
                        "value": "Shairport Sync 5.2+ · classic AirPlay/RAOP receiver",
                    },
                ],
                # Keep these settings cards rendering inline: the ui-level
                # item_fields_popup/item_fields_dropdown defaults are for the
                # People tab's link modals only.
                "fields_popup": False,
                "fields_dropdown": False,
                "fields": [
                    {
                        "key": "airplay_receiver_enabled",
                        "label": "Make This Receiver Available",
                        "type": "checkbox",
                        "value": airplay_enabled,
                        "description": "Advertise this Tater server as an AirPlay audio destination.",
                    },
                    {
                        "key": "airplay_receiver_targets",
                        "label": "Play Incoming AirPlay On",
                        "type": "multiselect",
                        "presentation": "cards",
                        "full_width": True,
                        "value": airplay_targets,
                        "options": receiver_settings_target_options,
                        "description": (
                            "Choose one or more speakers for incoming AirPlay. Tater keeps the selected "
                            "destinations synchronized as one receiver."
                        ),
                    },
                    {
                        "key": "airplay_receiver_name",
                        "label": "Receiver Name",
                        "type": "text",
                        "value": _text(cfg.get("airplay_receiver_name")) or "Tater Music",
                        "placeholder": "Tater Music",
                    },
                    {
                        "key": "airplay_receiver_pin",
                        "label": "Pairing PIN (optional)",
                        "type": "password",
                        "value": _text(cfg.get("airplay_receiver_pin")),
                        "placeholder": "Four digits",
                        "description": "A fixed four-digit PIN is requested only when a device first pairs.",
                    },
                ],
                "actions": (
                    [
                        {
                            "action": "music_airplay_stop",
                            "label": "Stop AirPlay Input",
                            "tone": "danger",
                        }
                    ]
                    if external_audio.get("input_active")
                    else []
                ),
                "save_action": "music_save_settings",
                "save_label": "Save AirPlay",
            },
            {
                "id": "settings:music",
                "group": "settings",
                "card_variant": "settings_wide",
                "title": "Playback Defaults",
                "subtitle": "Where music starts and how broad requests behave.",
                "fields_popup": False,
                "fields_dropdown": False,
                "fields": [
                    {
                        "key": "default_targets",
                        "label": "Default Speakers",
                        "type": "multiselect",
                        "presentation": "cards",
                        "full_width": True,
                        "value": saved_default_targets,
                        "options": settings_target_options,
                        "description": (
                            "Used only when the request does not name rooms or players "
                            "and did not originate from a satellite."
                        ),
                    },
                    {
                        "key": "default_volume_percent",
                        "label": "Default Volume",
                        "type": "range",
                        "value": _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
                        "min": 0,
                        "max": 100,
                        "step": 1,
                        "suffix": "%",
                    },
                    {
                        "key": "default_shuffle",
                        "label": "Shuffle Broad Requests",
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("default_shuffle"), True),
                    },
                    {
                        "key": "maximum_queue_tracks",
                        "label": "Maximum Queue Tracks",
                        "type": "number",
                        "value": _as_int(cfg.get("maximum_queue_tracks"), 200, 1, 1000),
                    },
                ],
                "save_action": "music_save_settings",
                "save_label": "Save Playback Defaults",
            },
            {
                "id": "settings:library_sync",
                "group": "settings",
                "title": "Library & Sync",
                "subtitle": "Background refresh timing and the starting point for mixed speaker groups.",
                "fields_popup": False,
                "fields_dropdown": False,
                "fields": [
                    {
                        "key": "catalog_sync_interval_seconds",
                        "label": "Catalog Refresh (seconds)",
                        "type": "number",
                        "value": _as_int(cfg.get("catalog_sync_interval_seconds"), 900, 60, 86400),
                        "min": 60,
                        "max": 86400,
                        "step": 60,
                    },
                    {
                        "key": "mixed_sync_default_adjustment_ms",
                        "label": "Mixed Group Starting Offset (ms)",
                        "type": "number",
                        "value": _as_int(cfg.get("mixed_sync_default_adjustment_ms"), 0, -750, 3000),
                        "min": -750,
                        "max": 3000,
                        "step": 25,
                        "description": (
                            "Starting adjustment for new Sonos + Tater Sat groups. Fine-tune each player "
                            "from the Players popup."
                        ),
                    },
                ],
                "save_action": "music_save_settings",
                "save_label": "Save Library & Sync",
            },
            {
                "id": "settings:personalization",
                "group": "settings",
                "title": "Personalization",
                "subtitle": f"Control {assistant_possessive} recommendations and one Person's listening profile.",
                "fields_popup": False,
                "fields_dropdown": False,
                "fields": [
                    {
                        "key": "recommendations_enabled",
                        "label": recommendations_label,
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("recommendations_enabled"), True),
                        "description": (
                            f"Uses listening metadata and {assistant_possessive} primary AI model to make named playlists."
                        ),
                    },
                    {
                        "key": "recommendation_interval_hours",
                        "label": "Recommendation Refresh (hours)",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168),
                    },
                    {
                        "key": "recommendation_playlist_count",
                        "label": "Recommendation Playlists",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_playlist_count"), 3, 1, 6),
                    },
                    {
                        "key": "recommendation_items_per_playlist",
                        "label": "Albums & Songs Per Playlist",
                        "type": "number",
                        "value": _as_int(cfg.get("recommendation_items_per_playlist"), 6, 3, 12),
                    },
                    {
                        "key": "prompt_context_enabled",
                        "label": "Music Prompt Context",
                        "type": "checkbox",
                        "value": _as_bool(cfg.get("prompt_context_enabled"), True),
                        "description": (
                            "Gives Tater a small music profile only when the selected Person is speaking."
                        ),
                    },
                    {
                        "key": "prompt_person_id",
                        "label": "Music Profile Person",
                        "type": "select",
                        "value": prompt_person_id,
                        "options": people_options,
                        "description": (
                            "Links listening history, favorite genres and artists, recent tracks, and prompt context "
                            "to one Person from Tater's People settings."
                        ),
                    },
                    {
                        "key": "prompt_profile_interval_hours",
                        "label": "Music Profile Refresh (hours)",
                        "type": "number",
                        "value": _as_int(cfg.get("prompt_profile_interval_hours"), 12, 1, 168),
                    },
                ],
                "save_action": "music_save_settings",
                "save_label": "Save Personalization",
            },
        ]
    )
    return {
        "summary": "Personal Emby and network-share music with voice control, per-person recommendations, and multi-room playback.",
        "stats": [
            *(
                [{"label": "Viewing", "value": f"{viewer_name}'s music"}]
                if viewer_person_id
                else []
            ),
            {
                "label": "Music Source",
                "value": (
                    PROVIDER_LABELS[active_provider]
                    if connected
                    else f"{PROVIDER_LABELS[active_provider]} · Setup Needed"
                ),
            },
            {"label": "Tracks", "value": len(catalog.get("tracks") or [])},
            {"label": "Artists", "value": len(catalog.get("artists") or [])},
            {"label": "Albums", "value": len(catalog.get("albums") or [])},
            {"label": "Genres", "value": len(catalog.get("genres") or [])},
            {
                "label": "Last Scan",
                "value": _format_time(
                    catalog.get("synced_at") or runtime.get("last_sync_at")
                ),
            },
            {
                "label": "AirPlay Receiver",
                "value": external_status_label if airplay_enabled else "Off",
            },
        ],
        "items": [],
        "empty_message": "Connect a music source (Emby or a mounted network share) to load your library.",
        "ui": {
            "kind": "settings_manager",
            "title": "Personal Music Core",
            "appearance": "music_library",
            "live_updates": True,
            "poll_interval_ms": 3000,
            "persistent_item_groups": ["player"],
            "default_tab": "playlist",
            "manager_tabs": [
                {
                    "key": "playlist",
                    "label": "Playlist",
                    "source": "player_queue",
                    "empty_message": "Play something from the library to start a playlist.",
                },
                {
                    "key": "library",
                    "label": "Browse Library",
                    "source": "grouped_items",
                    "groups": [
                        {
                            "key": "search",
                            "label": "Search",
                            "item_group": "search",
                            "selector": False,
                            "empty_message": "Search is unavailable.",
                        },
                        {
                            "key": "genres",
                            "label": "Genres",
                            "item_group": "genres",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No genres are available in the active library.",
                        },
                        {
                            "key": "artists",
                            "label": "Artists",
                            "item_group": "artists",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No artists are available in the active library.",
                        },
                        {
                            "key": "albums",
                            "label": "Albums",
                            "item_group": "albums",
                            "selector": False,
                            "page_size": 36,
                            "empty_message": "No albums are available in the active library.",
                        },
                    ],
                },
                {
                    "key": "recommendations",
                    "label": recommendations_label,
                    "source": "items",
                    "item_group": "recommendations",
                    "empty_message": f"Play some music to help {assistant_name} build recommendations.",
                },
                {"key": "providers", "label": "Sources", "source": "items", "item_group": "providers"},
                {
                    "key": "people",
                    "label": "People",
                    "source": "items",
                    "item_group": "people",
                    "empty_message": "Link a Person to give them their own music source and history.",
                },
                {
                    "key": "airplay",
                    "label": "AirPlay",
                    "source": "items",
                    "item_group": "airplay",
                    "empty_message": "AirPlay Receiver is unavailable in this Tater build.",
                },
                {"key": "settings", "label": "Settings", "source": "items", "item_group": "settings"},
            ],
            "item_fields_dropdown": True,
            "item_fields_popup": True,
            "item_forms": item_forms,
        },
    }


def _payload_values(payload: Dict[str, Any]) -> Dict[str, Any]:
    values = payload.get("values") if isinstance(payload, dict) else {}
    return values if isinstance(values, dict) else {}


def _provider_from_card(payload: Dict[str, Any], fallback: Any = "") -> str:
    item_id = _text(payload.get("id"))
    candidate = _provider_id(
        _text(payload.get("provider") or fallback) or item_id.replace("provider:", ""),
        "",
    )
    if candidate not in PROVIDER_LABELS:
        raise ValueError("Unknown music source.")
    return candidate


def _save_person_link_action(values: Dict[str, Any], store: Any) -> Dict[str, Any]:
    person_id = _text(values.get("person_link_person_id"))
    if not person_id:
        raise ValueError("Choose which Person this music source belongs to.")
    name = _people_person_name(person_id, store)
    if not name:
        raise ValueError("That Person no longer exists. Re-check Tater's People settings.")
    source = _person_link_source({"music_source": values.get("person_link_source")})
    existing = _person_link(person_id, store)
    existing_values = (
        existing.get(source) if isinstance(existing.get(source), dict) else {}
    ) if source else {}
    link: Dict[str, Any] = {"music_source": source}
    conflict_mode = _text(values.get("person_link_queue_conflict_mode")).casefold()
    if conflict_mode in QUEUE_CONFLICT_MODES:
        link["queue_conflict_mode"] = conflict_mode
    # Personalization overrides: "" (or blank) means inherit the global
    # setting, so the key is simply left out of the rebuilt link.
    recommendations_state = _text(values.get("person_link_recommendations_enabled")).casefold()
    if recommendations_state in {"on", "off"}:
        link["recommendations_enabled"] = recommendations_state
    prompt_state = _text(values.get("person_link_prompt_context_enabled")).casefold()
    if prompt_state in {"on", "off"}:
        link["prompt_context_enabled"] = prompt_state
    smart_shuffle_state = _text(values.get("person_link_smart_shuffle_enabled")).casefold()
    if smart_shuffle_state in {"on", "off"}:
        link["smart_shuffle_enabled"] = smart_shuffle_state
    # Selects inherit the stored value when the payload carries an invalid one,
    # so a stale form can never wipe a saved mode.
    endless_mode = _text(values.get("person_link_endless_playback_mode")).casefold()
    if endless_mode not in ENDLESS_PLAYBACK_MODES:
        endless_mode = _text(existing.get("endless_playback_mode"))
    if endless_mode in ENDLESS_PLAYBACK_MODES:
        link["endless_playback_mode"] = endless_mode
    if "person_link_endless_playback_playlist" in values:
        link["endless_playback_playlist"] = _text(
            values.get("person_link_endless_playback_playlist")
        ).strip()
    # Blank means inherit the global Folder Playlists setting, so the key is
    # left out of the rebuilt link, like the other personalization overrides.
    person_folder_playlists = _text(values.get("person_link_folder_playlists")).strip()
    if person_folder_playlists:
        link["folder_playlists"] = person_folder_playlists
    for field_key, link_key, override_max in (
        ("person_link_recommendation_interval_hours", "recommendation_interval_hours", 168),
        ("person_link_recommendation_playlist_count", "recommendation_playlist_count", 6),
        (
            "person_link_recommendation_items_per_playlist",
            "recommendation_items_per_playlist",
            12,
        ),
    ):
        raw = _text(values.get(field_key))
        if not raw:
            continue
        try:
            parsed = int(float(raw))
        except Exception:
            continue
        if parsed > 0:
            link[link_key] = parsed if parsed <= override_max else override_max
    # Resume-delay overrides: unlike the counts above, an explicit 0 is a real
    # choice ("resume immediately even though the global default waits"), so
    # only blank means inherit the global setting.
    for field_key, link_key in (
        ("person_link_transfer_resume_delay_seconds", "transfer_resume_delay_seconds"),
        (
            "person_link_follow_me_move_resume_delay_seconds",
            "follow_me_move_resume_delay_seconds",
        ),
        ("person_link_follow_me_resume_delay_seconds", "follow_me_resume_delay_seconds"),
    ):
        if field_key not in values:
            continue
        raw = _text(values.get(field_key))
        if raw == "":
            continue
        try:
            parsed = int(float(raw))
        except Exception:
            continue
        link[link_key] = min(600, max(0, parsed))
    if "person_link_follow_me_entity" in values:
        link["follow_me_person_entity"] = _text(values.get("person_link_follow_me_entity")).strip()
    if "person_link_follow_me_room_overrides" in values:
        link["follow_me_room_overrides"] = _text(
            values.get("person_link_follow_me_room_overrides")
        ).strip()
    # Selects inherit the stored value when the payload carries an invalid one,
    # so a stale form can never wipe a saved mode.
    follow_takeover = _text(values.get("person_link_follow_me_takeover_mode")).casefold()
    if follow_takeover not in FOLLOW_ME_TAKEOVER_MODES:
        follow_takeover = _text(existing.get("follow_me_takeover_mode")).casefold()
    if follow_takeover in FOLLOW_ME_TAKEOVER_MODES:
        link["follow_me_takeover_mode"] = follow_takeover
    follow_away = _text(values.get("person_link_follow_me_away_action")).casefold()
    if follow_away not in FOLLOW_ME_AWAY_ACTIONS:
        follow_away = _text(existing.get("follow_me_away_action")).casefold()
    if follow_away in FOLLOW_ME_AWAY_ACTIONS:
        link["follow_me_away_action"] = follow_away
    resume_room = _text(values.get("person_link_resume_room_mode")).casefold()
    if resume_room not in RESUME_ROOM_MODES:
        resume_room = _text(existing.get("resume_room_mode")).casefold()
    if resume_room in RESUME_ROOM_MODES:
        link["resume_room_mode"] = resume_room
    if source:
        spec = PROVIDER_FIELD_SPECS[source]
        built = spec.build_values("person_link", values, existing_values)
        if source == "plex" and not _text(built.get("token")):
            # Resolve (and cache) the Plex token now so the link holds a
            # complete, immediately playable state; a plex.tv failure surfaces
            # through the sync error below rather than blocking the save.
            try:
                built = _plex_resolve_link_values(built, existing_values, store=store)
            except Exception as exc:
                logger.warning("[Music] Plex link token resolution failed: %s", exc)
        elif source in ("emby", "jellyfin") and built.get("auth_mode") == "user_token" and not _text(
            built.get("user_id")
        ):
            # Store the user id defensively so identity-based caches and
            # scopes stay stable even if a later save carries a stale form.
            try:
                provider = spec.build_provider(built)
                _token, user_id = provider.authenticate(client=store)
                built["user_id"] = user_id
            except Exception as exc:
                logger.warning("[Music] %s link user-id resolution failed: %s", source, exc)
        link[source] = built
    # Second linked source (optional): another provider account or share folder
    # on top of the primary, so one Person can listen across both.
    extra_source = _provider_id(values.get("person_link_extra_source"), "")
    if extra_source:
        existing_extra = (
            existing.get("extra") if isinstance(existing.get("extra"), dict) else {}
        )
        # Only keep secrets from a stored second source of the same kind.
        existing_extra = existing_extra if _person_link_extra_source(existing) == extra_source else {}
        built_extra = PROVIDER_FIELD_SPECS[extra_source].build_values(
            "person_link_extra", values, existing_extra
        )
        if extra_source == "plex" and not _text(built_extra.get("token")):
            try:
                built_extra = _plex_resolve_link_values(built_extra, existing_extra, store=store)
            except Exception as exc:
                logger.warning("[Music] Plex extra-source token resolution failed: %s", exc)
        elif extra_source in ("emby", "jellyfin") and built_extra.get(
            "auth_mode"
        ) == "user_token" and not _text(built_extra.get("user_id")):
            try:
                provider = PROVIDER_FIELD_SPECS[extra_source].build_provider(built_extra)
                _token, user_id = provider.authenticate(client=store)
                built_extra["user_id"] = user_id
            except Exception as exc:
                logger.warning("[Music] %s extra-source user-id resolution failed: %s", extra_source, exc)
        link["extra"] = built_extra
        link["extra_source"] = extra_source
    else:
        link.pop("extra_source", None)
        link.pop("extra", None)
    _save_person_link(person_id, link, store)
    # The link now holds the real values; drop the test draft and its result.
    _clear_person_link_test_state(person_id, store)
    sync_note = ""
    if source:
        try:
            catalog = _sync_catalog(store, source, person_id)
            sync_note = f" Loaded {len(catalog.get('tracks') or [])} of {name}'s tracks."
        except Exception as exc:
            sync_note = f" Their library did not load yet: {_text(exc)}"
            # The tab UI drops success-path messages, so record the failure for
            # the link card's "Their Library" row.
            _record_catalog_stats(
                store,
                person_id,
                {"status": "error", "error": _text(exc)[:200], "failed_at": time.time()},
            )
    if link.get("extra_source"):
        extra_slot = _person_extra_slot(person_id, link["extra_source"])
        try:
            extra_catalog = _sync_catalog(store, link["extra_source"], extra_slot)
            sync_note += (
                f" Plus {len(extra_catalog.get('tracks') or [])} tracks from their second source."
            )
        except Exception as exc:
            sync_note += f" Their second source did not load yet: {_text(exc)}"
            _record_catalog_stats(
                store,
                extra_slot,
                {"status": "error", "error": _text(exc)[:200], "failed_at": time.time()},
            )
    return {"ok": True, "message": f"Saved {name}'s music link.{sync_note}"}


def _test_person_link_source_action(values: Dict[str, Any], store: Any) -> Dict[str, Any]:
    """Test one Person's link credentials from the form without saving them.

    Tests whatever music source the form currently selects; the outcome is
    recorded so its summary row stays on the link card after the tab's success
    toast is gone (see _person_link_items).
    """
    person_id = _text(values.get("person_link_person_id"))
    name = _people_person_name(person_id, store) if person_id else ""
    label = name or "This person"
    source = _person_link_source({"music_source": values.get("person_link_source")}) or "emby"
    spec = PROVIDER_FIELD_SPECS.get(source)
    if spec is None:
        raise ValueError("Choose a music source to test first.")
    existing = _person_link(person_id, store).get(source) if person_id else {}
    if not isinstance(existing, dict):
        existing = {}
    draft = {key: _text(values.get(key)) for key in PERSON_LINK_TEST_FIELD_KEYS}

    def _record(status: str, message: str) -> None:
        _save_person_link_test_state(person_id, draft, status, message, store)

    try:
        message = f"{label}'s connection works — {spec.test_link_form(values, existing, store)}."
    except ValueError as exc:
        _record("error", _text(exc))
        raise
    except Exception as exc:
        message = f"Could not reach {PROVIDER_LABELS[source]} for {label}: {_text(exc)}"
        _record("error", message)
        raise ValueError(message) from exc
    _record("ok", message)
    return {"ok": True, "message": message}


def _connect_provider(
    provider_id: str,
    values: Dict[str, Any],
    client: Any,
) -> Dict[str, Any]:
    provider_id = _provider_id(provider_id, "")
    spec = PROVIDER_FIELD_SPECS.get(provider_id)
    if spec is None:
        raise ValueError(f"{PROVIDER_LABELS.get(provider_id, provider_id)} support is not enabled in this build.")
    return spec.connect(values, _settings(client), client)


def _disconnect_provider(provider_id: str, client: Any) -> Dict[str, Any]:
    provider_id = _provider_id(provider_id, "")
    spec = PROVIDER_FIELD_SPECS.get(provider_id)
    if spec is None:
        raise ValueError(f"{PROVIDER_LABELS.get(provider_id, provider_id)} support is not enabled in this build.")
    disconnect = getattr(spec, "disconnect", None)
    if not callable(disconnect):
        return _disconnect_generic(provider_id, client)
    return disconnect(client)


def _webui_viewer_person_id(store: Any = None) -> str:
    """Person whose music the dashboard tab is viewing ("" = household/global)."""
    store = store or globals().get("redis_client")
    person_id = _text(_settings(store).get("webui_view_as_person"))
    # Only linked Persons have their own library to view.
    if person_id and person_id in _person_links(store):
        return person_id
    return ""


def _view_as_items(cfg: Dict[str, Any], store: Any) -> List[Dict[str, Any]]:
    """'View Music As' cards shown atop the Library, Recommendations, and People tabs."""
    viewer = _webui_viewer_person_id(store)
    options = [{"value": "", "label": "Household (global source)"}]
    for person_id in sorted(_person_links(store)):
        options.append(
            {
                "value": person_id,
                "label": _people_person_name(person_id, store) or person_id,
            }
        )
    if len(options) <= 1:
        return []
    viewer_name = _people_person_name(viewer, store) or viewer if viewer else ""
    cards = []
    for group in ("search", "recommendations", "people"):
        cards.append(
            {
                "id": f"view_as:{group}",
                "group": group,
                "title": "View Music As",
                "subtitle": (
                    f"Viewing {viewer_name}'s music — their library, queue, and mixes."
                    if viewer
                    else "Pick a linked Person to browse their library, queue, and mixes."
                ),
                "detail": (
                    "Switches what the Playlist, Browse Library, and Recommendations tabs show. "
                    "Voice requests always follow the speaking Person."
                ),
                "hero_badges": (
                    [{"label": f"VIEWING: {viewer_name.upper()}", "tone": "good"}]
                    if viewer
                    else [{"label": "HOUSEHOLD", "tone": "muted"}]
                ),
                "fields_popup": False,
                "fields_dropdown": False,
                "fields": [
                    {
                        "key": "view_as_person_id",
                        "label": "View Music As",
                        "type": "select",
                        "value": viewer,
                        "options": options,
                    }
                ],
                "save_action": "music_view_as_switch",
                "save_label": "Switch Viewer",
                "actions": (
                    [{"action": "music_view_as_exit", "label": "Back to Household"}]
                    if viewer
                    else []
                ),
            }
        )
    return cards


def _play_recommendation(
    item_id: Any,
    client: Any = None,
    *,
    requested_targets: Any = None,
    volume_percent: Any = None,
    person_id: Any = "",
) -> Dict[str, Any]:
    store = client or globals().get("redis_client")
    recommendations_label = _recommendations_label(store)
    recommendation_id = _text(item_id)
    if recommendation_id.startswith("recommendation:"):
        recommendation_id = recommendation_id.split(":", 1)[1]
    published = _recommendations(store, person_id)
    cfg = _settings(store)
    # A viewed Person's mixes come from their own source, not the global one.
    provider_id = (
        _person_source_id(person_id, store) or _provider_id(cfg.get("provider"))
    )
    if _provider_id(published.get("provider"), "") != provider_id:
        raise ValueError(f"Refresh {recommendations_label} for the active music provider first.")
    playlist = next(
        (
            row
            for row in published.get("playlists") or []
            if isinstance(row, dict) and _text(row.get("id")) == recommendation_id
        ),
        None,
    )
    if not isinstance(playlist, dict):
        raise ValueError("That Tater recommendation is no longer available.")
    catalog = _person_catalog(store, provider_id, person_id)
    track_by_id = {
        _text(track.get("id")): track
        for track in catalog.get("tracks") or []
        if isinstance(track, dict) and _text(track.get("id"))
    }
    tracks = [
        dict(track_by_id[track_id])
        for track_id in playlist.get("track_ids") or []
        if _text(track_id) in track_by_id
    ]
    if not tracks:
        raise ValueError("Those recommended tracks are no longer in the active library. Refresh recommendations.")
    # The Playlist Order setting decides whether a mix plays shuffled or in a
    # fixed order (track number, title, artist, album).
    tracks = _order_playlist_tracks(tracks, _playlist_order_value(cfg))

    current = _player(store, person_id)
    selected_targets = _list(requested_targets) or _list(
        current.get("targets") or current.get("target")
    ) or _list(cfg.get("default_targets") or cfg.get("default_target"))
    targets = _resolve_targets(
        selected_targets,
        client=store,
        provider_id=provider_id,
        person_id=person_id,
    )
    if not targets:
        raise ValueError("Choose one or more players in the Music Player before starting this playlist.")
    _validate_catalog_provider_targets(targets)
    volume = _as_int(
        current.get("volume_percent") if volume_percent is None else volume_percent,
        _as_int(cfg.get("default_volume_percent"), 75, 0, 100),
        0,
        100,
    )
    return _create_and_start_queue(
        tracks,
        targets=targets,
        shuffle=False,
        volume_percent=volume,
        person_id=person_id,
        client=store,
    )


def handle_htmlui_tab_action(
    *,
    action: str,
    payload: Dict[str, Any],
    redis_client=None,
    **_kwargs,
) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    recommendations_label = _recommendations_label(store)
    action_name = _text(action).lower()
    body = payload if isinstance(payload, dict) else {}
    values = _payload_values(body)
    # Dashboard actions follow the "View Music As" Person ("" = household).
    viewer_person_id = _webui_viewer_person_id(store)
    viewer_origin = {"person_id": viewer_person_id} if viewer_person_id else {}
    # The viewed Person's own source is the active provider for their actions.
    viewer_provider_id = (
        _person_source_id(viewer_person_id, store) if viewer_person_id else ""
    )

    def _action_provider(default: Any) -> str:
        return viewer_provider_id or _provider_id(default)

    if action_name == "music_view_as_switch":
        person_id = _text(values.get("view_as_person_id")) or _text(
            values.get("person_link_person_id")
        )
        if person_id and person_id not in _person_links(store):
            raise ValueError(
                "Choose a Person that has a music link (People tab) to view their music."
            )
        if person_id:
            name = _people_person_name(person_id, store) or person_id
            message = f"Now viewing {name}'s music in this tab."
            # Switching alone never loads their library; if their catalog was
            # never synced (or their source changed), start one so the Browse
            # Library tabs fill in without re-saving the link.
            if not (
                _person_catalog(store, _person_source_id(person_id, store), person_id).get("tracks")
                or []
            ):
                message += (
                    " Their library is syncing now; Browse Library fills in when it finishes."
                    if _schedule_catalog_sync(person_id, store)
                    else " Their library sync is already running."
                )
        else:
            message = "Now viewing the household's shared music."
        _save_hash(store, SETTINGS_KEY, {"webui_view_as_person": person_id})
        return {"ok": True, "message": message}

    if action_name == "music_view_as_exit":
        _save_hash(store, SETTINGS_KEY, {"webui_view_as_person": ""})
        return {"ok": True, "message": "Back to the household's shared music view."}

    if action_name == "music_provider_connect":
        return _connect_provider(_provider_from_card(body), values, store)

    if action_name == "music_provider_activate":
        provider_id = _provider_from_card(body)
        if not _paired(_settings(store), provider_id):
            raise ValueError(f"Connect {PROVIDER_LABELS[provider_id]} first.")
        catalog = _sync_catalog(store, provider_id)
        return {
            "ok": True,
            "message": (
                f"{PROVIDER_LABELS[provider_id]} loaded "
                f"{len(catalog.get('tracks') or [])} tracks."
            ),
        }

    if action_name in {"music_disconnect", "music_provider_disconnect"}:
        return _disconnect_provider(_provider_from_card(body), store)

    if action_name == "music_person_link_save":
        return _save_person_link_action(values, store)

    if action_name in {
        "music_person_sleep_start_30",
        "music_person_sleep_start_60",
        "music_person_sleep_cancel",
    }:
        person_id = _text(values.get("person_link_person_id")) or _text(body.get("id")).replace(
            "person:", ""
        )
        name = _people_person_name(person_id, store) or person_id
        minutes = {
            "music_person_sleep_start_30": 30,
            "music_person_sleep_start_60": 60,
        }.get(action_name, 0)
        _set_sleep_timer(minutes, person_id=person_id, client=store)
        if minutes:
            return {
                "ok": True,
                "message": f"Sleep timer set for {name}: their music stops in {minutes} minutes.",
            }
        return {"ok": True, "message": f"Cancelled {name}'s sleep timer."}

    if action_name == "music_person_link_test":
        return _test_person_link_source_action(values, store)

    if action_name == "music_person_link_remove":
        person_id = _text(values.get("person_link_person_id"))
        if not person_id:
            person_id = _text(body.get("id")).replace("person:", "")
        name = _people_person_name(person_id, store) or person_id
        _delete_person_link(person_id, store)
        _clear_person_link_test_state(person_id, store)
        if _text(_settings(store).get("webui_view_as_person")) == person_id:
            _save_hash(store, SETTINGS_KEY, {"webui_view_as_person": ""})
        removed_link = _person_link(person_id, store)
        extra_clear_keys = [
            _catalog_key(_person_extra_slot(person_id, source))
            for source in _person_link_sources(removed_link)[1:]
        ]
        for clear_key in (
            _catalog_key(person_id),
            *extra_clear_keys,
            _history_key(person_id),
            _recommendations_key(person_id),
            _profile_key(person_id),
            # Also drop the Person's own queue, follow-me tracking state, and
            # sync-stats entry, so re-linking them later starts clean.
            _player_key(person_id),
            _follow_me_state_key(person_id),
        ):
            try:
                store.delete(clear_key)
            except Exception:
                pass
        # Their queue slot leaves the background loop's registry, and their
        # catalog-stats field stops describing a card that no longer exists.
        try:
            registry = _queue_registry(store)
            registry.pop(_text(person_id), None)
            _save_json(store, QUEUE_REGISTRY_KEY, registry)
        except Exception:
            pass
        try:
            store.hdel(CATALOG_STATS_KEY, _catalog_stats_field(person_id))
        except Exception:
            pass
        return {"ok": True, "message": f"Removed {name}'s personal music link."}

    if action_name == "music_sync_now":
        selected_provider = _provider_id(_settings(store).get("provider"))
        catalog = _sync_catalog(store, selected_provider)
        return {"ok": True, "message": f"Music library updated with {len(catalog.get('tracks') or [])} tracks."}

    if action_name == "music_save_settings":
        current_settings = _settings(store)
        allowed = {
            "catalog_sync_interval_seconds",
            "default_targets",
            "default_volume_percent",
            "mixed_sync_default_adjustment_ms",
            "default_shuffle",
            "maximum_queue_tracks",
            "airplay_receiver_enabled",
            "airplay_receiver_name",
            "airplay_receiver_pin",
            "airplay_receiver_targets",
            "recommendations_enabled",
            "recommendation_interval_hours",
            "recommendation_playlist_count",
            "recommendation_items_per_playlist",
            "prompt_context_enabled",
            "prompt_person_id",
            "prompt_profile_interval_hours",
            "queue_conflict_mode",
        }
        updates = {key: values.get(key) for key in allowed if key in values}
        if "default_targets" in updates:
            updates["default_targets"] = json.dumps(
                _normalize_stereo_targets(updates["default_targets"])
            )
        if "airplay_receiver_targets" in updates:
            targets = _normalize_stereo_targets(updates["airplay_receiver_targets"])
            unsupported = [target for target in targets if not _is_external_audio_target(target)]
            if unsupported:
                raise ValueError(
                    "AirPlay Receiver destinations must be Tater Native satellites, stereo pairs, "
                    "AirPlay-capable Sonos players, or AirPlay speakers."
                )
            unavailable_sonos = [
                target
                for target in targets
                if _is_sonos_target(target) and not _sonos_airplay_target(target)
            ]
            if unavailable_sonos:
                raise ValueError(
                    "Each Sonos receiver destination needs a currently discovered matching AirPlay endpoint: "
                    + ", ".join(unavailable_sonos)
                )
            updates["airplay_receiver_targets"] = json.dumps(targets)
        if "airplay_receiver_name" in updates:
            updates["airplay_receiver_name"] = (
                _text(updates.get("airplay_receiver_name"))[:80] or "Tater Music"
            )
        if "airplay_receiver_pin" in updates:
            raw_pin = _text(updates.get("airplay_receiver_pin"))
            pin = "".join(char for char in raw_pin if char.isdigit())
            if raw_pin and (len(pin) != 4 or pin != raw_pin):
                raise ValueError("The AirPlay pairing PIN must be exactly four digits, or left blank.")
            updates["airplay_receiver_pin"] = pin
        if "prompt_person_id" in updates:
            updates["prompt_person_id"] = _text(updates.get("prompt_person_id"))
            if updates["prompt_person_id"] and not _people_person_name(
                updates["prompt_person_id"], store
            ):
                raise ValueError("Choose an existing Person for Music Prompt Context.")
        if "queue_conflict_mode" in updates:
            mode = _text(updates.get("queue_conflict_mode")).casefold()
            if mode and mode not in QUEUE_CONFLICT_MODES:
                raise ValueError("Playback Conflicts must be ask or auto_move.")
            updates["queue_conflict_mode"] = mode or DEFAULT_QUEUE_CONFLICT_MODE
        person_changed = (
            "prompt_person_id" in updates
            and _text(updates.get("prompt_person_id")) != _text(current_settings.get("prompt_person_id"))
        )
        _save_hash(store, SETTINGS_KEY, updates)
        if person_changed:
            store.delete(PROMPT_PROFILE_KEY)
            store.hdel(
                RUNTIME_KEY,
                "last_profile_finished_at",
                "last_profile_duration_ms",
                "last_profile_error",
            )
        next_settings = {**current_settings, **updates}
        selected_person_id = _text(next_settings.get("prompt_person_id"))
        if (
            person_changed
            and _person_prompt_context_enabled(selected_person_id, next_settings, store)
            and selected_person_id
            and _profile_history(
                store,
                person_id=selected_person_id,
                provider_id=next_settings.get("provider"),
            )
        ):
            _schedule_music_prompt_profile_refresh(store)
        if any(key.startswith("airplay_receiver_") for key in updates):
            _configure_external_audio(next_settings, _player(store))
        return {"ok": True, "message": "Personal Music Core settings saved."}

    if action_name == "music_airplay_stop":
        module = _external_audio_module()
        if module is None:
            raise RuntimeError("External Audio Input is not available in this Tater build.")
        result = module.stop_external_audio_input()
        return {
            "ok": True,
            "message": "AirPlay input stopped on the selected satellites.",
            "status": result if isinstance(result, dict) else {},
        }

    if action_name == "music_recommendations_refresh":
        started = _schedule_recommendation_refresh(store, force=True)
        return {
            "ok": True,
            "message": (
                f"{assistant_name} is preparing fresh recommendation playlists in the background."
                if started
                else f"{assistant_name} is already refreshing music recommendations."
            ),
        }

    if action_name == "music_recommendation_play":
        player = _play_recommendation(body.get("id"), store, person_id=viewer_person_id)
        return {
            "ok": True,
            "message": f"Playing {_track_label(player.get('current') or {})} from {recommendations_label}.",
        }

    if action_name == "music_ui_play":
        existing = _player(store, viewer_person_id)
        selected_provider = _provider_id(
            values.get("provider"),
            _action_provider(_settings(store).get("provider")),
        )
        existing_provider = _provider_id(
            existing.get("provider"),
            _action_provider(_settings(store).get("provider")),
        )
        queue = existing.get("queue") if isinstance(existing.get("queue"), list) else []
        if (
            not _text(values.get("query"))
            and queue
            and selected_provider == existing_provider
        ):
            resume_existing = _text(existing.get("status")).lower() == "paused"
            old_targets = _list(existing.get("targets") or existing.get("target"))
            requested_targets = values.get("targets")
            if not _list(requested_targets):
                requested_targets = old_targets
            targets = _resolve_targets(
                requested_targets,
                client=store,
                provider_id=selected_provider,
            )
            if not targets:
                raise ValueError("Choose one or more valid satellites, stereo pairs, or media players.")
            _validate_catalog_provider_targets(targets)
            existing["shuffle"] = _as_bool(values.get("shuffle"), bool(existing.get("shuffle")))
            existing["volume_percent"] = _as_int(
                values.get("volume_percent"),
                _as_int(existing.get("volume_percent"), 75, 0, 100),
                0,
                100,
            )
            _save_player(existing, store, viewer_person_id)
            _route_player_targets(targets, person_id=viewer_person_id, client=store)
            player = _resume_player(person_id=viewer_person_id, client=store)
            return {
                "ok": True,
                "message": (
                    f"Resumed {_track_label(player.get('current') or {})}."
                    if resume_existing
                    else f"Playing {_track_label(player.get('current') or {})}."
                ),
            }
        requested_targets = values.get("targets")
        if not _list(requested_targets):
            requested_targets = _list(existing.get("targets") or existing.get("target"))
        result = _play_request(
            {
                "provider": selected_provider,
                "query": values.get("query"),
                "targets": requested_targets,
                "shuffle": (
                    values.get("shuffle")
                    if values.get("shuffle") is not None
                    else existing.get("shuffle")
                ),
                "volume_percent": (
                    values.get("volume_percent")
                    if values.get("volume_percent") is not None
                    else existing.get("volume_percent")
                ),
            },
            viewer_origin,
            store,
            # The dashboard acts deliberately: take over busy rooms without
            # the TTS confirmation used for voice requests.
            force=True,
        )
        return {"ok": True, "message": _text(result.get("summary_for_user"))}

    if action_name == "music_ui_save_player":
        player = _player(store, viewer_person_id)
        current_settings = _settings(store)
        selected_provider = _action_provider(current_settings.get("provider"))
        old_targets = _list(player.get("targets") or player.get("target"))
        old_player_settings = _selected_player_settings(
            old_targets,
            current_settings,
            default_volume=_as_int(player.get("volume_percent"), 75, 0, 100),
        )
        targets = _resolve_targets(
            values.get("targets") or values.get("target"),
            client=store,
            provider_id=selected_provider,
        )
        if not targets:
            raise ValueError("Choose one or more valid satellites, stereo pairs, or media players.")
        _validate_catalog_provider_targets(targets)
        requested_volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        submitted_player_settings = _normalize_player_settings(
            values.get("player_settings"),
            targets=targets,
            cfg=current_settings,
            default_volume=requested_volume,
        )
        if "player_settings" in values:
            _save_player_calibrations(store, submitted_player_settings)
        next_settings = _settings(store)
        if "mixed_sync_adjustment_ms" in values:
            base_mixed_sync_adjustment = _save_mixed_sync_adjustment(
                store,
                targets,
                values.get("mixed_sync_adjustment_ms"),
            )
        else:
            base_mixed_sync_adjustment = _mixed_sync_adjustment(targets, next_settings)
        mixed_sync_adjustment = _mixed_sync_from_player_settings(
            targets,
            submitted_player_settings,
            base_mixed_sync_adjustment,
        )
        was_playing = _text(player.get("status")).lower() == "playing"
        old_mixed_sync_adjustment = _mixed_sync_from_player_settings(
            old_targets,
            old_player_settings,
            _mixed_sync_adjustment(old_targets, current_settings),
        )
        mixed_sync_changed = old_mixed_sync_adjustment != mixed_sync_adjustment
        player_settings_changed = old_player_settings != {
            target: submitted_player_settings[target]
            for target in targets
            if target in submitted_player_settings
        }
        player["provider"] = selected_provider
        player["mixed_sync_adjustment_ms"] = mixed_sync_adjustment
        player["shuffle"] = _as_bool(values.get("shuffle"), bool(player.get("shuffle")))
        player["volume_percent"] = requested_volume
        _save_player(player, store, viewer_person_id)
        # Sleep timer from the player card: a submitted number arms the
        # countdown; blank/0 clears it.
        if "sleep_timer_minutes" in values:
            raw_sleep = values.get("sleep_timer_minutes")
            sleep_minutes = (
                0 if raw_sleep in (None, "") else _as_int(raw_sleep, 0, 0, SLEEP_TIMER_MAX_MINUTES)
            )
            _set_sleep_timer(sleep_minutes, person_id=viewer_person_id, client=store)
        targets_changed = old_targets != targets
        player = _route_player_targets(
            targets,
            force_restart=mixed_sync_changed or player_settings_changed,
            resume_delay=_transfer_resume_delay(viewer_person_id, store),
            person_id=viewer_person_id,
            client=store,
        )
        if was_playing and (targets_changed or mixed_sync_changed or player_settings_changed):
            return {
                "ok": True,
                "message": (
                    f"Updated player calibration for {_target_summary(targets)}."
                    if (mixed_sync_changed or player_settings_changed) and not targets_changed
                    else f"Moved music to {_target_summary(targets)}."
                ),
            }
        return {"ok": True, "message": f"Music player set to {_target_summary(targets)}."}

    if action_name == "music_ui_test_sync":
        player = _player(store, viewer_person_id)
        cfg = _settings(store)
        selected_provider = _provider_id(
            values.get("provider"),
            _action_provider(cfg.get("provider")),
        )
        targets = _resolve_targets(
            values.get("targets") or values.get("target"),
            client=store,
            provider_id=selected_provider,
        )
        if not targets:
            raise ValueError("Choose one or more players before testing sync.")
        _validate_catalog_provider_targets(targets)
        volume = _as_int(
            values.get("volume_percent"),
            _as_int(player.get("volume_percent"), 75, 0, 100),
            0,
            100,
        )
        player_settings = _normalize_player_settings(
            values.get("player_settings"),
            targets=targets,
            cfg=cfg,
            default_volume=volume,
        )
        active_targets = _list(player.get("targets") or player.get("target"))
        if _text(player.get("status")).lower() == "playing" and active_targets:
            _stop_target(
                active_targets,
                expected_voice_core_sessions=_playback_voice_core_sessions(player),
            )
            player["status"] = "stopped"
            player["started_at"] = 0.0
            _save_player(player, store)

        from media_playback import play_media_url_targets

        result = play_media_url_targets(
            targets,
            "",
            audio_bytes=_sync_test_wav(),
            media_type="audio/wav",
            media_content_type="music",
            filename="tater-sync-test.wav",
            text="Tater player sync test",
            volume_percent=volume,
            mixed_sync_adjustment_ms=_mixed_sync_from_player_settings(
                targets,
                player_settings,
                _mixed_sync_adjustment(targets, cfg),
            ),
            target_volume_percent={
                target: setting["volume_percent"]
                for target, setting in player_settings.items()
            },
            target_sync_offset_ms={
                target: setting["sync_offset_ms"]
                for target, setting in player_settings.items()
            },
            target_transport_mode={
                target: _player_transport_mode(setting.get("transport_mode"))
                for target, setting in player_settings.items()
                if target.casefold().startswith(("sonos:", "integration:sonos:"))
            },
            timeout_s=30.0,
            respect_reply_playback=False,
        )
        if not isinstance(result, dict) or result.get("ok") is False:
            raise ValueError(_text((result or {}).get("error")) or "The sync test could not start.")
        return {
            "ok": True,
            "message": "Playing sync clicks. Adjust any player that sounds early or late, then save.",
        }

    if action_name == "music_ui_set_volume":
        player = _player(store, viewer_person_id)
        live_result = _set_player_volume(
            player,
            values.get("volume_percent"),
            store=store,
        )
        volume = _as_int(player.get("volume_percent"), 75, 0, 100)
        warnings = [
            _text(value)
            for value in list(live_result.get("warnings") or [])
            if _text(value)
        ]
        if warnings:
            player["warnings"] = warnings
        _save_player(player, store, viewer_person_id)
        return {
            "ok": True,
            "message": (
                f"Music volume set to {volume}%. " + " ".join(warnings)
                if warnings
                else f"Music volume set to {volume}%."
            ),
        }

    if action_name == "music_ui_mute_all":
        player = _player(store, viewer_person_id)
        live_result = _apply_player_mute(player, mute=True, client=store)
        _apply_mute_warnings(player, live_result)
        _save_player(player, store, viewer_person_id)
        return {"ok": True, "message": "All speakers muted."}

    if action_name == "music_ui_unmute_all":
        player = _player(store, viewer_person_id)
        live_result = _apply_player_mute(player, mute=False, client=store)
        _apply_mute_warnings(player, live_result)
        _save_player(player, store, viewer_person_id)
        volume = _as_int(player.get("volume_percent"), 75, 0, 100)
        return {"ok": True, "message": f"All speakers unmuted — volume back to {volume}%."}

    if action_name == "music_ui_seek":
        player = _seek_player(
            _as_float(values.get("position_seconds")),
            person_id=viewer_person_id,
            client=store,
        )
        position = _player_position_seconds(player)
        return {"ok": True, "message": f"Moved to {round(position)} seconds."}

    if action_name == "music_ui_seek_relative":
        current = _player(store, viewer_person_id)
        delta = _as_float(values.get("delta_seconds"))
        player = _seek_player(
            _player_position_seconds(current) + delta,
            person_id=viewer_person_id,
            client=store,
        )
        position = _player_position_seconds(player)
        return {"ok": True, "message": f"Moved to {round(position)} seconds."}

    if action_name == "music_ui_set_shuffle":
        player = _set_player_shuffle(
            _as_bool(values.get("shuffle"), False),
            person_id=viewer_person_id,
            client=store,
        )
        return {
            "ok": True,
            "message": "Shuffle is on." if player.get("shuffle") else "Shuffle is off.",
        }

    if action_name == "music_ui_stop":
        _stop_player(person_id=viewer_person_id, client=store)
        return {"ok": True, "message": "Music stopped."}

    if action_name == "music_ui_pause":
        player = _pause_player(person_id=viewer_person_id, client=store)
        return {
            "ok": True,
            "message": f"Paused {_track_label(player.get('current') or {})}.",
        }

    if action_name == "music_ui_next":
        player = _advance_player(1, person_id=viewer_person_id, client=store)
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_previous":
        player = _advance_player(-1, person_id=viewer_person_id, client=store)
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_queue_play":
        item_id = _text(body.get("id"))
        try:
            index = int(item_id.split(":", 1)[1])
        except Exception as exc:
            raise ValueError("Queue position is invalid.") from exc
        player = _start_player_index(index, person_id=viewer_person_id, client=store)
        return {"ok": True, "message": f"Playing {_track_label(player.get('current') or {})}."}

    if action_name == "music_ui_facet_play":
        item_id = _text(body.get("id"))
        facet, separator, value = item_id.partition(":")
        if not separator or not value or facet not in {"genre", "artist", "album"}:
            raise ValueError("Music category is invalid.")
        current_player = _player(store, viewer_person_id)
        current_settings = _settings(store)
        player_result = _play_request(
            {
                "provider": _action_provider(current_settings.get("provider")),
                facet: value,
                "targets": (
                    _list(current_player.get("targets") or current_player.get("target"))
                    or _list(
                        current_settings.get("default_targets")
                        or current_settings.get("default_target")
                    )
                ),
                "shuffle": facet != "album",
                "volume_percent": current_player.get("volume_percent"),
            },
            viewer_origin,
            store,
            force=True,
        )
        return {"ok": True, "message": _text(player_result.get("summary_for_user"))}

    raise ValueError(f"Unknown Personal Music Core action: {action_name}")


def _fetch_track_artwork(track: Dict[str, Any], client: Any = None, person_id: Any = "") -> Dict[str, Any]:
    provider_id = _provider_id(track.get("provider"))
    person_scope = _text(person_id) or _text(track.get("person_scope"))
    provider = (
        _provider(client, provider_id, person_scope)
        if person_scope
        else _provider(client, provider_id)
    )
    artwork_url_fn = getattr(provider, "artwork_url", None)
    source_url = artwork_url_fn(track) if callable(artwork_url_fn) else ""
    source_url = _text(source_url)
    if not source_url:
        raise KeyError("This provider does not have artwork for the track.")

    cache_key = hashlib.sha256(source_url.encode("utf-8")).hexdigest()
    with _artwork_cache_lock:
        cached = _artwork_cache.get(cache_key)
        if isinstance(cached, dict) and cached.get("body"):
            return dict(cached)
        if _artwork_failure_until.get(cache_key, 0.0) > time.monotonic():
            raise RuntimeError("Provider artwork is temporarily unavailable.")
        inflight = _artwork_inflight.get(cache_key)
        fetch_owner = inflight is None
        if fetch_owner:
            inflight = threading.Event()
            _artwork_inflight[cache_key] = inflight

    if not fetch_owner:
        if not inflight.wait(ARTWORK_INFLIGHT_WAIT_TIMEOUT_SECONDS):
            raise TimeoutError("Timed out waiting for provider artwork.")
        with _artwork_cache_lock:
            cached = _artwork_cache.get(cache_key)
            if isinstance(cached, dict) and cached.get("body"):
                return dict(cached)
        raise RuntimeError("Provider artwork is temporarily unavailable.")

    slot_acquired = False
    try:
        slot_acquired = _artwork_fetch_slots.acquire(
            timeout=ARTWORK_INFLIGHT_WAIT_TIMEOUT_SECONDS,
        )
        if not slot_acquired:
            raise TimeoutError("Provider artwork queue is busy.")
        response = requests.get(
            source_url,
            headers={"Accept": "image/jpeg,image/png,image/webp,image/*"},
            timeout=(ARTWORK_CONNECT_TIMEOUT_SECONDS, ARTWORK_READ_TIMEOUT_SECONDS),
        )
        response.raise_for_status()
        body = bytes(response.content or b"")
        content_type = _text(response.headers.get("Content-Type")).split(";", 1)[0].lower()
        if not content_type.startswith("image/"):
            raise ValueError("The music provider did not return an image.")
        if not body or len(body) > 12 * 1024 * 1024:
            raise ValueError("The provider artwork is empty or too large.")

        cached = {"body": body, "content_type": content_type}
        with _artwork_cache_lock:
            if len(_artwork_cache) >= 256:
                _artwork_cache.clear()
            _artwork_cache[cache_key] = cached
            _artwork_failure_until.pop(cache_key, None)
        return dict(cached)
    except Exception as exc:
        with _artwork_cache_lock:
            _artwork_failure_until[cache_key] = (
                time.monotonic() + ARTWORK_FAILURE_CACHE_SECONDS
            )
        logger.warning(
            "[Music] artwork fetch failed track=%s error=%s",
            _text(track.get("id")) or "-",
            _text(exc) or type(exc).__name__,
        )
        raise
    finally:
        if slot_acquired:
            _artwork_fetch_slots.release()
        with _artwork_cache_lock:
            completed = _artwork_inflight.pop(cache_key, None)
        if completed is not None:
            completed.set()


def _fallback_track_artwork(track: Dict[str, Any]) -> Dict[str, Any]:
    label = _text(track.get("album") or track.get("artist") or track.get("title") or "Music")
    digest = hashlib.sha256(label.encode("utf-8")).hexdigest()
    color_a = f"#{digest[:6]}"
    color_b = f"#{digest[6:12]}"
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="360" height="360" viewBox="0 0 360 360">
<defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop stop-color="{color_a}"/><stop offset="1" stop-color="{color_b}"/></linearGradient></defs>
<rect width="360" height="360" rx="28" fill="#15111f"/><circle cx="180" cy="165" r="118" fill="url(#g)" opacity=".92"/>
<circle cx="180" cy="165" r="48" fill="#15111f"/><circle cx="180" cy="165" r="13" fill="#ffbd59"/>
<path d="M254 77v142c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V96l-88 19v124c0 24-20 43-45 43-20 0-36-13-36-30s16-30 36-30c9 0 18 3 24 7V95z" fill="#fff" opacity=".9"/>
</svg>"""
    return {"body": svg.encode("utf-8"), "content_type": "image/svg+xml"}


def handle_core_webhook(
    *,
    webhook: str,
    query: Optional[Dict[str, Any]] = None,
    redis_client=None,
    **_kwargs,
) -> Any:
    if _text(webhook).lower() != "artwork":
        raise KeyError(f"Unsupported Personal Music Core webhook: {webhook}")
    params = query if isinstance(query, dict) else {}
    provider_id = _provider_id(
        params.get("provider"),
        _provider_id(_settings(redis_client).get("provider")),
    )
    # The viewed Person (when the tab is scoped) or the fallback search in
    # _client_track decides whose credentials fetch the artwork.
    viewer_person = _text(params.get("person"))
    track = _client_track(params.get("track_id"), provider_id, redis_client, viewer_person)
    fallback = False
    try:
        artwork = _fetch_track_artwork(
            track,
            redis_client,
            person_id=_text(track.get("person_scope")) or viewer_person,
        )
    except Exception:
        artwork = _fallback_track_artwork(track)
        fallback = True
    from starlette.responses import Response

    headers = {
        "Cache-Control": "private, max-age=30" if fallback else "private, max-age=86400"
    }
    if fallback:
        headers["X-Tater-Artwork-Fallback"] = "1"
    return Response(
        content=artwork["body"],
        media_type=artwork["content_type"],
        headers=headers,
    )


def get_core_system_tasks(*, redis_client=None, **_kwargs) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    assistant_name = _assistant_first_name(store)
    recommendations_label = _recommendations_label(store)
    cfg = _settings(store)
    runtime = _runtime(store)
    provider_id = _provider_id(cfg.get("provider"))
    connected = _paired(cfg, provider_id)
    catalog_interval = _as_int(
        cfg.get("catalog_sync_interval_seconds"),
        DEFAULT_SYNC_INTERVAL_SECONDS,
        60,
        86400,
    )
    recommendation_interval = (
        _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168) * 3600
    )
    profile_interval = (
        _as_int(cfg.get("prompt_profile_interval_hours"), 12, 1, 168) * 3600
    )
    # A task stays available while either the global toggle or any Person's
    # own override still wants it to run.
    recommendations_enabled = _recommendations_possible(cfg, store)
    prompt_context_enabled = _prompt_context_possible(cfg, store)
    prompt_person_id = _text(cfg.get("prompt_person_id"))
    prompt_person_name = _people_person_name(prompt_person_id, store) if prompt_person_id else ""
    has_history = any(
        _provider_id(row.get("provider")) == provider_id
        for row in _listening_history(store)
    ) or any(
        # Linked People's own listening history also feeds recommendations.
        _provider_id(row.get("provider")) == provider_id
        for linked_id in _linked_person_ids(store)
        for row in _listening_history(store, linked_id)
    )
    last_sync = max(
        _as_float(runtime.get("last_sync_finished_at")),
        _as_float(runtime.get("last_sync_at")),
    )
    last_recommendation = max(
        _as_float(runtime.get("last_recommendation_finished_at")),
        _as_float(runtime.get("last_recommendation_at")),
        _as_float(runtime.get("last_recommendation_attempt_at")),
    )
    last_continuation = max(
        _as_float(runtime.get("last_continuation_finished_at")),
        _as_float(runtime.get("last_continuation_at")),
    )
    last_profile = _as_float(runtime.get("last_profile_finished_at"))
    recommendation_running = bool(
        _recommendation_lock.locked()
        or (_recommendation_thread is not None and _recommendation_thread.is_alive())
    )
    continuation_running = bool(
        _continuation_lock.locked()
        or any(
            thread is not None and thread.is_alive()
            for thread in _continuation_threads.values()
        )
    )
    profile_running = bool(
        _profile_lock.locked()
        or (_profile_thread is not None and _profile_thread.is_alive())
    )
    has_profile_history = bool(
        prompt_person_id
        and _profile_history(
            store,
            person_id=prompt_person_id,
            provider_id=provider_id,
        )
    )
    profile_available = bool(
        connected
        and _person_prompt_context_enabled(prompt_person_id, cfg, store)
        and prompt_person_id
        and prompt_person_name
        and has_profile_history
    )
    follow_me_enabled = _as_bool(cfg.get("follow_me_enabled"), False)
    follow_me_interval = _follow_me_poll_interval(cfg)
    last_follow_me = _as_float(runtime.get("last_follow_me_at"))
    follow_me_ha_ready = bool(_text(_ha_config(store).get("token")))
    return {
        "label": "Personal Music Core",
        "order": 36,
        "tasks": [
            {
                "id": "catalog_sync",
                "label": "Music Library Sync",
                "description": "Refreshes artists, albums, genres, tracks, and provider artwork.",
                "interval_seconds": catalog_interval,
                "running": _catalog_sync_lock.locked(),
                "started_at": _catalog_sync_started_at,
                "finished_at": last_sync,
                "duration_ms": _as_float(runtime.get("last_sync_duration_ms")),
                "next_run_at": last_sync + catalog_interval if last_sync else 0.0,
                "last_error": _text(runtime.get("last_sync_error")),
                "run_count": _as_int(runtime.get("sync_run_count"), 0, 0, 1_000_000_000),
                "available": connected,
                "unavailable_reason": f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before syncing music.",
                "status": "idle" if connected else "waiting",
                "requires_running": True,
                "order": 10,
            },
            {
                "id": "recommendation_refresh",
                "label": recommendations_label,
                "description": "Builds fresh AI-named playlists from listening history.",
                "interval_seconds": recommendation_interval,
                "enabled": recommendations_enabled,
                "running": recommendation_running,
                "started_at": _recommendation_started_at,
                "finished_at": last_recommendation,
                "duration_ms": _as_float(runtime.get("last_recommendation_duration_ms")),
                "next_run_at": (
                    last_recommendation + recommendation_interval
                    if last_recommendation and recommendations_enabled
                    else 0.0
                ),
                "last_error": _text(runtime.get("last_recommendation_error")),
                "run_count": _as_int(
                    runtime.get("recommendation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                ),
                "available": connected and has_history,
                "unavailable_reason": (
                    f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before refreshing recommendations."
                    if not connected
                    else f"Play some music first so {assistant_name} has listening history to use."
                ),
                "status": "idle" if connected and has_history else "waiting",
                "requires_running": True,
                "order": 20,
            },
            {
                "id": "music_profile_refresh",
                "label": "Music Prompt Profile",
                "description": "Builds compact favorite genre, favorite artist, and recent-track context for the selected Person.",
                "interval_seconds": profile_interval,
                "enabled": prompt_context_enabled,
                "running": profile_running,
                "started_at": _profile_started_at,
                "finished_at": last_profile,
                "duration_ms": _as_float(runtime.get("last_profile_duration_ms")),
                "next_run_at": (
                    last_profile + profile_interval
                    if last_profile and prompt_context_enabled
                    else 0.0
                ),
                "last_error": _text(runtime.get("last_profile_error")),
                "run_count": _as_int(
                    runtime.get("profile_run_count"),
                    0,
                    0,
                    1_000_000_000,
                ),
                "available": profile_available,
                "unavailable_reason": (
                    f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before building a music profile."
                    if not connected
                    else "Turn on Music Prompt Context in Personal Music Core Settings."
                    if not prompt_context_enabled
                    else "Choose an existing Person in Personal Music Core Settings."
                    if not prompt_person_id or not prompt_person_name
                    else f"Play some music for {prompt_person_name} first."
                ),
                "status": "idle" if profile_available else "waiting",
                "requires_running": True,
                "order": 30,
            },
            {
                "id": "continuous_radio_refill",
                "label": "Continuous-Radio Refill",
                "description": "Automatically extends an active queue when playback nears its final tracks.",
                "interval_seconds": 0,
                "enabled": True,
                "manual": False,
                "schedule_label": "Event driven",
                "next_run_label": "Near queue end",
                "running": continuation_running,
                "started_at": _continuation_started_at,
                "finished_at": last_continuation,
                "duration_ms": _as_float(runtime.get("last_continuation_duration_ms")),
                "last_error": _text(runtime.get("last_continuation_error")),
                "run_count": _as_int(
                    runtime.get("continuation_run_count"),
                    0,
                    0,
                    1_000_000_000,
                ),
                "available": connected,
                "unavailable_reason": f"Connect {PROVIDER_LABELS.get(provider_id, provider_id)} before starting continuous radio.",
                "status": "idle" if connected else "waiting",
                "requires_running": True,
                "order": 40,
            },
            {
                "id": "follow_me",
                "label": "Follow-Me Presence",
                "description": "Checks linked People's Home Assistant person entities and moves their music to the room they're in.",
                "interval_seconds": follow_me_interval,
                "enabled": follow_me_enabled,
                "running": bool(_follow_me_lock.locked()),
                "started_at": 0.0,
                "finished_at": last_follow_me,
                "duration_ms": 0.0,
                "next_run_at": (
                    last_follow_me + follow_me_interval
                    if last_follow_me and follow_me_enabled
                    else 0.0
                ),
                "last_error": _text(runtime.get("follow_me_last_error")),
                "run_count": _as_int(
                    runtime.get("follow_me_run_count"),
                    0,
                    0,
                    1_000_000_000,
                ),
                "available": follow_me_ha_ready,
                "unavailable_reason": (
                    "Enable the Home Assistant integration in Tater (base URL and token) before "
                    "Follow-Me can track People."
                    if not follow_me_ha_ready
                    else ""
                ),
                "status": "idle" if follow_me_enabled and follow_me_ha_ready else "waiting",
                "requires_running": True,
                "order": 50,
            },
        ],
    }


def run_core_system_task(*, task_id: str, redis_client=None, **_kwargs) -> Dict[str, Any]:
    store = redis_client or globals().get("redis_client")
    task = _text(task_id).lower()
    provider_id = _provider_id(_settings(store).get("provider"))
    if task == "catalog_sync":
        catalog = _sync_catalog(store, provider_id)
        return {"ok": True, "track_count": len(catalog.get("tracks") or [])}
    if task == "recommendation_refresh":
        recommendations = _generate_recommendations(store)
        return {
            "ok": True,
            "playlist_count": len(recommendations.get("playlists") or []),
        }
    if task == "music_profile_refresh":
        profile = _generate_music_prompt_profile(store)
        return {
            "ok": True,
            "person_id": _text(profile.get("person_id")),
            "history_event_count": _as_int(profile.get("history_event_count"), 0, 0, 1_000_000_000),
        }
    if task == "follow_me":
        return _follow_me_tick(store)
    raise KeyError(f"Unknown Personal Music Core task: {task_id}")


def _upstream_provider(provider_id: str, scope: str) -> Any:
    """Provider instance one proxied request resolves against.

    A scoped kind ("<provider>:<person>") serves that Person's own credentials
    (or their second linked source via the "<person>+<source>" slot id); an
    unscoped kind serves the household's configured source.
    """
    provider_id = _provider_id(provider_id, "")
    if provider_id == "network_share":
        raise LookupError("Unknown stream request.")
    provider = None
    if scope:
        provider = _person_link_provider(scope, provider_id)
    if provider is None:
        provider_class = _provider_class(provider_id)
        builder = getattr(provider_class, "from_settings", None)
        provider = builder(_settings()) if builder else None
    if provider is None:
        raise LookupError("Unknown stream request.")
    return provider


def _upstream_request(
    kind: str,
    item_id: str,
    *,
    sync: bool = False,
    person_id: Any = "",
) -> "tuple[str, Dict[str, str]]":
    """Build (url, headers) for proxying one provider stream or artwork request.

    Kinds are "<provider>" / "<provider>_art" for the household source and
    "<provider>:<scope>" / "<provider>_art:<scope>" for a Person's own linked
    account. item_id is provider-specific (an Emby/Jellyfin item id, a Plex part
    or thumb path, a Subsonic song or coverArt id).
    """
    base_kind, _, scope = kind.partition(":")
    if base_kind.endswith("_art"):
        provider_id, art = base_kind[: -len("_art")], True
    else:
        provider_id, art = base_kind, False
    if provider_id in ("share", "network_share") or not provider_id:
        raise LookupError("Unknown stream request.")
    provider = _upstream_provider(provider_id, _text(scope) or _text(person_id))
    if not getattr(provider, "connected", False):
        raise RuntimeError(f"{PROVIDER_LABELS.get(provider_id, provider_id)} is not connected.")
    proxy_request = getattr(provider, "proxy_request", None)
    if not callable(proxy_request):
        raise LookupError("Unknown stream request.")
    return proxy_request(_text(item_id), art=art, sync=sync)


def _share_parse_range(value: str, size: int) -> Optional[tuple[int, int]]:
    """Parse a Range header into an inclusive (start, end) byte pair."""
    if not value or size <= 0:
        return None
    match = re.match(r"^bytes=(\d*)-(\d*)$", value.strip(), re.IGNORECASE)
    if not match:
        return None
    start_raw, end_raw = match.group(1), match.group(2)
    if not start_raw and not end_raw:
        return None
    if not start_raw:  # suffix range: last N bytes
        suffix = int(end_raw)
        if suffix <= 0:
            return None
        start = max(0, size - suffix)
    else:
        start = int(start_raw)
    end = size - 1 if not end_raw else min(int(end_raw), size - 1)
    if start >= size or start > end:
        return None
    return start, end


def _share_send_file(
    handler: "_MusicStreamHandler",
    file_path: str,
    *,
    download_name: str = "",
) -> None:
    """Serve one local file with Range support (the Emby proxy path serves 206s too)."""
    size = os.path.getsize(file_path)
    if size <= 0:
        handler._send_error(404, "File is empty.")
        return
    content_type = SHARE_MIME_TYPES.get(Path(file_path).suffix.casefold(), "application/octet-stream")
    span = _share_parse_range(handler.headers.get("Range") or "", size)
    if handler.headers.get("Range") and span is None:
        handler.send_response(416)
        handler.send_header("Content-Range", f"bytes */{size}")
        handler.send_header("Content-Length", "0")
        handler.end_headers()
        return
    status = 206 if span else 200
    start, end = span if span else (0, size - 1)
    try:
        stat = os.stat(file_path)
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(end - start + 1))
        handler.send_header("Accept-Ranges", "bytes")
        handler.send_header("Last-Modified", time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime(stat.st_mtime)))
        handler.send_header("ETag", f'"share-{int(stat.st_mtime)}-{size}"')
        if span:
            handler.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        if download_name:
            handler.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
        handler.end_headers()
        if handler.command == "HEAD":
            return
        with open(file_path, "rb") as handle:
            handle.seek(start)
            remaining = end - start + 1
            while remaining > 0:
                chunk = handle.read(min(STREAM_CHUNK_SIZE, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
                handler.wfile.write(chunk)
    except (BrokenPipeError, ConnectionResetError):
        pass


class _MusicStreamHandler(BaseHTTPRequestHandler):
    """Range-capable local server that keeps provider tokens out of stream URLs
    and serves mounted-share files straight from disk."""

    server_version = "TaterCustomMusic/1.0"
    protocol_version = "HTTP/1.1"
    upstream = None

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A002 - stdlib signature
        logger.debug("[Music] stream server: " + fmt % args)

    def do_GET(self) -> None:  # noqa: N802 - stdlib signature
        self._handle()

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib signature
        self._handle()

    def _handle(self) -> None:
        try:
            parsed = urlparse(self.path)
            parts = [part for part in parsed.path.split("/") if part]
            if len(parts) != 4 or parts[0] != "stream":
                raise LookupError("not found")
            token, kind, item_id = parts[1], parts[2], parts[3]
            if not hmac.compare_digest(token, _stream_token()):
                self._send_error(403, "Invalid stream token.")
                return
            base_kind = kind.partition(":")[0]
            if base_kind in ("share", "share_art"):
                self._serve_share(kind, item_id)
                return
            sync = any(
                key.casefold() == "sync" and _text(value) not in {"0", "false", "no"}
                for key, value in parse_qsl(parsed.query)
            )
            # Proxy kinds quote their item id (a Plex part path carries "/"s);
            # decode it once for the upstream request builders.
            upstream_url, headers = _upstream_request(
                kind,
                unquote(item_id),
                sync=sync,
                person_id="",
            )
            forward_headers = {
                key.title(): value
                for key, value in (
                    ("range", self.headers.get("Range")),
                    ("if-range", self.headers.get("If-Range")),
                )
                if value
            }
            headers.update(forward_headers)
            upstream = requests.request(
                "HEAD" if self.command == "HEAD" else "GET",
                upstream_url,
                headers=headers,
                stream=True,
                timeout=(10, 60),
            )
            if upstream.status_code >= 400:
                upstream.close()
                self._send_error(
                    502 if upstream.status_code >= 500 else upstream.status_code,
                    "The music source rejected the stream request.",
                )
                return
            response_headers = {"Cache-Control": "private, max-age=300"}
            for name in (
                "Accept-Ranges",
                "Content-Length",
                "Content-Range",
                "Content-Type",
                "ETag",
                "Last-Modified",
            ):
                value = upstream.headers.get(name)
                if value:
                    response_headers[name] = value
            media_type = _text(upstream.headers.get("Content-Type")).split(";", 1)[0].strip()
            if media_type:
                response_headers["Content-Type"] = media_type
            self.send_response(upstream.status_code)
            for name, value in response_headers.items():
                self.send_header(name, value)
            self.end_headers()
            if self.command == "HEAD":
                upstream.close()
                return
            try:
                for chunk in upstream.iter_content(chunk_size=STREAM_CHUNK_SIZE):
                    if chunk:
                        self.wfile.write(chunk)
            except Exception:
                # Client disconnected mid-stream; nothing further to clean up.
                pass
            finally:
                upstream.close()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except LookupError as exc:
            # Unknown stream route/kind: a client error, not an upstream fault.
            try:
                self._send_error(404, _text(exc) or "Not found.")
            except Exception:
                pass
        except Exception as exc:
            logger.warning("[Music] stream server request failed: %s", exc)
            try:
                self._send_error(502, _text(exc) or "Stream request failed.")
            except Exception:
                pass

    def _serve_share(self, kind: str, item_id: str) -> None:
        """Serve a mounted-share file or cached artwork directly from disk."""
        try:
            # Scoped ids carry the share root they were scanned from, so a
            # Person's own share subfolder streams from their root, not the
            # household's (legacy unscoped ids keep using the global root).
            global_root = _text(_settings().get("share_root_path"))
            if kind == "share_art":
                scope, sep, _bare = _text(item_id).partition(":")
                index_key = (
                    f"{SHARE_ART_INDEX_KEY}:{scope}"
                    if sep
                    else _share_art_index_key(global_root)
                )
                # Index entries are paths this core wrote itself, never client input.
                entry = _load_json(globals().get("redis_client"), index_key, {}).get(item_id)
                art_path = _text(entry.get("path")) if isinstance(entry, dict) else ""
                if not art_path or not os.path.isfile(art_path):
                    self._send_error(404, "Artwork is not available.")
                    return
                _share_send_file(self, art_path)
                return
            root, rel_path = _share_root_and_relpath_from_id(item_id, global_root)
            file_path = (
                _share_contained_path(root or "", rel_path or "") if rel_path else None
            )
            if not file_path or not os.path.isfile(file_path):
                self._send_error(404, "Share file is not available.")
                return
            _share_send_file(self, file_path, download_name=Path(file_path).name)
        except Exception as exc:
            logger.warning("[Music] share stream request failed: %s", exc)
            try:
                self._send_error(500, "Share stream request failed.")
            except Exception:
                pass

    def _send_error(self, status: int, message: str) -> None:
        body = message.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)


_stream_server_lock = threading.Lock()
_stream_server: Optional["ThreadingHTTPServer"] = None
_stream_server_signature = ""


def _ensure_stream_server() -> Dict[str, Any]:
    """Start or restart the internal stream server when its port changes."""
    global _stream_server, _stream_server_signature
    port = _stream_port()
    token = _stream_token()
    signature = f"{port}:{hashlib.sha256(token.encode('utf-8')).hexdigest()[:12]}"
    with _stream_server_lock:
        if (
            _stream_server is not None
            and _stream_server_signature == signature
        ):
            return {"ok": True, "status": "ready", "port": port}
        if _stream_server is not None:
            try:
                _stream_server.shutdown()
                _stream_server.server_close()
            except Exception:
                pass
            _stream_server = None
        try:
            server = ThreadingHTTPServer(("", port), _MusicStreamHandler)
            server.daemon_threads = True
        except Exception as exc:
            return {
                "ok": False,
                "status": "error",
                "port": port,
                "error": f"Could not start the stream server on port {port}: {exc}",
            }
        thread = threading.Thread(
            target=server.serve_forever,
            name="personal-music-stream-server",
            daemon=True,
        )
        thread.start()
        _stream_server = server
        _stream_server_signature = signature
        return {"ok": True, "status": "ready", "port": port}


def _shutdown_stream_server() -> None:
    global _stream_server, _stream_server_signature
    with _stream_server_lock:
        if _stream_server is not None:
            try:
                _stream_server.shutdown()
                _stream_server.server_close()
            except Exception:
                pass
        _stream_server = None
        _stream_server_signature = ""


def run(stop_event: Optional[object] = None) -> None:
    logger.info("[Music] Core starting.")
    # The pre-3.5.0 shared Emby auth cache is orphaned by the per-identity
    # cache keys; drop it once so no stale household token survives a restart.
    try:
        redis_client.delete(EMBY_AUTH_CACHE_KEY)
    except Exception:
        pass
    try:
        while not (stop_event and getattr(stop_event, "is_set", lambda: False)()):
            cfg = _settings()
            _configure_external_audio(cfg, _player())
            active_provider = _provider_id(cfg.get("provider"))
            linked_person_ids = _linked_person_ids()

            def _any_source_connected() -> bool:
                if _paired(cfg, active_provider):
                    return True
                for pid in linked_person_ids:
                    try:
                        if _provider(None, "", pid).connected:
                            return True
                    except Exception:
                        continue
                return False

            if not _any_source_connected():
                _save_hash(redis_client, RUNTIME_KEY, {"status": "waiting_for_pairing"})
                time.sleep(1.0)
                continue
            runtime = _runtime()
            now = time.time()
            stream_status = _ensure_stream_server()
            if not stream_status.get("ok"):
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {
                        "stream_server_status": "error",
                        "stream_server_error": _text(stream_status.get("error"))[:500],
                    },
                )
                logger.warning("[Music] %s", stream_status.get("error"))
            interval = _as_int(
                cfg.get("catalog_sync_interval_seconds"),
                DEFAULT_SYNC_INTERVAL_SECONDS,
                60,
                86400,
            )
            try:
                if (
                    active_provider in CATALOG_PROVIDER_IDS
                    and (
                        _catalog_needs_artwork_refresh(provider_id=active_provider)
                        or now - _as_float(runtime.get("last_sync_at")) >= interval
                    )
                ):
                    _sync_catalog(provider_id=active_provider)
                    runtime = _runtime()
                # Linked People refresh their own catalogs on the same cadence.
                for pid in linked_person_ids:
                    for source_index, source_id in enumerate(
                        _person_catalog_source_ids(pid, redis_client)
                    ):
                        # Each linked source keeps its own catalog slot; a
                        # Person with two sources syncs and plays from both.
                        try:
                            slot_id = pid if source_index == 0 else _person_extra_slot(pid, source_id)
                            slot_payload = _catalog(provider_id=source_id, person_id=slot_id)
                            slot_synced = _as_float(slot_payload.get("synced_at"))
                            if (
                                _provider_id(slot_payload.get("provider"), "") != source_id
                                or not slot_synced
                                or now - slot_synced >= interval
                            ):
                                _sync_catalog(provider_id=source_id, person_id=slot_id)
                        except Exception as exc:
                            logger.warning(
                                "[Music] library sync for %s failed: %s",
                                _people_person_name(pid) or pid,
                                exc,
                            )
                # Every queue slot (shared + each Person's) is advanced and
                # kept topped up independently, so two People can listen to
                # their own music in different rooms at the same time.
                for queue_id in _active_queue_ids(store):
                    try:
                        # Sleep timers first: an expired timer force-stops the
                        # queue, so no maintenance (or refill) runs afterwards.
                        _sleep_timer_tick(store, queue_id)
                        # Gaining-room resume delays: start queues that were
                        # moved and are waiting out their resume delay.
                        _delayed_resume_tick(store, queue_id)
                        _advance_finished_player(store, person_id=queue_id)
                        _schedule_continuation_refresh(
                            person_id=queue_id,
                            client=store,
                        )
                    except Exception as exc:
                        logger.warning(
                            "[Music] queue %s maintenance failed: %s",
                            queue_id or "shared",
                            exc,
                        )
                # Follow-Me presence: check linked People's Home Assistant
                # person entities and hand their queues off to new rooms.
                if _as_bool(cfg.get("follow_me_enabled"), False) and now - _as_float(
                    runtime.get("last_follow_me_at")
                ) >= _follow_me_poll_interval(cfg):
                    try:
                        _follow_me_tick()
                    except Exception as exc:
                        logger.warning("[Music] follow-me presence check failed: %s", exc)
                        _save_hash(
                            redis_client,
                            RUNTIME_KEY,
                            {
                                "last_follow_me_at": time.time(),
                                "follow_me_last_error": _text(exc)[:300],
                            },
                        )
                recommendation_interval = (
                    _as_int(cfg.get("recommendation_interval_hours"), 12, 1, 168) * 3600
                )
                published = _recommendations()
                last_recommendation_cycle = max(
                    _as_float(runtime.get("last_recommendation_at")),
                    _as_float(runtime.get("last_recommendation_attempt_at")),
                )
                if _provider_id(published.get("provider"), "") != active_provider:
                    last_recommendation_cycle = 0.0
                has_history = any(
                    _provider_id(row.get("provider")) == active_provider
                    for row in _listening_history()
                ) or any(
                    # A fully linked household may have no shared history at
                    # all; its People's own mixes still need refreshing.
                    _provider_id(row.get("provider")) == active_provider
                    for linked_id in _linked_person_ids()
                    for row in _listening_history(None, linked_id)
                )
                if (
                    _recommendations_possible(cfg)
                    and active_provider in CATALOG_PROVIDER_IDS
                    and has_history
                    and now - last_recommendation_cycle >= recommendation_interval
                ):
                    _schedule_recommendation_refresh()
                profile_interval = (
                    _as_int(cfg.get("prompt_profile_interval_hours"), 12, 1, 168) * 3600
                )
                prompt_person_id = _text(cfg.get("prompt_person_id"))
                prompt_profile = _music_prompt_profile()
                last_profile_cycle = max(
                    _as_float(runtime.get("last_profile_finished_at")),
                    _as_float(prompt_profile.get("generated_at")),
                )
                if (
                    _text(prompt_profile.get("person_id")) != prompt_person_id
                    or _provider_id(prompt_profile.get("provider")) != active_provider
                ):
                    last_profile_cycle = 0.0
                if (
                    _prompt_context_possible(cfg)
                    and prompt_person_id
                    and _people_person_name(prompt_person_id)
                    and _profile_history(
                        None,
                        person_id=prompt_person_id,
                        provider_id=active_provider,
                    )
                    and now - last_profile_cycle >= profile_interval
                ):
                    _schedule_music_prompt_profile_refresh()
            except PermissionError as exc:
                logger.warning("[Music] provider authorization was revoked: %s", exc)
                redis_client.delete(EMBY_AUTH_CACHE_KEY)
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {
                        "status": "authorization_revoked",
                        "last_error": _text(exc)[:500],
                        "last_error_at": now,
                    },
                )
            except Exception as exc:
                logger.warning("[Music] background cycle failed: %s", exc)
                _save_hash(
                    redis_client,
                    RUNTIME_KEY,
                    {
                        "status": "error",
                        "last_error": _text(exc)[:500],
                        "last_error_at": now,
                    },
                )
            time.sleep(1.0)
    finally:
        _shutdown_stream_server()
        module = _external_audio_module()
        if module is not None:
            try:
                module.configure_external_audio_runtime({"enabled": False})
            except Exception:
                pass
        logger.info("[Music] Core stopped.")
