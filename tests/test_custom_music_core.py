"""Tests for cores/custom_music_core.py.

Mirrors Tater_Shop's core test style: stub the shared ``helpers`` module with a
FakeRedis, load the core file directly via importlib, and exercise it without a
running Tater.
"""

import asyncio
import importlib.util
import json
import os
import struct
import sys
import tempfile
import time
import types
import threading
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.hashes = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value

    def delete(self, *keys):
        for key in keys:
            self.values.pop(key, None)
            self.hashes.pop(key, None)

    def hgetall(self, key):
        return dict(self.hashes.get(key) or {})

    def hset(self, key, field=None, value=None, mapping=None, **_kwargs):
        row = self.hashes.setdefault(key, {})
        if field is not None:
            row[field] = value
        row.update(mapping or {})

    def hdel(self, key, *fields):
        row = self.hashes.setdefault(key, {})
        for field in fields:
            row.pop(field, None)


def load_custom_music_core():
    helpers = types.ModuleType("helpers")
    helpers.redis_client = FakeRedis()
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    sys.modules["helpers"] = helpers

    path = Path(__file__).resolve().parents[1] / "cores" / "custom_music_core.py"
    spec = importlib.util.spec_from_file_location("custom_music_core_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CustomMusicCoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_custom_music_core()
        cls.helpers = sys.modules["helpers"]

    def setUp(self):
        # The core binds ``redis_client`` from ``helpers`` at import time, so
        # point both at a fresh store for every test.
        self.redis = FakeRedis()
        self.helpers.redis_client = self.redis
        self.core.redis_client = self.redis
        self.core._shutdown_stream_server()

    def tearDown(self):
        self.core._shutdown_stream_server()

    def save_settings(self, mapping):
        self.core._save_hash(self.redis, self.core.SETTINGS_KEY, mapping)

    # ---- namespace isolation from the upstream Music Core ----

    def test_redis_keys_do_not_collide_with_music_core(self):
        core = self.core
        self.assertEqual(core.SETTINGS_KEY, "custom_music_core_settings")
        self.assertEqual(core.RUNTIME_KEY, "custom_music_core:runtime")
        self.assertEqual(core.PERSON_LINKS_KEY, "custom_music_core:person_links")
        self.assertEqual(core.CATALOG_KEY, "custom_music_core:catalog:v1")
        self.assertEqual(core.PLAYER_KEY, "custom_music_core:player")
        self.assertEqual(core.HISTORY_KEY, "custom_music_core:history:v1")
        self.assertEqual(core.RECOMMENDATIONS_KEY, "custom_music_core:recommendations:v1")
        self.assertEqual(core.PROMPT_PROFILE_KEY, "custom_music_core:profile:v1")
        self.assertEqual(core.ACTIVITY_KEY, "custom_music_core:activity_feed")
        for key in (core.SETTINGS_KEY, core.RUNTIME_KEY, core.CATALOG_KEY, core.PLAYER_KEY):
            self.assertFalse(key.startswith("music_core"), key)

    def test_settings_category_and_tab_are_distinct(self):
        core = self.core
        self.assertEqual(core.CORE_SETTINGS["category"], "Custom Music Core Settings")
        self.assertEqual(core.CORE_WEBUI_TAB["label"], "Custom Music")

    def test_provider_ids_never_reuse_tater_tube(self):
        core = self.core
        self.assertEqual(core.PROVIDER_LABELS, {"emby": "Emby", "network_share": "Network Share"})
        self.assertEqual(core.CATALOG_PROVIDER_IDS, {"emby", "network_share"})
        self.assertNotIn("tater_tube", core.PROVIDER_LABELS)
        self.assertNotIn("tater_tube", core.CATALOG_PROVIDER_IDS)

    def test_hydra_tool_ids_are_namespaced(self):
        ids = [row["id"] for row in self.core.get_hydra_kernel_tools()]
        self.assertEqual(
            ids,
            [
                "custom_music_play",
                "custom_music_search",
                "custom_music_control",
                "custom_music_now_playing",
                "custom_music_move",
                "custom_music_confirm",
                "custom_music_browse",
            ],
        )
        for tool_id in ids:
            self.assertNotIn("tater_tube", tool_id)

    # ---- provider id + track normalization ----

    def test_provider_id_normalization(self):
        self.assertEqual(self.core._provider_id("emby"), "emby")
        self.assertEqual(self.core._provider_id("network-share"), "network_share")
        self.assertEqual(self.core._provider_id("smb"), "network_share")
        self.assertEqual(self.core._provider_id(""), "emby")
        self.assertEqual(self.core._provider_id("tater_tube"), "emby")

    def test_normalize_emby_track(self):
        row = {
            "Id": "abc123",
            "Name": "Track One",
            "Artists": ["Artist A", "Artist B"],
            "AlbumArtist": "Artist A",
            "Album": "Album X",
            "Genres": ["Rock", "Indie"],
            "RunTimeTicks": 215_000_000,
            "ProductionYear": 2020,
            "IndexNumber": 3,
            "ParentIndexNumber": 1,
            "Container": "mp3",
            "Path": "/music/a.mp3",
            "ImageTags": {"Primary": "tag123"},
        }
        track = self.core._normalize_track(row)
        self.assertEqual(track["id"], "abc123")
        self.assertEqual(track["provider"], "emby")
        self.assertEqual(track["artist"], "Artist A, Artist B")
        self.assertEqual(track["album_artist"], "Artist A")
        self.assertEqual(track["duration_seconds"], 21.5)
        self.assertEqual(track["duration_display"], "0:21")
        self.assertEqual(track["track_number"], 3)
        self.assertEqual(track["disc_number"], 1)
        self.assertEqual(track["year"], "2020")
        self.assertTrue(track["has_artwork"])
        self.assertEqual(track["artwork_item_id"], "abc123")
        self.assertEqual(track["artwork_version"], "tag123")

    # ---- Emby stream and artwork URLs ----

    def test_api_key_mode_builds_direct_urls(self):
        provider = self.core.EmbyMusicProvider(
            server_url="http://emby.local:8096",
            auth_mode="api_key",
            api_key="K",
            user_id="u1",
        )
        self.assertTrue(provider.connected)
        self.assertEqual(
            provider.stream_url({"provider_track_id": "abc123"}),
            "http://emby.local:8096/Audio/abc123/stream?api_key=K&Static=true",
        )
        sync_url = provider.stream_url({"provider_track_id": "abc123"}, audio_sync=True)
        self.assertIn("AudioCodec=wav", sync_url)
        self.assertNotIn("Static", sync_url)
        artwork = provider.artwork_url(
            {"artwork_item_id": "abc123", "artwork_version": "tag123"}
        )
        self.assertTrue(artwork.startswith("http://emby.local:8096/Items/abc123/Images/Primary?"))
        self.assertIn("tag=tag123", artwork)

    def test_user_token_mode_routes_through_the_stream_proxy(self):
        self.save_settings(
            {"stream_token": "tok123", "stream_host": "192.168.1.10", "stream_bind_port": "8621"}
        )
        provider = self.core.EmbyMusicProvider(
            server_url="http://emby.local:8096",
            auth_mode="user_token",
            username="u",
            password="p",
        )
        self.assertTrue(provider.connected)
        self.assertEqual(
            provider.stream_url({"provider_track_id": "abc123"}),
            "http://192.168.1.10:8621/stream/tok123/emby/abc123",
        )
        self.assertIn(
            "/stream/tok123/emby_art/abc123",
            provider.artwork_url({"artwork_item_id": "abc123"}),
        )

    def test_disconnected_provider_is_reported(self):
        provider = self.core.EmbyMusicProvider(server_url="http://emby.local:8096")
        self.assertFalse(provider.connected)
        self.save_settings({})
        self.assertFalse(self.core._paired())

    # ---- catalog sync, search, history ----

    def test_catalog_sync_search_and_history(self):
        rows = [
            {
                "Id": f"id{i}",
                "Name": f"Song {i}",
                "Artists": ["Bob Marley"],
                "AlbumArtist": "Bob Marley",
                "Album": "Exodus",
                "Genres": ["Reggae"],
                "RunTimeTicks": 200_000_000 + i,
                "ProductionYear": 1977,
                "IndexNumber": i + 1,
                "Container": "mp3",
                "ImageTags": {"Primary": f"tag{i}"},
            }
            for i in range(5)
        ]

        class FakeEmbyProvider:
            provider_id = "emby"

            def __init__(self):
                self.connected = True

            def catalog(self):
                return {
                    "catalog_id": "view1",
                    "tracks": [dict(row) for row in rows],
                    "total": len(rows),
                    "libraries": {"view1": "Music"},
                }

        original_provider = self.core._provider
        self.core._provider = lambda client=None, provider_id="", person_id="": FakeEmbyProvider()
        try:
            payload = self.core._sync_catalog()
            self.assertEqual(payload["provider"], "emby")
            self.assertEqual(len(payload["tracks"]), 5)
            self.assertIn("Bob Marley", payload["artists"])
            self.assertIn("Exodus", payload["albums"])
            self.assertIn("Reggae", payload["genres"])
            self.assertEqual(self.core._runtime()["status"], "connected")

            catalog = self.core._catalog()
            self.assertEqual(len(catalog["tracks"]), 5)
            self.assertEqual(self.core._catalog(provider_id="network_share"), {})

            hits = self.core._search_tracks(query="bob marley")
            self.assertEqual(len(hits), 5)
            self.assertEqual(len(self.core._search_tracks(album="exodus", limit=2)), 2)

            artwork_url = self.core._public_track(hits[0])["artwork_url"]
            self.assertIn("/api/cores/custom_music_core/webhook/artwork?", artwork_url)

            track = dict(hits[0])
            self.core._record_listening_history(
                track, ["voice_core:native:kitchen"], person_id="person_abc"
            )
            # History is stored per person under a scoped key.
            history = self.core._listening_history(person_id="person_abc")
            self.assertEqual(self.core._listening_history(), [])
            self.assertEqual(len(history), 1)
            self.assertEqual(history[0]["person_id"], "person_abc")
            self.assertEqual(history[0]["provider"], "emby")
            activity = self.core.get_music_activity_events()
            self.assertEqual(len(activity), 1)
            self.assertEqual(activity[0]["source"], "custom_music_core")
            # The same track within the dedupe window is not recorded twice.
            self.core._record_listening_history(
                track, ["voice_core:native:kitchen"], person_id="person_abc"
            )
            self.assertEqual(len(self.core._listening_history(person_id="person_abc")), 1)
        finally:
            self.core._provider = original_provider

    # ---- stream server proxy (end to end) ----

    def test_stream_server_proxies_with_range_and_token_auth(self):
        payload = bytes(range(256)) * 3200

        class FakeEmby(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                if self.headers.get("Range") == "bytes=100-199":
                    self.send_response(206)
                    self.send_header("Content-Type", "audio/mpeg")
                    self.send_header("Content-Range", f"bytes 100-199/{len(payload)}")
                    self.send_header("Content-Length", "100")
                    self.end_headers()
                    self.wfile.write(payload[100:200])
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "audio/mpeg")
                    self.send_header("Content-Length", str(len(payload)))
                    self.send_header("Accept-Ranges", "bytes")
                    self.end_headers()
                    self.wfile.write(payload)

        upstream = HTTPServer(("127.0.0.1", 0), FakeEmby)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        self.save_settings(
            {
                "emby_server_url": f"http://127.0.0.1:{upstream.server_address[1]}",
                "emby_auth_mode": "api_key",
                "emby_api_key": "K",
                "stream_token": "sekret",
                "stream_host": "127.0.0.1",
                "stream_bind_port": "8891",
            }
        )
        try:
            status = self.core._ensure_stream_server()
            self.assertTrue(status["ok"], status)
            base = "http://127.0.0.1:8891/stream/sekret"

            with urllib.request.urlopen(f"{base}/emby/item1", timeout=10) as response:
                self.assertEqual(response.read(), payload)

            request = urllib.request.Request(
                f"{base}/emby/item1", headers={"Range": "bytes=100-199"}
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                self.assertEqual(response.status, 206)
                self.assertEqual(response.read(), payload[100:200])

            with self.assertRaises(Exception) as ctx:
                urllib.request.urlopen(
                    "http://127.0.0.1:8891/stream/wrongtok/emby/item1", timeout=10
                )
            self.assertIn("403", str(ctx.exception))
        finally:
            upstream.shutdown()

    def test_stream_server_rejects_unknown_routes(self):
        self.save_settings(
            {"stream_token": "tok", "stream_host": "127.0.0.1", "stream_bind_port": "8892"}
        )
        status = self.core._ensure_stream_server()
        self.assertTrue(status["ok"], status)
        with self.assertRaises(Exception) as ctx:
            urllib.request.urlopen("http://127.0.0.1:8892/stream/tok/bogus/x", timeout=10)
        self.assertIn("404", str(ctx.exception))

    # ---- settings manager UI ----

    def test_htmlui_tab_data_builds_with_provider_card(self):
        class FakeEmbyProvider:
            provider_id = "emby"
            connected = True

            def catalog(self):
                return {"tracks": [], "artists": [], "albums": [], "genres": []}

        original_provider = self.core._provider
        self.core._provider = lambda client=None, provider_id="": FakeEmbyProvider()
        try:
            data = self.core.get_htmlui_tab_data()
            self.assertEqual(data["ui"]["title"], "Custom Music Core")
            tab_keys = [tab["key"] for tab in data["ui"]["manager_tabs"]]
            self.assertIn("providers", tab_keys)
            self.assertIn("settings", tab_keys)
            item_ids = [item.get("id") for item in data["ui"]["item_forms"]]
            self.assertIn("provider:emby", item_ids)
        finally:
            self.core._provider = original_provider

    def test_core_system_tasks_shape(self):
        tasks = self.core.get_core_system_tasks()
        self.assertEqual(tasks["label"], "Custom Music Core")
        self.assertEqual(
            [task["id"] for task in tasks["tasks"]],
            [
                "catalog_sync",
                "recommendation_refresh",
                "music_profile_refresh",
                "continuous_radio_refill",
                "follow_me",
            ],
        )


# --------------------------------------------------------------------------
# Hand-crafted audio fixtures (no third-party tag libraries available).
# --------------------------------------------------------------------------

JPEG_BYTES = b"\xff\xd8\xff\xe0fakejpegdata" + b"\x00" * 32
PNG_BYTES = b"\x89PNG\r\n\x1a\nfakepngdata" + b"\x00" * 32


def _id3_text_frame(frame_id: str, text: str) -> bytes:
    body = b"\x03" + text.encode("utf-8")  # encoding 3 = UTF-8
    return frame_id.encode("ascii") + struct.pack(">I", len(body)) + b"\x00\x00" + body


def _id3_pic_frame(data: bytes, mime: str) -> bytes:
    body = (
        b"\x00"  # latin-1
        + mime.encode("ascii")
        + b"\x00\x03\x00"  # picture type 3, empty description
        + data
    )
    return b"APIC" + struct.pack(">I", len(body)) + b"\x00\x00" + body


def _write_mp3(path, *, title="Tagged Song", artist="Share Artist", album="Share Album"):
    frames = (
        _id3_text_frame("TIT2", title)
        + _id3_text_frame("TPE1", artist)
        + _id3_text_frame("TALB", album)
        + _id3_text_frame("TPE2", artist)
        + _id3_text_frame("TCON", "Reggae")
        + _id3_text_frame("TRCK", "7/12")
        + _id3_text_frame("TPOS", "1/1")
        + _id3_text_frame("TYER", "1977")
        + _id3_pic_frame(JPEG_BYTES, "image/jpeg")
    )
    body = b"\x00" * 128  # stand-in for audio frames
    blob = b"ID3" + bytes([3, 0, 0]) + struct.pack(">I", 0)  # size patched below
    size = len(frames)
    blob += bytes(
        [
            (size >> 21) & 0x7F,
            (size >> 14) & 0x7F,
            (size >> 7) & 0x7F,
            size & 0x7F,
        ]
    )[:4] if size < (1 << 21) else b"\x00\x00\x00\x00"
    # Rebuild header with syncsafe size properly.
    blob = b"ID3" + bytes([3, 0, 0]) + bytes(
        [(size >> 21) & 0x7F, (size >> 14) & 0x7F, (size >> 7) & 0x7F, size & 0x7F]
    ) + frames
    path.write_bytes(blob + body + b"\x00" * 64)


def _flac_block(block_type: int, payload: bytes, *, last: bool = False) -> bytes:
    return bytes([(0x80 if last else 0) | block_type]) + len(payload).to_bytes(3, "big") + payload


def _vorbis_comment_payload(comments) -> bytes:
    vendor = b"tater-test"
    out = struct.pack("<I", len(vendor)) + vendor + struct.pack("<I", len(comments))
    for key, value in comments:
        entry = f"{key}={value}".encode("utf-8")
        out += struct.pack("<I", len(entry)) + entry
    return out


def _write_flac(path, *, with_picture=True):
    # STREAMINFO: 44100 Hz, 88200 samples => 2.0 s
    streaminfo = bytearray(34)
    rate_bits = 44100 << 2
    streaminfo[10:13] = rate_bits.to_bytes(3, "big")
    streaminfo[13] &= 0xF0
    streaminfo[14:18] = (88200).to_bytes(4, "big")
    blocks = _flac_block(0, bytes(streaminfo))
    blocks += _flac_block(
        4,
        _vorbis_comment_payload(
            [
                ("TITLE", "Flac Song"),
                ("ARTIST", "Flac Artist"),
                ("ALBUM", "Flac Album"),
                ("GENRE", "Jazz"),
                ("TRACKNUMBER", "3"),
                ("DATE", "1999"),
            ]
        ),
    )
    if with_picture:
        picture = (
            struct.pack(">I", 3)  # front cover
            + struct.pack(">I", len(b"image/png")) + b"image/png"
            + struct.pack(">I", 0)  # no description
            + struct.pack(">IIII", 500, 500, 24, 0)
            + struct.pack(">I", len(PNG_BYTES))
            + PNG_BYTES
        )
        blocks += _flac_block(6, picture, last=True)
    else:
        blocks = blocks[:-1] and blocks  # keep as-is; last flag only matters for parsers
    path.write_bytes(b"fLaC" + blocks + b"\x00" * 64)


def _ogg_page(packet: bytes, *, granule=0, seq=0, serial=1, first=False):
    segments = []
    remaining = len(packet)
    while remaining >= 255:
        segments.append(255)
        remaining -= 255
    segments.append(remaining)
    header = b"OggS" + bytes([0, 0]) + struct.pack("<q", granule)
    header += struct.pack("<I", serial) + struct.pack("<I", seq) + b"\x00\x00\x00\x00"
    header += bytes([len(segments)]) + bytes(segments)
    return header + packet


def _write_ogg(path):
    id_packet = b"\x01vorbis" + struct.pack("<I", 0) + bytes([2]) + struct.pack("<I", 44100)
    id_packet += struct.pack("<iii", 0, 128000, 0) + bytes([0, 1])
    comment_packet = b"\x03vorbis" + _vorbis_comment_payload(
        [("TITLE", "Ogg Song"), ("ARTIST", "Ogg Artist"), ("ALBUM", "Ogg Album")]
    )
    audio_packet = b"\x00" * 32
    path.write_bytes(
        _ogg_page(id_packet, seq=0)
        + _ogg_page(comment_packet, granule=88200, seq=1)
        + _ogg_page(audio_packet, granule=88200, seq=2)
    )


def _mp4_atom(kind: bytes, payload: bytes) -> bytes:
    return struct.pack(">I", len(payload) + 8) + kind + payload


def _mp4_data_atom(flags: int, payload: bytes) -> bytes:
    # "data" atoms carry 8 bytes of header (version/flags + locale) after the atom header.
    return struct.pack(">I", len(payload) + 16) + b"data" + struct.pack(">I", flags) + b"\x00\x00\x00\x00" + payload


def _write_m4a(path):
    mvhd = (
        struct.pack(">I", 0)  # version 0 + flags
        + struct.pack(">II", 0, 0)  # creation, modification
        + struct.pack(">II", 44100, 88200)  # timescale, duration
        + b"\x00" * 80
    )
    ilst = (
        _mp4_atom(b"\xa9nam", _mp4_data_atom(1, "M4a Song".encode()))
        + _mp4_atom(b"\xa9ART", _mp4_data_atom(1, "M4a Artist".encode()))
        + _mp4_atom(b"\xa9alb", _mp4_data_atom(1, "M4a Album".encode()))
        + _mp4_atom(b"trkn", _mp4_data_atom(0, b"\x00\x00\x00\x05\x00\x0c\x00\x00"))
        + _mp4_atom(b"covr", _mp4_data_atom(14, PNG_BYTES))
    )
    meta = struct.pack(">I", 0) + _mp4_atom(b"ilst", ilst)
    moov = _mp4_atom(b"mvhd", mvhd) + _mp4_atom(b"udta", _mp4_atom(b"meta", meta))
    blob = _mp4_atom(b"ftyp", b"M4A " + b"\x00" * 4) + _mp4_atom(b"moov", moov)
    path.write_bytes(blob + b"\x00" * 64)


def _write_wav(path, seconds=2.0):
    rate = 44100
    data_size = int(rate * seconds) * 4  # stereo 16-bit => 4 bytes per frame
    fmt = struct.pack("<HHIIHH", 1, 2, rate, rate * 4, 4, 16)
    blob = b"RIFF" + struct.pack("<I", 4 + 8 + len(fmt) + 8 + data_size) + b"WAVE"
    blob += b"fmt " + struct.pack("<I", len(fmt)) + fmt
    blob += b"data" + struct.pack("<I", data_size) + b"\x00" * data_size
    path.write_bytes(blob)


class ShareFixture:
    def __init__(self, root):
        self.root = Path(root)

    def write_library(self):
        artist = self.root / "Share Artist"
        album = artist / "Share Album"
        album.mkdir(parents=True)
        (album / "cover.jpg").write_bytes(JPEG_BYTES)
        _write_mp3(album / "01 - Tagged Song.mp3")
        _write_flac(album / "02 - flac song.flac")
        _write_ogg(album / "03 - ogg song.ogg")
        _write_wav(album / "05 - wav song.wav")
        # No folder image here, so the M4A's embedded cover is the only art source.
        embedded = self.root / "Embedded Artist" / "Embedded Album"
        embedded.mkdir(parents=True)
        _write_m4a(embedded / "04 - m4a song.m4a")
        fallback = self.root / "Untagged Artist" / "Untagged Album"
        fallback.mkdir(parents=True)
        (fallback / "06 - Untitled.wav").write_bytes(
            (album / "05 - wav song.wav").read_bytes()
        )
        # A file that must never escape the share root via the stream proxy.
        secret = self.root.parent / "outside-secret.wav"
        if not secret.exists():
            _write_wav(secret)
        return album


class NetworkShareProviderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_custom_music_core()

    def setUp(self):
        self.redis = FakeRedis()
        self.core.redis_client = self.redis
        self.core._shutdown_stream_server()
        self._tmp = tempfile.TemporaryDirectory()
        self.share_root = os.path.join(self._tmp.name, "music")
        os.makedirs(self.share_root, exist_ok=True)
        self.album = ShareFixture(self.share_root).write_library()

    def tearDown(self):
        self.core._shutdown_stream_server()
        self._tmp.cleanup()

    def connect_share(self, port=8895):
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={
                "share_root_path": self.share_root,
                "provider": "network_share",
                "stream_token": "sharetok",
                "stream_host": "127.0.0.1",
                "stream_bind_port": str(port),
            },
        )

    def catalog(self):
        return self.core._sync_catalog(provider_id="network_share")

    def test_tag_readers(self):
        read = self.core._share_read_tags
        mp3 = read(str(self.album / "01 - Tagged Song.mp3"))
        self.assertEqual(mp3["title"], "Tagged Song")
        self.assertEqual(mp3["artist"], "Share Artist")
        self.assertEqual(mp3["album"], "Share Album")
        self.assertEqual(mp3["genre"], "Reggae")
        self.assertEqual(mp3["track"], "7/12")
        self.assertEqual(mp3["disc"], "1/1")
        self.assertEqual(mp3["year"], "1977")
        self.assertEqual(mp3["picture"]["mime"], "jpg")
        self.assertTrue(mp3["picture"]["data"].startswith(b"\xff\xd8"))

        flac = read(str(self.album / "02 - flac song.flac"))
        self.assertEqual(flac["title"], "Flac Song")
        self.assertEqual(flac["artist"], "Flac Artist")
        self.assertEqual(flac["duration"], 2.0)
        self.assertEqual(flac["year"], "1999")
        self.assertEqual(flac["picture"]["mime"], "png")

        ogg = read(str(self.album / "03 - ogg song.ogg"))
        self.assertEqual(ogg["title"], "Ogg Song")
        self.assertEqual(ogg["artist"], "Ogg Artist")
        self.assertAlmostEqual(ogg["duration"], 2.0, places=2)

        m4a = read(
            str(
                Path(self.share_root)
                / "Embedded Artist"
                / "Embedded Album"
                / "04 - m4a song.m4a"
            )
        )
        self.assertEqual(m4a["title"], "M4a Song")
        self.assertEqual(m4a["artist"], "M4a Artist")
        self.assertEqual(m4a["album"], "M4a Album")
        self.assertEqual(m4a["track"], "5")
        self.assertEqual(m4a["duration"], 2.0)
        self.assertEqual(m4a["picture"]["mime"], "png")

        wav = read(str(self.album / "05 - wav song.wav"))
        self.assertEqual(wav["title"], "wav song")
        self.assertAlmostEqual(wav["duration"], 2.0, places=2)

    def test_catalog_sync_indexes_share_files(self):
        self.connect_share()
        payload = self.catalog()
        self.assertEqual(payload["provider"], "network_share")
        self.assertEqual(len(payload["tracks"]), 6)
        titles = {track["title"] for track in payload["tracks"]}
        self.assertIn("Tagged Song", titles)
        self.assertIn("Flac Song", titles)
        self.assertIn("Ogg Song", titles)
        self.assertIn("M4a Song", titles)
        self.assertIn("Untitled", titles)  # filename fallback
        self.assertIn("Share Artist", payload["artists"])
        self.assertIn("Reggae", payload["genres"])
        self.assertEqual(self.core._runtime()["provider"], "network_share")
        # Tracks in the album folder resolve artwork: the folder image for the
        # untagged WAV, extracted cache art for embedded tags.
        by_title = {track["title"]: track for track in payload["tracks"]}
        for title in ("Tagged Song", "Flac Song", "Ogg Song", "M4a Song"):
            self.assertTrue(by_title[title]["has_artwork"], title)
        self.assertFalse(by_title["Untitled"]["has_artwork"])  # no art source at all

    def test_stream_proxy_serves_share_files_with_range(self):
        self.connect_share()
        self.catalog()
        status = self.core._ensure_stream_server()
        self.assertTrue(status["ok"], status)
        base = "http://127.0.0.1:8895/stream/sharetok"
        mp3_path = self.album / "01 - Tagged Song.mp3"
        rel = os.path.relpath(mp3_path, self.share_root)
        stream_id = self.core._share_stream_id(rel)
        url = f"{base}/share/{stream_id}"

        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["Content-Type"], "audio/mpeg")
            self.assertEqual(body, mp3_path.read_bytes())

        request = urllib.request.Request(url, headers={"Range": "bytes=5-15"})
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.status, 206)
            self.assertEqual(response.read(), mp3_path.read_bytes()[5:16])

        # Path traversal must never escape the share root.
        secret_rel = os.path.relpath(
            Path(self.share_root).parent / "outside-secret.wav", self.share_root
        )
        traversal_id = self.core._share_stream_id("../" + secret_rel)
        with self.assertRaises(Exception) as ctx:
            urllib.request.urlopen(f"{base}/share/{traversal_id}", timeout=10)
        self.assertIn("404", str(ctx.exception))

        # Cached artwork serves through share_art: the FLAC picks up the album's
        # folder image, the M4A's embedded cover was extracted at sync time.
        catalog_tracks = self.core._catalog(provider_id="network_share")["tracks"]
        flac = next(row for row in catalog_tracks if row["title"] == "Flac Song")
        m4a = next(row for row in catalog_tracks if row["title"] == "M4a Song")
        provider = self.core.NetworkShareMusicProvider(root_path=self.share_root)
        with urllib.request.urlopen(provider.artwork_url(flac), timeout=10) as response:
            self.assertEqual(response.read(), JPEG_BYTES)
        art_url = provider.artwork_url(m4a)
        self.assertIn("/share_art/", art_url)
        with urllib.request.urlopen(art_url, timeout=10) as response:
            self.assertEqual(response.read(), PNG_BYTES)

    def test_em_disconnect_removes_share_state(self):
        self.connect_share()
        self.catalog()
        result = self.core._disconnect_provider("network_share", self.redis)
        self.assertTrue(result["ok"])
        self.assertNotIn("share_root_path", self.redis.hgetall(self.core.SETTINGS_KEY))
        self.assertEqual(self.core._catalog(provider_id="network_share"), {})


class PerPersonLinkageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_custom_music_core()

    def setUp(self):
        self.redis = FakeRedis()
        self.core.redis_client = self.redis
        self.core._shutdown_stream_server()
        self._tmp = tempfile.TemporaryDirectory()
        self.share_root = os.path.join(self._tmp.name, "music")
        os.makedirs(self.share_root, exist_ok=True)
        ShareFixture(self.share_root).write_library()

    def tearDown(self):
        self.core._shutdown_stream_server()
        self._tmp.cleanup()

    def link_person(self, person_id="person_zoe", root=None):
        self.redis.hset(
            self.core.PERSON_LINKS_KEY,
            mapping={
                person_id: json.dumps(
                    {
                        "music_source": "network_share",
                        "network_share": {"root_path": root or self.share_root},
                    }
                )
            },
        )

    def test_person_link_storage_round_trip(self):
        self.link_person()
        link = self.core._person_link("person_zoe", self.redis)
        self.assertEqual(self.core._person_link_source(link), "network_share")
        self.assertEqual(self.core._linked_person_ids(self.redis), ["person_zoe"])
        self.assertEqual(self.core._linked_person_ids(self.redis), ["person_zoe"])
        self.core._delete_person_link("person_zoe", self.redis)
        self.assertEqual(self.core._linked_person_ids(self.redis), [])

    def test_person_link_overrides_provider_and_catalog_key(self):
        self.link_person()
        provider = self.core._provider(self.redis, "network_share", "person_zoe")
        self.assertIsInstance(provider, self.core.NetworkShareMusicProvider)
        self.assertEqual(provider.root_path, self.share_root)
        # A different provider request for that person does not get the link.
        self.assertEqual(self.core._provider(self.redis, "emby", "person_zoe").provider_id, "emby")
        # Without a link the global settings decide.
        self.assertEqual(self.core._person_source_id("", self.redis), "emby")
        self.assertEqual(self.core._person_source_id("person_zoe", self.redis), "network_share")

    def test_person_scoped_catalog_and_history(self):
        self.link_person()
        payload = self.core._sync_catalog(provider_id="network_share", person_id="person_zoe")
        self.assertEqual(payload["provider"], "network_share")
        self.assertEqual(len(payload["tracks"]), 6)
        # Global catalog stays empty; both live under distinct scoped keys.
        self.assertEqual(self.core._catalog(), {})
        self.assertEqual(len(self.core._catalog(person_id="person_zoe")["tracks"]), 6)
        self.assertEqual(
            self.core._catalog_key("person_zoe"),
            "custom_music_core:catalog:v1:person_zoe",
        )
        track = dict(payload["tracks"][0])
        self.core._record_listening_history(
            track, ["voice_core:native:kitchen"], person_id="person_zoe", client=self.redis
        )
        self.assertEqual(len(self.core._listening_history(self.redis, "person_zoe")), 1)
        self.assertEqual(self.core._listening_history(self.redis), [])

    def test_link_save_and_remove_actions(self):
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        self.core._PEOPLE_API_MODULE = people
        try:
            result = self.core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "network_share",
                    "person_link_share_root_path": self.share_root,
                },
                self.redis,
            )
            self.assertTrue(result["ok"])
            self.assertIn("6", result["message"])  # track count from the sync
            link = self.core._person_link("person_zoe", self.redis)
            self.assertEqual(link["network_share"]["root_path"], self.share_root)

            # Blank password keeps the saved one; emby link shape is right.
            self.core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "emby",
                    "person_link_emby_server_url": "http://emby.local:8096",
                    "person_link_emby_username": "zoe",
                    "person_link_emby_password": "pw",
                },
                self.redis,
            )
            link = self.core._person_link("person_zoe", self.redis)
            self.assertEqual(link["music_source"], "emby")
            self.assertEqual(link["emby"]["password"], "pw")
            self.core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "emby",
                    "person_link_emby_server_url": "http://emby.local:8096",
                    "person_link_emby_username": "zoe",
                },
                self.redis,
            )
            link = self.core._person_link("person_zoe", self.redis)
            self.assertEqual(link["emby"]["password"], "pw")

            # Remove clears the link and every scoped data key.
            self.redis.set(self.core._catalog_key("person_zoe"), "{}")
            self.redis.set(self.core._history_key("person_zoe"), "[]")
            result = self.core.handle_htmlui_tab_action(
                action="music_person_link_remove",
                payload={"values": {"person_link_person_id": "person_zoe"}},
                redis_client=self.redis,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(self.core._linked_person_ids(self.redis), [])
            self.assertIsNone(self.redis.get(self.core._catalog_key("person_zoe")))
            self.assertIsNone(self.redis.get(self.core._history_key("person_zoe")))
        finally:
            self.core._PEOPLE_API_MODULE = original_people

    def test_people_section_appears_in_tab_data(self):
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [
                    {"id": "person_zoe", "display_name": "Zoe"},
                    {"id": "person_ama", "display_name": "Ama"},
                ]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        self.core._PEOPLE_API_MODULE = people
        try:
            self.link_person()
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            tab_keys = [tab["key"] for tab in data["ui"]["manager_tabs"]]
            self.assertIn("people", tab_keys)
            item_ids = [item.get("id") for item in data["ui"]["item_forms"]]
            self.assertIn("person:person_zoe", item_ids)
            self.assertIn("person:new", item_ids)  # Ama is still unlinked
        finally:
            self.core._PEOPLE_API_MODULE = original_people


    def test_person_scoped_emby_proxy_routes(self):
        self.redis.hset(
            self.core.PERSON_LINKS_KEY,
            mapping={
                "person_lee": json.dumps(
                    {
                        "music_source": "emby",
                        "emby": {
                            "server_url": "http://emby.local:8096",
                            "auth_mode": "api_key",
                            "api_key": "LEEKEY",
                            "user_id": "u-lee",
                        },
                    }
                )
            },
        )
        provider = self.core._person_link_provider("person_lee", "emby", self.redis)
        self.assertEqual(provider.stream_scope, "person_lee")
        self.assertEqual(provider.api_key, "LEEKEY")
        # Person-scoped proxy routes resolve to the linked person's credentials.
        url, headers = self.core._emby_upstream_request("emby:person_lee", "song1")
        self.assertTrue(url.startswith("http://emby.local:8096/Audio/song1/stream?"))
        self.assertIn("api_key=LEEKEY", url)
        art_url, _headers = self.core._emby_upstream_request("emby_art:person_lee", "song1")
        self.assertTrue(art_url.startswith("http://emby.local:8096/Items/song1/Images/Primary?"))


def _track_row(number, title=None, duration=180.0):
    return {
        "id": f"track:{number}",
        "provider": "emby",
        "title": title or f"Song {number}",
        "artist": "Bob Marley",
        "album_artist": "Bob Marley",
        "album": "Exodus",
        "duration_seconds": duration,
    }


class MultiQueueTests(unittest.TestCase):
    """Per-person queues, room bindings, conflict handling, and follow-me."""

    @classmethod
    def setUpClass(cls):
        cls.core = load_custom_music_core()

    def setUp(self):
        self.redis = FakeRedis()
        self.core.redis_client = self.redis
        self.core._shutdown_stream_server()
        self.played = []
        self.stopped = []
        self._originals = {}

    def tearDown(self):
        for name, value in self._originals.items():
            setattr(self.core, name, value)
        self.core._shutdown_stream_server()

    def stub_playback(self):
        self._originals["_play_track"] = self.core._play_track

        def fake_play_track(track, targets, *, volume_percent, start_position_seconds=0.0, **_kwargs):
            self.played.append(
                {
                    "track_id": track.get("id"),
                    "targets": list(targets),
                    "start_position": float(start_position_seconds or 0.0),
                    "volume": volume_percent,
                }
            )
            return {"ok": True, "sent_count": len(targets), "voice_core_sessions": []}

        self.core._play_track = fake_play_track
        self._originals["_stop_target"] = self.core._stop_target

        def fake_stop_target(targets, *, expected_voice_core_sessions=None):
            self.stopped.append(list(targets))
            return []

        self.core._stop_target = fake_stop_target

    def make_queue(self, person_id, targets, *, index=0, position=0.0, duration=180.0, status="playing"):
        tracks = [_track_row(1, "Jamming", duration), _track_row(2, "Exodus", duration)]
        return self.core._create_and_start_queue(
            tracks,
            targets=targets,
            shuffle=False,
            volume_percent=60,
            person_id=person_id,
            client=self.redis,
        )

    def seed_playing_queue(self, person_id, targets, *, position=30.0, elapsed=10.0, duration=180.0):
        player = {
            "status": "playing",
            "provider": "emby",
            "queue": [_track_row(1, "Jamming", duration), _track_row(2, "Exodus", duration)],
            "queue_original": [_track_row(1, "Jamming", duration), _track_row(2, "Exodus", duration)],
            "index": 0,
            "current": _track_row(1, "Jamming", duration),
            "targets": targets,
            "person_id": person_id,
            "shuffle": False,
            "repeat": "off",
            "volume_percent": 60,
            "mixed_sync_adjustment_ms": 0,
            "created_at": time.time(),
            "queue_session_id": f"session-{person_id or 'shared'}",
            "continuous_radio": True,
            "continuation_pending": False,
            "radio_name": "Tater Continuous Radio",
            "started_at": time.time() - elapsed if position else 0.0,
            "position_offset_seconds": position,
            "duration_seconds": duration,
            "last_error": "",
        }
        self.core._save_player(player, self.redis, person_id)
        return player

    # ---- per-person queue state ----

    def test_player_keys_and_registry(self):
        core = self.core
        self.assertEqual(core._player_key(""), core.PLAYER_KEY)
        self.assertEqual(core._player_key("p1"), "custom_music_core:player:p1")
        self.stub_playback()
        self.make_queue("person_a", ["voice_core:native:kitchen"])
        registry = core._queue_registry(self.redis)
        self.assertIn("person_a", registry)
        self.assertEqual(
            core._player(self.redis, "person_a")["person_id"], "person_a"
        )
        # The shared queue slot is untouched by a Person queue.
        self.assertEqual(core._player(self.redis)["status"], "idle")

    def test_two_people_keep_separate_queues_and_timelines(self):
        self.stub_playback()
        alice = self.make_queue("person_a", ["voice_core:native:kitchen"])
        bob = self.make_queue("person_b", ["voice_core:native:office"])
        self.assertEqual(alice["queue_id"], "person_a")
        self.assertEqual(bob["queue_id"], "person_b")
        self.assertEqual(alice["targets"], ["voice_core:native:kitchen"])
        self.assertEqual(bob["targets"], ["voice_core:native:office"])
        # Advancing one queue does not touch the other.
        self.core._advance_player(1, person_id="person_a", client=self.redis)
        self.assertEqual(self.core._player(self.redis, "person_a")["index"], 1)
        self.assertEqual(self.core._player(self.redis, "person_b")["index"], 0)
        self.assertEqual(self.core._player(self.redis)["status"], "idle")

    def test_run_loop_advances_each_queue_independently(self):
        self.stub_playback()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=0.0, elapsed=0.0)
        bob = self.seed_playing_queue("person_b", ["voice_core:native:office"])
        # Bob's track finished; Alice's just started.
        bob["started_at"] = time.time() - 500.0
        self.core._save_player(bob, self.redis, "person_b")
        self.core._advance_finished_player(self.redis, person_id="person_b")
        self.assertEqual(self.core._player(self.redis, "person_b")["index"], 1)
        self.assertEqual(self.core._player(self.redis, "person_a")["index"], 0)

    # ---- room bindings ----

    def test_room_bindings_default_targets(self):
        core = self.core
        changed = core._set_room_bindings(["voice_core:native:kitchen"], "person_a", self.redis)
        self.assertEqual(changed, ["voice_core:native:kitchen"])
        self.assertEqual(core._bound_targets_for_person("person_a", self.redis), ["voice_core:native:kitchen"])
        self.assertEqual(core._bound_targets_for_person("person_b", self.redis), [])
        self.assertEqual(core._room_bindings(self.redis)["voice_core:native:kitchen"], "person_a")
        core._set_room_bindings(["voice_core:native:kitchen"], "", self.redis)
        self.assertEqual(core._bound_targets_for_person("person_a", self.redis), [])

    def test_resolve_targets_prefers_person_bindings(self):
        self._originals["_target_options"] = self.core._target_options

        def fake_target_options(*_args, **_kwargs):
            return [
                {"value": "voice_core:native:kitchen", "label": "Kitchen"},
                {"value": "voice_core:native:office", "label": "Office"},
            ]

        self.core._target_options = fake_target_options
        self.core._set_room_bindings(["voice_core:native:kitchen"], "person_a", self.redis)
        targets = self.core._resolve_targets(person_id="person_a", client=self.redis)
        self.assertEqual(targets, ["voice_core:native:kitchen"])
        # Without a binding (or for a different person) nothing is invented.
        self.assertEqual(self.core._resolve_targets(person_id="person_b", client=self.redis), [])

    # ---- conflict handling ----

    def test_conflict_mode_resolution(self):
        core = self.core
        self.assertEqual(core._queue_conflict_mode("", self.redis), "ask")
        self.core._save_hash(self.redis, core.SETTINGS_KEY, {"queue_conflict_mode": "auto_move"})
        self.assertEqual(core._queue_conflict_mode("person_a", self.redis), "auto_move")
        self.redis.hset(
            core.PERSON_LINKS_KEY,
            mapping={"person_a": json.dumps({"music_source": "", "queue_conflict_mode": "ask"})},
        )
        self.assertEqual(core._queue_conflict_mode("person_a", self.redis), "ask")

    def stub_play_request(self, targets):
        """Make _play_request resolvable offline with one canned match."""
        self._originals["_resolve_targets"] = self.core._resolve_targets
        self._originals["_search_tracks"] = self.core._search_tracks
        self._originals["_catalog"] = self.core._catalog
        self._originals["_sync_catalog"] = self.core._sync_catalog
        track = _track_row(9, "Requested Song")
        self.core._resolve_targets = (
            lambda requested="", room="", origin=None, client=None, provider_id="", person_id="": list(targets)
        )
        self.core._search_tracks = lambda **_kwargs: [track]
        payload = {"provider": "emby", "tracks": [track], "artists": [], "albums": [], "genres": []}
        self.core._catalog = lambda client=None, provider_id="", person_id="": payload
        self.core._sync_catalog = lambda client=None, provider_id="", person_id="": payload
        return track

    def origin_for(self, person_id, selector="kitchen"):
        return {
            "people_resolution": {"master_user_id": person_id},
            "satellite_selector": selector,
        }

    def test_ask_mode_asks_before_relocating_own_queue(self):
        core = self.core
        self.stub_playback()
        self.stub_play_request(["voice_core:native:office"])
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        result = core._play_request(
            {"query": "reggae", "targets": ["voice_core:native:office"]},
            self.origin_for("person_a"),
            self.redis,
        )
        self.assertTrue(result.get("needs_confirmation"))
        self.assertEqual(result.get("pending"), "relocate")
        self.assertIn("still playing", result.get("question", ""))
        pending = core._load_pending_confirmation(self.redis, "person_a")
        self.assertEqual(pending["type"], "relocate")
        self.assertEqual(pending["targets"], ["voice_core:native:office"])
        # Nothing was played or stopped by the question itself.
        self.assertEqual(self.played, [])
        self.assertEqual(self.stopped, [])

    def test_confirm_move_preserves_position_and_track(self):
        core = self.core
        self.stub_playback()
        self.stub_play_request(["voice_core:native:office"])
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        result = core._play_request(
            {"query": "reggae", "targets": ["voice_core:native:office"]},
            self.origin_for("person_a"),
            self.redis,
        )
        self.assertTrue(result.get("needs_confirmation"))
        confirmed = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="custom_music_confirm",
                args={"choice": "yes"},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(confirmed.get("ok"), confirmed)
        moved = core._player(self.redis, "person_a")
        self.assertEqual(moved["targets"], ["voice_core:native:office"])
        self.assertEqual(moved["index"], 0)
        self.assertEqual(moved["current"]["id"], "track:1")
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        self.assertEqual(core._load_pending_confirmation(self.redis, "person_a"), {})

    def test_confirm_start_new_replaces_own_queue(self):
        core = self.core
        self.stub_playback()
        self.stub_play_request(["voice_core:native:office"])
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        core._play_request(
            {"query": "reggae", "targets": ["voice_core:native:office"]},
            self.origin_for("person_a"),
            self.redis,
        )
        confirmed = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="custom_music_confirm",
                args={"choice": "start_new"},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(confirmed.get("ok"), confirmed)
        # The old rooms were stopped and the new request is playing.
        self.assertIn(["voice_core:native:kitchen"], self.stopped)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:native:office"])

    def test_ask_mode_asks_before_taking_over_another_queue(self):
        core = self.core
        self.stub_playback()
        self.stub_play_request(["voice_core:native:office"])
        self.seed_playing_queue("person_b", ["voice_core:native:office"])
        result = core._play_request(
            {"query": "reggae", "targets": ["voice_core:native:office"]},
            self.origin_for("person_a"),
            self.redis,
        )
        self.assertTrue(result.get("needs_confirmation"))
        self.assertEqual(result.get("pending"), "takeover")
        self.assertIn("Take over", result.get("question", ""))
        # Bob's stream was untouched by the question.
        self.assertEqual(self.core._player(self.redis, "person_b")["status"], "playing")

    def test_auto_move_takes_over_rooms_without_asking(self):
        core = self.core
        self.stub_playback()
        self.stub_play_request(["voice_core:native:office"])
        self.core._save_hash(self.redis, core.SETTINGS_KEY, {"queue_conflict_mode": "auto_move"})
        self.seed_playing_queue("person_b", ["voice_core:native:office", "voice_core:native:den"], position=30.0)
        result = core._play_request(
            {"query": "reggae", "targets": ["voice_core:native:office"]},
            self.origin_for("person_a"),
            self.redis,
        )
        self.assertTrue(result.get("ok"), result)
        # Bob's queue lost the office but kept the den, paused at its position.
        bob = core._player(self.redis, "person_b")
        self.assertEqual(bob["status"], "paused")
        self.assertEqual(bob["targets"], ["voice_core:native:den"])
        self.assertEqual(len(self.played), 1)
        self.assertEqual(self.played[0]["targets"], ["voice_core:native:office"])

    def test_control_actions_follow_the_room_queue(self):
        core = self.core
        self.stub_playback()
        self.seed_playing_queue("person_b", ["voice_core:native:kitchen"])
        self.seed_playing_queue("person_a", ["voice_core:native:office"])
        self._originals["_preferred_room_target"] = core._preferred_room_target
        core._preferred_room_target = lambda names, client=None: "voice_core:native:kitchen"
        try:
            result = asyncio.run(
                core.run_hydra_kernel_tool(
                    tool_id="custom_music_control",
                    args={"action": "pause"},
                    origin=self.origin_for("person_a", selector="native:kitchen"),
                    redis_client=self.redis,
                )
            )
            self.assertTrue(result.get("ok"), result)
            # Bob's queue (the room's music) paused; Alice's kept playing.
            self.assertEqual(core._player(self.redis, "person_b")["status"], "paused")
            self.assertEqual(core._player(self.redis, "person_a")["status"], "playing")
        finally:
            core._preferred_room_target = self._originals["_preferred_room_target"]

    def test_move_tool_hands_off_with_position(self):
        core = self.core
        self.stub_playback()
        self._originals["_resolve_targets"] = core._resolve_targets
        core._resolve_targets = (
            lambda requested="", room="", origin=None, client=None, provider_id="", person_id="": [
                "voice_core:native:office"
            ]
        )
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        result = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="custom_music_move",
                args={"rooms": ["Office"]},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(result.get("ok"), result)
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        moved = core._player(self.redis, "person_a")
        self.assertEqual(moved["targets"], ["voice_core:native:office"])
        self.assertEqual(moved["status"], "playing")

    def test_move_tool_requires_own_queue(self):
        core = self.core
        self.stub_playback()
        self._originals["_resolve_targets"] = core._resolve_targets
        core._resolve_targets = (
            lambda requested="", room="", origin=None, client=None, provider_id="", person_id="": [
                "voice_core:native:office"
            ]
        )
        result = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="custom_music_move",
                args={"rooms": ["Office"]},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertFalse(result.get("ok"))
        self.assertIn("no music playing", json.dumps(result))

    def test_pending_confirmations_expire(self):
        core = self.core
        payload = {"type": "relocate", "targets": ["a"]}
        core._save_pending_confirmation(self.redis, "person_a", payload)
        self.assertTrue(core._load_pending_confirmation(self.redis, "person_a"))
        stale = dict(payload)
        stale["expires_at"] = time.time() - 1.0
        core._save_json(self.redis, core._pending_confirmation_key("person_a"), stale)
        self.assertEqual(core._load_pending_confirmation(self.redis, "person_a"), {})

    def test_occupied_targets_and_release(self):
        core = self.core
        self.stub_playback()
        self.seed_playing_queue("person_b", ["voice_core:native:office", "voice_core:native:den"])
        occupied = core._occupied_targets(self.redis)
        self.assertEqual(occupied["voice_core:native:office"], "person_b")
        released = core._release_targets_to(
            self.redis, ["voice_core:native:office"], except_queue_id="person_a"
        )
        self.assertEqual(released, ["voice_core:native:office"])
        bob = core._player(self.redis, "person_b")
        # Bob keeps his other room, paused with the position intact.
        self.assertEqual(bob["status"], "paused")
        self.assertEqual(bob["targets"], ["voice_core:native:den"])
        self.assertAlmostEqual(bob["position_offset_seconds"], 40.0, delta=1.0)
        # A queue with no rooms left is stopped entirely.
        self.seed_playing_queue("person_c", ["voice_core:native:den"])
        core._release_targets_to(self.redis, ["voice_core:native:den"], except_queue_id="person_a")
        self.assertEqual(core._player(self.redis, "person_c")["status"], "stopped")


class FollowMeTests(unittest.TestCase):
    """Follow-Me presence: HA person tracking that moves music room to room."""

    @classmethod
    def setUpClass(cls):
        cls.core = load_custom_music_core()

    def setUp(self):
        self.redis = FakeRedis()
        self.core.redis_client = self.redis
        self.core._shutdown_stream_server()
        self.played = []
        self.stopped = []
        self.spoken = []
        self.ha_states = {}
        self.room_targets = {}
        self._originals = {}

    def tearDown(self):
        for name, value in self._originals.items():
            setattr(self.core, name, value)
        self.core._shutdown_stream_server()

    def stub_playback(self):
        self._originals["_play_track"] = self.core._play_track

        def fake_play_track(track, targets, *, volume_percent, start_position_seconds=0.0, **_kwargs):
            self.played.append(
                {
                    "track_id": track.get("id"),
                    "targets": list(targets),
                    "start_position": float(start_position_seconds or 0.0),
                    "volume": volume_percent,
                }
            )
            return {"ok": True, "sent_count": len(targets), "voice_core_sessions": []}

        self.core._play_track = fake_play_track
        self._originals["_stop_target"] = self.core._stop_target

        def fake_stop_target(targets, *, expected_voice_core_sessions=None):
            self.stopped.append(list(targets))
            return []

        self.core._stop_target = fake_stop_target

    def seed_playing_queue(self, person_id, targets, *, position=30.0, elapsed=10.0, duration=180.0):
        player = {
            "status": "playing",
            "provider": "emby",
            "queue": [_track_row(1, "Jamming", duration), _track_row(2, "Exodus", duration)],
            "queue_original": [_track_row(1, "Jamming", duration), _track_row(2, "Exodus", duration)],
            "index": 0,
            "current": _track_row(1, "Jamming", duration),
            "targets": targets,
            "person_id": person_id,
            "shuffle": False,
            "repeat": "off",
            "volume_percent": 60,
            "mixed_sync_adjustment_ms": 0,
            "created_at": time.time(),
            "queue_session_id": f"session-{person_id or 'shared'}",
            "continuous_radio": True,
            "continuation_pending": False,
            "radio_name": "Tater Continuous Radio",
            "started_at": time.time() - elapsed if position else 0.0,
            "position_offset_seconds": position,
            "duration_seconds": duration,
            "last_error": "",
        }
        self.core._save_player(player, self.redis, person_id)
        return player

    def stub_ha(self):
        """Point HA polling, room resolution, and TTS prompts at fakes."""
        core = self.core
        self._originals["_ha_person_location"] = core._ha_person_location
        core._ha_person_location = lambda client, entity: dict(
            self.ha_states.get(entity, {"state": "not_home"})
        )
        self._originals["_room_name_to_targets"] = core._room_name_to_targets
        core._room_name_to_targets = lambda name, client=None: self.room_targets.get(
            str(name), ""
        )
        self._originals["_speak_follow_me_prompt"] = core._speak_follow_me_prompt

        def fake_speak(targets, text):
            self.spoken.append({"targets": list(targets), "text": text})
            return True

        core._speak_follow_me_prompt = fake_speak
        # Tater's built-in HA integration settings (reused, never rewritten).
        self.redis.hset(
            core.HA_SETTINGS_KEY,
            mapping={"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "secret"},
        )

    def enable_follow_me(self, delay="0"):
        self.redis.hset(
            self.core.SETTINGS_KEY,
            mapping={"follow_me_enabled": "1", "follow_me_move_delay_seconds": delay},
        )

    def link_person(self, person_id, entity="person.john", **extra):
        link = {"music_source": ""}
        if entity:
            link["follow_me_person_entity"] = entity
        link.update(extra)
        self.redis.hset(self.core.PERSON_LINKS_KEY, mapping={person_id: json.dumps(link)})

    # ---- HA settings + REST polling ----

    def test_ha_config_reuses_tater_settings(self):
        core = self.core
        self.redis.hset(
            core.HA_SETTINGS_KEY,
            mapping={"HA_BASE_URL": "http://ha.local:8123/", "HA_TOKEN": "secret"},
        )
        self.assertEqual(
            core._ha_config(self.redis),
            {"base": "http://ha.local:8123", "token": "secret"},
        )
        # Without Tater's HA integration configured: default base, no token.
        self.assertEqual(
            core._ha_config(FakeRedis()),
            {"base": core.HA_DEFAULT_BASE_URL, "token": ""},
        )

    def test_ha_person_location_polls_and_maps_errors(self):
        core = self.core
        self.redis.hset(
            core.HA_SETTINGS_KEY,
            mapping={"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "secret"},
        )

        class Resp:
            def __init__(self, status, payload):
                self.status_code = status
                self._payload = payload

            def json(self):
                return self._payload

        seen = {}
        self._originals["_ha_http_get"] = core._ha_http_get

        def fake_get(url, headers, timeout):
            seen["url"] = url
            seen["headers"] = headers
            seen["timeout"] = timeout
            return Resp(200, {"state": "Office", "last_changed": 123.0})

        core._ha_http_get = fake_get
        try:
            self.assertEqual(
                core._ha_person_location(self.redis, "person.john"),
                {"state": "Office", "last_changed": 123.0},
            )
            self.assertEqual(seen["url"], "http://ha.local:8123/api/states/person.john")
            self.assertEqual(seen["headers"], {"Authorization": "Bearer secret"})
            self.assertEqual(seen["timeout"], core.HA_STATE_TIMEOUT_SECONDS)
            for status, code in (
                (404, "entity_not_found"),
                (401, "ha_unauthorized"),
                (403, "ha_unauthorized"),
                (500, "ha_http_500"),
            ):
                core._ha_http_get = lambda *a, **k: Resp(status, {})
                self.assertEqual(core._ha_person_location(self.redis, "person.john"), {"error": code})

            def boom(*_args, **_kwargs):
                raise RuntimeError("connection refused")

            core._ha_http_get = boom
            self.assertEqual(
                core._ha_person_location(self.redis, "person.john"),
                {"error": "connection refused"},
            )
        finally:
            core._ha_http_get = self._originals["_ha_http_get"]
        # No HA token in Tater settings -> not configured, no request at all.
        self.assertEqual(
            core._ha_person_location(FakeRedis(), "person.john"),
            {"error": "ha_not_configured"},
        )

    # ---- mode + zone resolution ----

    def test_room_overrides_and_zone_resolution(self):
        core = self.core
        self.assertEqual(
            core._parse_room_overrides("The Kitchen=Kitchen, Guest Room=Beds"),
            {"thekitchen": "Kitchen", "guestroom": "Beds"},
        )
        self.assertEqual(core._parse_room_overrides("no-equals-pair"), {})
        self.assertEqual(core._resolve_follow_me_zone("not_home", {}, self.redis), ("", ""))
        self.assertEqual(core._resolve_follow_me_zone("Home", {}, self.redis), ("", ""))
        self.assertEqual(core._resolve_follow_me_zone("", {"follow_me_room_overrides": "A=B"}, self.redis), ("", ""))
        self._originals["_room_name_to_targets"] = core._room_name_to_targets
        core._room_name_to_targets = lambda name, client=None: {
            "Kitchen": "voice_core:native:kitchen",
            "Beds": "voice_core:native:beds",
        }.get(str(name), "")
        try:
            self.assertEqual(
                core._resolve_follow_me_zone(
                    "The Kitchen",
                    {"follow_me_room_overrides": "The Kitchen=Kitchen"},
                    self.redis,
                ),
                ("Kitchen", "voice_core:native:kitchen"),
            )
            # No override: the zone name itself is used as the Tater room name.
            self.assertEqual(core._resolve_follow_me_zone("Kitchen", {}, self.redis), ("Kitchen", "voice_core:native:kitchen"))
            # A zone with no matching Tater room resolves to nothing.
            self.assertEqual(core._resolve_follow_me_zone("Patio", {}, self.redis), ("Patio", ""))
        finally:
            core._room_name_to_targets = self._originals["_room_name_to_targets"]

    def test_takeover_and_away_mode_resolution(self):
        core = self.core
        self.assertEqual(core._follow_me_takeover_mode("person_a", self.redis), "auto")
        self.assertEqual(core._follow_me_away_action("person_a", self.redis), "keep_pause")
        self.redis.hset(
            core.SETTINGS_KEY,
            mapping={"follow_me_takeover_mode": "ask", "follow_me_away_action": "pause"},
        )
        self.assertEqual(core._follow_me_takeover_mode("person_a", self.redis), "ask")
        self.assertEqual(core._follow_me_away_action("person_a", self.redis), "pause")
        # Per-Person link overrides win over the global setting.
        self.redis.hset(
            core.PERSON_LINKS_KEY,
            mapping={
                "person_a": json.dumps(
                    {
                        "music_source": "",
                        "follow_me_takeover_mode": "auto",
                        "follow_me_away_action": "keep",
                    }
                )
            },
        )
        self.assertEqual(core._follow_me_takeover_mode("person_a", self.redis), "auto")
        self.assertEqual(core._follow_me_away_action("person_a", self.redis), "keep")
        # An invalid value falls through to the default.
        self.redis.hset(core.SETTINGS_KEY, mapping={"follow_me_takeover_mode": "yolo"})
        self.assertEqual(core._follow_me_takeover_mode("person_b", self.redis), "auto")

    # ---- the handoff primitive ----

    def test_follow_me_move_hands_off_at_the_same_position(self):
        core = self.core
        self.stub_playback()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        result = core._follow_me_move("person_a", "Office", ["voice_core:native:office"], self.redis)
        self.assertTrue(result["moved"], result)
        self.assertEqual(result["targets"], ["voice_core:native:office"])
        self.assertEqual(self.played[-1]["targets"], ["voice_core:native:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        self.assertEqual(core._player(self.redis, "person_a")["status"], "playing")
        # Nobody playing -> no move; no resolvable targets -> no move.
        self.assertEqual(
            core._follow_me_move("person_b", "Office", ["voice_core:native:office"], self.redis),
            {"moved": False, "reason": "no_playback"},
        )
        self.assertEqual(
            core._follow_me_move("person_a", "Office", [], self.redis),
            {"moved": False, "reason": "no_targets"},
        )

    # ---- tick: follow between rooms ----

    def test_follow_me_tick_moves_music_to_the_new_room(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="0")
        self.link_person("person_a", "person.john")
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        self.ha_states["person.john"] = {"state": "Office"}
        self.room_targets["Office"] = "voice_core:native:office"
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["checked"], 1)
        self.assertEqual(summary["moved"], 1)
        self.assertEqual(core._player(self.redis, "person_a")["targets"], ["voice_core:native:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        state = core._follow_me_state("person_a", self.redis)
        self.assertEqual(state["status"], "following")
        self.assertEqual(state["zone"], "Office")
        self.assertEqual(state["resolved_room"], "Office")
        runtime = core._runtime(self.redis)
        self.assertGreater(float(runtime.get("last_follow_me_at") or 0), 0)
        self.assertEqual(int(runtime.get("follow_me_run_count") or 0), 1)
        self.assertEqual(runtime.get("follow_me_last_error"), "")

    def test_follow_me_tick_waits_for_the_move_delay(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="30")
        self.link_person("person_a", "person.john")
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        self.ha_states["person.john"] = {"state": "Office"}
        self.room_targets["Office"] = "voice_core:native:office"
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["moved"], 0)
        self.assertEqual(core._follow_me_state("person_a", self.redis)["status"], "tracking")
        self.assertEqual(self.played, [])
        # The zone holds past the delay: the next pass moves.
        state = core._follow_me_state("person_a", self.redis)
        state["zone_since"] = time.time() - 31
        core._save_follow_me_state("person_a", state, self.redis)
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["moved"], 1)
        self.assertEqual(core._player(self.redis, "person_a")["targets"], ["voice_core:native:office"])

    # ---- tick: away + dead zones ----

    def test_follow_me_tick_pauses_away_and_resumes_on_return(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="0")
        self.link_person("person_a", "person.john")
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        # Default keep_pause action: leaving the home pauses, position kept.
        self.ha_states["person.john"] = {"state": "not_home"}
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["paused"], 1)
        self.assertEqual(core._player(self.redis, "person_a")["status"], "paused")
        state = core._follow_me_state("person_a", self.redis)
        self.assertTrue(state.get("paused_by_follow_me"))
        self.assertEqual(state["status"], "paused_away")
        # A second away pass does not re-pause.
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["paused"], 0)
        self.assertEqual(core._follow_me_state("person_a", self.redis)["status"], "away_idle")
        # Walked back into a speaker room: hand off and resume in place.
        self.ha_states["person.john"] = {"state": "Office"}
        self.room_targets["Office"] = "voice_core:native:office"
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["moved"], 1)
        player = core._player(self.redis, "person_a")
        self.assertEqual(player["status"], "playing")
        self.assertEqual(player["targets"], ["voice_core:native:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        state = core._follow_me_state("person_a", self.redis)
        self.assertNotIn("paused_by_follow_me", state)
        self.assertEqual(state["status"], "following")

    def test_follow_me_tick_away_keep_action_never_pauses(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="0")
        self.redis.hset(core.SETTINGS_KEY, mapping={"follow_me_away_action": "keep"})
        self.link_person("person_a", "person.john")
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        self.ha_states["person.john"] = {"state": "not_home"}
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["paused"], 0)
        self.assertEqual(core._player(self.redis, "person_a")["status"], "playing")
        self.assertEqual(core._follow_me_state("person_a", self.redis)["status"], "away_kept")

    def test_follow_me_tick_dead_zone(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="0")
        self.link_person("person_a", "person.john")
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        # Patio exists in HA but maps to no Tater room.
        self.ha_states["person.john"] = {"state": "Patio"}
        # keep_pause (default): stays playing, just noted as a dead zone.
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["paused"], 0)
        self.assertEqual(core._player(self.redis, "person_a")["status"], "playing")
        self.assertEqual(core._follow_me_state("person_a", self.redis)["status"], "dead_zone")
        # The pause action stops the music in dead zones.
        self.redis.hset(core.SETTINGS_KEY, mapping={"follow_me_away_action": "pause"})
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["paused"], 1)
        self.assertEqual(core._player(self.redis, "person_a")["status"], "paused")
        self.assertEqual(core._follow_me_state("person_a", self.redis)["status"], "paused_dead_zone")

    # ---- tick: taking over occupied rooms ----

    def test_follow_me_tick_asks_before_taking_over(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="0")
        self.redis.hset(core.SETTINGS_KEY, mapping={"follow_me_takeover_mode": "ask"})
        self.link_person("person_a", "person.john")
        self.link_person("person_b", entity="")  # linked, but no tracker
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        self.seed_playing_queue("person_b", ["voice_core:native:office"])
        self.ha_states["person.john"] = {"state": "Office"}
        self.room_targets["Office"] = "voice_core:native:office"
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["awaiting"], 1)
        self.assertEqual(summary["moved"], 0)
        pending = core._load_pending_confirmation(self.redis, "person_a")
        self.assertEqual(pending["type"], "follow_takeover")
        self.assertEqual(pending["targets"], ["voice_core:native:office"])
        self.assertEqual(core._follow_me_state("person_a", self.redis)["status"], "awaiting_confirmation")
        # Bob's stream is untouched; the question was voiced in the room.
        self.assertEqual(core._player(self.redis, "person_b")["status"], "playing")
        self.assertEqual(len(self.spoken), 1)
        self.assertEqual(self.spoken[0]["targets"], ["voice_core:native:office"])
        self.assertIn("yes", self.spoken[0]["text"])
        # A repeated pass does not re-ask the same question.
        core._follow_me_tick(self.redis)
        self.assertEqual(len(self.spoken), 1)
        # The Person's yes takes the room over and stops the other queue.
        confirmed = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="custom_music_confirm",
                args={"choice": "yes"},
                origin={"people_resolution": {"master_user_id": "person_a"}},
                redis_client=self.redis,
            )
        )
        self.assertTrue(confirmed.get("ok"), confirmed)
        alice = core._player(self.redis, "person_a")
        self.assertEqual(alice["targets"], ["voice_core:native:office"])
        self.assertEqual(alice["status"], "playing")
        self.assertEqual(core._player(self.redis, "person_b")["status"], "stopped")
        self.assertEqual(core._load_pending_confirmation(self.redis, "person_a"), {})
        # A declining yes/no clears the question without moving anything.
        self.seed_playing_queue("person_b", ["voice_core:native:office"])
        core._follow_me_tick(self.redis)
        declined = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="custom_music_confirm",
                args={"choice": "no"},
                origin={"people_resolution": {"master_user_id": "person_a"}},
                redis_client=self.redis,
            )
        )
        self.assertTrue(declined.get("ok"), declined)
        self.assertEqual(core._player(self.redis, "person_a")["targets"], ["voice_core:native:office"])
        self.assertEqual(core._player(self.redis, "person_b")["status"], "playing")

    def test_follow_me_tick_auto_takes_over(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="0")  # default takeover mode is auto
        self.link_person("person_a", "person.john")
        self.link_person("person_b", entity="")
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        self.seed_playing_queue("person_b", ["voice_core:native:office", "voice_core:native:den"], position=30.0)
        self.ha_states["person.john"] = {"state": "Office"}
        self.room_targets["Office"] = "voice_core:native:office"
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["moved"], 1)
        self.assertEqual(core._player(self.redis, "person_a")["targets"], ["voice_core:native:office"])
        # Bob lost the office but keeps the den, paused in place.
        bob = core._player(self.redis, "person_b")
        self.assertEqual(bob["status"], "paused")
        self.assertEqual(bob["targets"], ["voice_core:native:den"])

    # ---- tick: errors + skips ----

    def test_follow_me_tick_records_errors_without_moving(self):
        core = self.core
        self.stub_playback()
        self.stub_ha()
        self.enable_follow_me(delay="0")
        self.link_person("person_a", "person.john")
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        self.ha_states["person.john"] = {"error": "entity_not_found"}
        summary = core._follow_me_tick(self.redis)
        self.assertEqual(summary["checked"], 1)
        self.assertEqual(summary["moved"], 0)
        self.assertEqual(len(summary["errors"]), 1)
        self.assertIn("entity_not_found", summary["errors"][0])
        state = core._follow_me_state("person_a", self.redis)
        self.assertEqual(state["status"], "error")
        self.assertEqual(state["last_error"], "entity_not_found")
        # Nothing moved; the last error is surfaced to the system-task UI.
        self.assertEqual(self.played, [])
        self.assertEqual(core._player(self.redis, "person_a")["targets"], ["voice_core:native:kitchen"])
        self.assertEqual(core._runtime(self.redis).get("follow_me_last_error"), "")

    def test_follow_me_tick_skips_when_disabled_or_unconfigured(self):
        core = self.core
        # Off by default.
        self.assertEqual(core._follow_me_tick(self.redis).get("skipped"), "disabled")
        # On, but Tater has no Home Assistant integration configured.
        self.redis.hset(core.SETTINGS_KEY, mapping={"follow_me_enabled": "1"})
        self.assertEqual(core._follow_me_tick(self.redis).get("skipped"), "ha_not_configured")
        # Skipped passes never record a run.
        self.assertIsNone(core._runtime(self.redis).get("last_follow_me_at"))

    # ---- system task + manual run ----

    def test_follow_me_system_task(self):
        core = self.core
        tasks = core.get_core_system_tasks(redis_client=self.redis)
        self.assertEqual(
            [task["id"] for task in tasks["tasks"]],
            [
                "catalog_sync",
                "recommendation_refresh",
                "music_profile_refresh",
                "continuous_radio_refill",
                "follow_me",
            ],
        )
        follow_me_task = tasks["tasks"][-1]
        # No HA token yet: unavailable and waiting.
        self.assertFalse(follow_me_task["available"])
        self.assertEqual(follow_me_task["status"], "waiting")
        self.assertIn("Home Assistant", follow_me_task["unavailable_reason"])
        # With Tater's HA integration configured and the feature enabled, the
        # task is available and a manual run performs one presence pass.
        self.redis.hset(
            core.HA_SETTINGS_KEY,
            mapping={"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "secret"},
        )
        self.redis.hset(core.SETTINGS_KEY, mapping={"follow_me_enabled": "1"})
        follow_me_task = core.get_core_system_tasks(redis_client=self.redis)["tasks"][-1]
        self.assertTrue(follow_me_task["available"])
        self.assertEqual(follow_me_task["status"], "idle")
        self.assertEqual(
            core.run_core_system_task(task_id="follow_me", redis_client=self.redis),
            {"ok": True, "checked": 0, "moved": 0, "paused": 0, "awaiting": 0, "errors": []},
        )

    # ---- hydra prompt note ----

    def test_follow_me_pending_note_and_prompt_fragment(self):
        core = self.core
        origin = {"people_resolution": {"master_user_id": "person_a"}}
        self.assertEqual(core._follow_me_pending_note("person_a", self.redis), "")
        core._save_pending_confirmation(
            self.redis,
            "person_a",
            {
                "type": "follow_takeover",
                "targets": ["voice_core:native:office"],
                "queue_id": "person_a",
                "zone": "Office",
            },
        )
        note = core._follow_me_pending_note("person_a", self.redis)
        self.assertIn("custom_music_confirm", note)
        self.assertIn("'yes'", note)
        fragments = core.get_hydra_system_prompt_fragments(
            role="chat", redis_client=self.redis, origin=origin
        )
        self.assertTrue(any("custom_music_confirm" in m for m in fragments["chat"]))
        # No pending question -> no note (and no fragment without a profile).
        core._clear_pending_confirmation(self.redis, "person_a")
        self.assertEqual(core._follow_me_pending_note("person_a", self.redis), "")
        self.assertEqual(
            core.get_hydra_system_prompt_fragments(role="chat", redis_client=self.redis, origin=origin),
            {},
        )

    # ---- person card + link save ----

    def test_person_card_shows_follow_me_fields_and_status(self):
        core = self.core
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [
                    {"id": "person_zoe", "display_name": "Zoe"},
                    {"id": "person_ama", "display_name": "Ama"},
                ]
            }
        )
        original_people = core._PEOPLE_API_MODULE
        core._PEOPLE_API_MODULE = people
        try:
            self.redis.hset(core.SETTINGS_KEY, mapping={"follow_me_enabled": "1"})
            self.redis.hset(
                core.PERSON_LINKS_KEY,
                mapping={
                    "person_zoe": json.dumps(
                        {
                            "music_source": "",
                            "follow_me_person_entity": "person.zoe",
                            "follow_me_room_overrides": "The Kitchen=Kitchen",
                            "follow_me_takeover_mode": "ask",
                            "follow_me_away_action": "pause",
                        }
                    )
                },
            )
            core._save_follow_me_state(
                "person_zoe",
                {
                    "status": "following",
                    "zone": "Kitchen",
                    "resolved_room": "Kitchen",
                    "resolved_targets": ["voice_core:native:kitchen"],
                },
                self.redis,
            )
            cards = {item["id"]: item for item in core._person_link_items(core._settings(self.redis), self.redis)}
            self.assertIn("person:person_zoe", cards)
            self.assertIn("person:new", cards)
            fields = {field["key"]: field for field in cards["person:person_zoe"]["fields"]}
            self.assertEqual(fields["person_link_follow_me_entity"]["value"], "person.zoe")
            self.assertEqual(fields["person_link_follow_me_room_overrides"]["value"], "The Kitchen=Kitchen")
            self.assertEqual(fields["person_link_follow_me_takeover_mode"]["value"], "ask")
            self.assertEqual(fields["person_link_follow_me_away_action"]["value"], "pause")
            self.assertIn("Follow-me: in Kitchen → Kitchen", cards["person:person_zoe"]["subtitle"])
            # The new-link card carries the same fields, defaulted from settings.
            new_fields = {field["key"]: field for field in cards["person:new"]["fields"]}
            self.assertEqual(new_fields["person_link_follow_me_takeover_mode"]["value"], "auto")
            self.assertEqual(new_fields["person_link_follow_me_away_action"]["value"], "keep_pause")
            # Error states are surfaced with a friendly label.
            core._save_follow_me_state(
                "person_zoe",
                {"status": "error", "last_error": "entity_not_found"},
                self.redis,
            )
            cards = {item["id"]: item for item in core._person_link_items(core._settings(self.redis), self.redis)}
            self.assertIn(
                "Follow-me: person entity not found in Home Assistant",
                cards["person:person_zoe"]["subtitle"],
            )
        finally:
            core._PEOPLE_API_MODULE = original_people

    def test_link_save_persists_follow_me_fields(self):
        core = self.core
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        original_people = core._PEOPLE_API_MODULE
        core._PEOPLE_API_MODULE = people
        try:
            result = core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "",
                    "person_link_follow_me_entity": " person.zoe ",
                    "person_link_follow_me_room_overrides": " The Kitchen=Kitchen ",
                    "person_link_follow_me_takeover_mode": "ASK",
                    "person_link_follow_me_away_action": "pause",
                },
                self.redis,
            )
            self.assertTrue(result["ok"], result)
            link = core._person_link("person_zoe", self.redis)
            self.assertEqual(link["follow_me_person_entity"], "person.zoe")
            self.assertEqual(link["follow_me_room_overrides"], "The Kitchen=Kitchen")
            self.assertEqual(link["follow_me_takeover_mode"], "ask")
            self.assertEqual(link["follow_me_away_action"], "pause")
            # Blank entity clears tracking; invalid select values are dropped.
            result = core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "",
                    "person_link_follow_me_entity": "",
                    "person_link_follow_me_takeover_mode": "yolo",
                    "person_link_follow_me_away_action": "teleport",
                },
                self.redis,
            )
            self.assertTrue(result["ok"], result)
            link = core._person_link("person_zoe", self.redis)
            self.assertEqual(link.get("follow_me_person_entity"), "")
            self.assertEqual(link.get("follow_me_takeover_mode"), "ask")
            self.assertEqual(link.get("follow_me_away_action"), "pause")
        finally:
            core._PEOPLE_API_MODULE = original_people


class UpstreamCoexistenceTests(unittest.TestCase):
    """Both this core and the upstream Music Core enabled side by side."""

    @classmethod
    def setUpClass(cls):
        cls.core = load_custom_music_core()
        # Snapshot of Tater_Shop's cores/music_core.py this file was renamed
        # from (pre-divergence), kept so coexistence is testable offline.
        upstream_path = (
            Path(__file__).resolve().parent / "fixtures" / "upstream_music_core.py"
        )
        if not upstream_path.exists():
            raise unittest.SkipTest("upstream music_core.py snapshot not present")
        spec = importlib.util.spec_from_file_location("tater_upstream_music_core", upstream_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        cls.upstream = module

    def test_redis_keys_never_overlap(self):
        ours = {
            value
            for name, value in vars(self.core).items()
            if name.isupper() and isinstance(value, str) and "custom_music_core" in value
        }
        theirs = {
            value
            for name, value in vars(self.upstream).items()
            if name.isupper() and isinstance(value, str) and "music" in value
        }
        overlap = {
            key
            for key in ours & theirs
            if key.startswith(("music_core", "custom_music_core"))
        }
        self.assertEqual(overlap, set())

    def test_hydra_tool_ids_do_not_collide(self):
        # Hydra kernel tool ids are a global first-declared-wins namespace, so a
        # collision here would silently drop one core's tools.
        ours = {row["id"] for row in self.core.get_hydra_kernel_tools()}
        theirs = {row["id"] for row in self.upstream.get_hydra_kernel_tools()}
        self.assertEqual(ours & theirs, set())

    def test_settings_category_and_tab_labels_differ(self):
        self.assertNotEqual(
            self.core.CORE_SETTINGS["category"],
            self.upstream.CORE_SETTINGS["category"],
        )
        self.assertNotEqual(
            self.core.CORE_WEBUI_TAB["label"],
            self.upstream.CORE_WEBUI_TAB["label"],
        )

    def test_provider_namespaces_are_disjoint(self):
        self.assertEqual(
            self.core.PROVIDER_LABELS.keys() & self.upstream.PROVIDER_LABELS.keys(),
            set(),
        )
        self.assertEqual(
            self.core.CATALOG_PROVIDER_IDS & self.upstream.CATALOG_PROVIDER_IDS,
            set(),
        )

    def test_both_cores_sync_catalogs_into_the_same_store_without_interference(self):
        store = self.core.redis_client

        class FakeEmby:
            provider_id = "emby"
            connected = True

            def catalog(self):
                return {
                    "catalog_id": "v1",
                    "tracks": [
                        {
                            "Id": "song1",
                            "Name": "Emby Song",
                            "Artists": ["Emby Artist"],
                            "Album": "Emby Album",
                            "RunTimeTicks": 180_000_000,
                            "ImageTags": {"Primary": "t"},
                        }
                    ],
                    "total": 1,
                    "libraries": {"v1": "Music"},
                }

        class FakeTaterTube:
            connected = True

            def catalog(self):
                return {
                    "tracks": [
                        {
                            "id": "tt1",
                            "title": "Tube Song",
                            "artist": "Tube Artist",
                            "album": "Tube Album",
                            "duration_seconds": 120,
                        }
                    ],
                    "total": 1,
                }

        original_core_provider = self.core._provider
        original_upstream_provider = self.upstream._provider
        self.core._provider = lambda client=None, provider_id="", person_id="": FakeEmby()
        self.upstream._provider = lambda *args, **kwargs: FakeTaterTube()
        try:
            ours = self.core._sync_catalog()
            theirs = self.upstream._sync_catalog()
            self.assertEqual(ours["provider"], "emby")
            self.assertEqual(theirs["provider"], "tater_tube")
            # Each core reads back exactly its own library from the shared store.
            self.assertEqual(
                [track["title"] for track in self.core._catalog()["tracks"]],
                ["Emby Song"],
            )
            self.assertEqual(
                [track["title"] for track in self.upstream._catalog()["tracks"]],
                ["Tube Song"],
            )
            # The upstream core's history stays untouched by ours.
            self.core._record_listening_history(dict(ours["tracks"][0]), ["t1"], client=store)
            self.assertEqual(self.upstream._listening_history(), [])
        finally:
            self.core._provider = original_core_provider
            self.upstream._provider = original_upstream_provider


if __name__ == "__main__":
    unittest.main()