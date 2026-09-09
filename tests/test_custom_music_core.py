"""Tests for cores/custom_music_core.py.

Mirrors Tater_Shop's core test style: stub the shared ``helpers`` module with a
FakeRedis, load the core file directly via importlib, and exercise it without a
running Tater.
"""

import importlib.util
import os
import struct
import sys
import tempfile
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

    def hset(self, key, mapping=None, **_kwargs):
        self.hashes.setdefault(key, {}).update(mapping or {})

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
        self.core._provider = lambda client=None, provider_id="": FakeEmbyProvider()
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
            history = self.core._listening_history()
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
            self.assertEqual(len(self.core._listening_history()), 1)
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


if __name__ == "__main__":
    unittest.main()