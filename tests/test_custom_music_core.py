"""Tests for cores/custom_music_core.py.

Mirrors Tater_Shop's core test style: stub the shared ``helpers`` module with a
FakeRedis, load the core file directly via importlib, and exercise it without a
running Tater.
"""

import importlib.util
import sys
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


if __name__ == "__main__":
    unittest.main()