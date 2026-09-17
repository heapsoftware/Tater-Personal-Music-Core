"""Tests for cores/personal_music_core.py.

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
from urllib.parse import parse_qs, urlparse
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


def load_personal_music_core():
    helpers = types.ModuleType("helpers")
    helpers.redis_client = FakeRedis()
    helpers.extract_json = lambda value: value
    helpers.get_llm_client_from_env = lambda: None
    helpers.get_primary_llm_client_from_env = lambda: None
    sys.modules["helpers"] = helpers

    path = Path(__file__).resolve().parents[1] / "cores" / "personal_music_core.py"
    spec = importlib.util.spec_from_file_location("personal_music_core_test_module", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CustomMusicCoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.core = load_personal_music_core()
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
        self.assertEqual(core.SETTINGS_KEY, "personal_music_core_settings")
        self.assertEqual(core.RUNTIME_KEY, "personal_music_core:runtime")
        self.assertEqual(core.PERSON_LINKS_KEY, "personal_music_core:person_links")
        self.assertEqual(core.CATALOG_KEY, "personal_music_core:catalog:v1")
        self.assertEqual(core.PLAYER_KEY, "personal_music_core:player")
        self.assertEqual(core.HISTORY_KEY, "personal_music_core:history:v1")
        self.assertEqual(core.RECOMMENDATIONS_KEY, "personal_music_core:recommendations:v1")
        self.assertEqual(core.PROMPT_PROFILE_KEY, "personal_music_core:profile:v1")
        self.assertEqual(core.ACTIVITY_KEY, "personal_music_core:activity_feed")
        for key in (core.SETTINGS_KEY, core.RUNTIME_KEY, core.CATALOG_KEY, core.PLAYER_KEY):
            self.assertFalse(key.startswith("music_core"), key)

    def test_settings_category_and_tab_are_distinct(self):
        core = self.core
        self.assertEqual(core.CORE_SETTINGS["category"], "Personal Music Core Settings")
        self.assertEqual(core.CORE_WEBUI_TAB["label"], "Personal Music")

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
                "personal_music_play",
                "personal_music_search",
                "personal_music_control",
                "personal_music_now_playing",
                "personal_music_move",
                "personal_music_confirm",
                "personal_music_browse",
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
            self.assertIn("/api/cores/personal_music_core/webhook/artwork?", artwork_url)

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
            self.assertEqual(activity[0]["source"], "personal_music_core")
            # The same track within the dedupe window is not recorded twice.
            self.core._record_listening_history(
                track, ["voice_core:native:kitchen"], person_id="person_abc"
            )
            self.assertEqual(len(self.core._listening_history(person_id="person_abc")), 1)
        finally:
            self.core._provider = original_provider

    def test_emby_library_folder_scopes_the_catalog(self):
        # A mixed-content library ("John Media": music + TV + movies) scoped to
        # its Music subfolder by name; the song pages query the folder id.
        core = self.core
        provider = core.EmbyMusicProvider(
            server_url="http://emby.local:8096",
            auth_mode="api_key",
            api_key="KEY",
            user_id="u-1",
            library_name="John Media",
            library_folder="Music",
        )
        calls = []

        def fake_request(_method, path, params=None, client=None, **_kwargs):
            calls.append(dict(params or {}))
            if path.endswith("/Views"):
                return {"Items": [{"Id": "view1", "Name": "John Media", "CollectionType": "folder"}]}
            if (params or {}).get("IncludeItemTypes") == "Folder":
                return {
                    "Items": [
                        {"Id": "tv", "Name": "TV", "Path": "/media/john/TV"},
                        {"Id": "music", "Name": "Music", "Path": "/media/john/Music"},
                    ]
                }
            return {"Items": [{"Id": "s1", "Name": "Song"}], "TotalRecordCount": 1}

        original_request = provider.request
        provider.request = fake_request
        try:
            payload = provider.catalog()
            self.assertEqual(payload["catalog_id"], "music")
            self.assertEqual(payload["libraries"], {"music": "John Media · Music"})
            song_params = [entry for entry in calls if entry.get("IncludeItemTypes") == "Song"]
            self.assertTrue(song_params)
            for params in song_params:
                self.assertEqual(params["ParentId"], "music")
            # The folder can also be addressed by its full server path.
            provider.library_folder = "/media/john/Music"
            provider.request = fake_request
            self.assertEqual(provider.catalog()["catalog_id"], "music")
            # Without the setting the whole library stays in scope.
            provider.library_folder = ""
            payload = provider.catalog()
            self.assertEqual(payload["catalog_id"], "view1")
            self.assertEqual(payload["libraries"], {"view1": "John Media"})
            # A folder that does not exist raises with the library named.
            provider.library_folder = "Playlists"
            with self.assertRaises(ValueError) as caught:
                provider.catalog()
            self.assertIn("Playlists", str(caught.exception))
        finally:
            provider.request = original_request

    def test_emby_user_playlists_are_fetched_with_their_songs(self):
        core = self.core
        provider = core.EmbyMusicProvider(
            server_url="http://emby.local:8096", auth_mode="api_key", api_key="KEY", user_id="u-1"
        )
        calls = []

        def fake_request(_method, path, params=None, client=None, **_kwargs):
            calls.append((path, dict(params or {})))
            include = (params or {}).get("IncludeItemTypes", "")
            if include == "Playlist":
                return {
                    "Items": [{"Id": "pl1", "Name": "Road Trip"}, {"Id": "", "Name": "Bad"}],
                    "TotalRecordCount": 2,
                }
            if include == "Audio":
                return {
                    "Items": [{"Id": "s2"}, {"Id": "s1"}],
                    "TotalRecordCount": 2,
                }
            return {"Items": [], "TotalRecordCount": 0}

        original_request = provider.request
        provider.request = fake_request
        try:
            playlists = provider.user_playlists()
        finally:
            provider.request = original_request
        self.assertEqual(
            playlists,
            [{"id": "emby_playlist:pl1", "name": "Road Trip", "description": "", "track_ids": ["s2", "s1"]}],
        )
        # Playlist items are paged through the playlist's Audio children.
        audio_calls = [entry for entry in calls if entry[1].get("IncludeItemTypes") == "Audio"]
        self.assertTrue(audio_calls)
        self.assertEqual(audio_calls[0][1]["ParentId"], "pl1")
        # The catalog carries the user playlists alongside the songs.
        def catalog_request(_method, path, params=None, client=None, **_kwargs):
            include = (params or {}).get("IncludeItemTypes", "")
            if include == "Playlist":
                return {"Items": [{"Id": "pl1", "Name": "Road Trip"}], "TotalRecordCount": 1}
            if include == "Audio":
                return {"Items": [{"Id": "s1"}], "TotalRecordCount": 1}
            if path.endswith("/Views"):
                return {"Items": [{"Id": "view1", "Name": "Music", "CollectionType": "music"}]}
            return {"Items": [], "TotalRecordCount": 0}

        provider.request = catalog_request
        try:
            payload = provider.catalog()
        finally:
            provider.request = original_request
        self.assertEqual(payload["playlists"][0]["name"], "Road Trip")

    def test_emby_album_art_fallback(self):
        # Cover art usually hangs off the Album item in Emby; when the song has
        # no Primary image of its own, artwork fetching uses the album item.
        core = self.core
        track = core._normalize_track(
            {"Id": "9941", "Name": "Song", "AlbumId": "album9", "Artists": ["Zoe"], "Album": "Exodus"}
        )
        self.assertEqual(track["album_id"], "album9")
        self.assertTrue(track["has_artwork"])
        provider = core.EmbyMusicProvider(
            server_url="http://emby.local:8096", auth_mode="api_key", api_key="KEY"
        )
        # No Primary tag on the song → the album's art is requested.
        self.assertIn(
            "/Items/album9/Images/Primary?", provider.artwork_url(track)
        )
        # A song with its own Primary image keeps using the song item.
        tagged = dict(track, artwork_version="tag7")
        self.assertIn(
            "/Items/9941/Images/Primary?", provider.artwork_url(tagged)
        )
        # user_token mode reaches the same album through the stream proxy.
        token_provider = core.EmbyMusicProvider(
            server_url="http://emby.local:8096",
            auth_mode="user_token",
            username="zoe",
            password="pw",
        )
        self.assertIn("/emby_art/album9", token_provider.artwork_url(track))

    def test_person_link_persists_emby_library_folder(self):
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
                    "person_link_source": "emby",
                    "person_link_emby_server_url": "http://emby.local:8096",
                    "person_link_emby_username": "zoe",
                    "person_link_emby_password": "pw",
                    "person_link_emby_library_name": "Zoe Media",
                    "person_link_emby_library_folder": "Music",
                },
                self.redis,
            )
            self.assertTrue(result["ok"], result)
            link = core._person_link("person_zoe", self.redis)
            self.assertEqual(link["emby"]["library_name"], "Zoe Media")
            self.assertEqual(link["emby"]["library_folder"], "Music")
            # The editor carries the folder field back to the form.
            core._save_person_link_edit_target("person_zoe", self.redis)
            cards = {
                item["id"]: item
                for item in core._person_link_items(core._settings(self.redis), self.redis)
            }
            fields = {field["key"]: field for field in cards["person:person_zoe"]["fields"]}
            self.assertEqual(fields["person_link_emby_library_folder"]["value"], "Music")
        finally:
            core._PEOPLE_API_MODULE = original_people
            core._clear_person_link_edit_target(self.redis)

    def test_link_remove_clears_queue_and_per_person_state(self):
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        self.core._PEOPLE_API_MODULE = people
        try:
            self.redis.hset(
                self.core.PERSON_LINKS_KEY,
                mapping={
                    "person_zoe": json.dumps(
                        {"music_source": "network_share", "network_share": {"root_path": "/mnt/music/zoe"}}
                    )
                },
            )
            self.core._save_json(
                self.redis,
                self.core._player_key("person_zoe"),
                {"status": "paused", "queue": [{"id": "t1"}]},
            )
            self.core._register_queue("person_zoe", self.redis)
            self.core._save_follow_me_state(
                "person_zoe", {"status": "following", "zone": "Kitchen"}, self.redis
            )
            self.core._record_catalog_stats(
                self.redis,
                "person_zoe",
                {"status": "ok", "track_count": 3, "synced_at": time.time()},
            )
            result = self.core.handle_htmlui_tab_action(
                action="music_person_link_remove",
                payload={"values": {"person_link_person_id": "person_zoe"}},
                redis_client=self.redis,
            )
            self.assertTrue(result["ok"], result)
            # Link, catalog, history, queue, follow-me state, and sync stats
            # are all gone; re-linking the Person starts clean.
            self.assertEqual(self.core._person_link("person_zoe", self.redis), {})
            player = self.core._player(self.redis, "person_zoe")
            self.assertEqual(player.get("queue") or [], [])
            self.assertEqual(player.get("status"), "idle")
            self.assertNotIn("person_zoe", self.core._queue_registry(self.redis))
            self.assertEqual(self.core._follow_me_state("person_zoe", self.redis), {})
            self.assertEqual(self.core._catalog_stats("person_zoe", self.redis), {})
        finally:
            self.core._PEOPLE_API_MODULE = original_people

    def test_artwork_lookup_falls_back_to_person_catalogs(self):
        # Artwork URLs from a viewed Person's catalog carry no scope, so the
        # artwork webhook must find the track in that Person's catalog and
        # fetch with their own source credentials — not the household's.
        core = self.core
        self.save_settings({"provider": "emby"})
        core._save_json(
            self.redis,
            core._catalog_key(""),
            {"provider": "emby", "tracks": [{"id": "household1", "provider": "emby"}]},
        )
        self.redis.hset(
            core.PERSON_LINKS_KEY,
            mapping={
                "person_zoe": json.dumps(
                    {
                        "music_source": "emby",
                        "emby": {
                            "server_url": "http://emby.local:8096",
                            "auth_mode": "api_key",
                            "api_key": "ZKEY",
                            "user_id": "u-zoe",
                        },
                    }
                )
            },
        )
        core._save_json(
            self.redis,
            core._catalog_key("person_zoe"),
            {
                "provider": "emby",
                "person": "person_zoe",
                "tracks": [
                    {
                        "id": "9941",
                        "provider": "emby",
                        "has_artwork": True,
                        "artwork_item_id": "9941",
                        "artwork_version": "1",
                    }
                ],
            },
        )
        # Unscoped track ids resolve through the Person fallback, tagged.
        track = core._client_track("9941", "emby", self.redis)
        self.assertEqual(track["id"], "9941")
        self.assertEqual(track["person_scope"], "person_zoe")
        # An explicit viewer scope resolves against that Person's catalog.
        scoped = core._client_track("9941", "emby", self.redis, "person_zoe")
        self.assertEqual(scoped["person_scope"], "person_zoe")
        # Household tracks are unaffected.
        self.assertEqual(core._client_track("household1", "emby", self.redis)["id"], "household1")
        # The 2.4.1 regression: an empty household catalog (here: no connected
        # Emby at all) must not block the Person fallback with a sync error.
        with core._catalog_memory_cache_lock:
            core._catalog_memory_cache.update(
                {"store": None, "payload": {}, "loaded_at": 0.0, "person": ""}
            )
        self.redis.delete(core._catalog_key(""))
        track = core._client_track("9941", "emby", self.redis)
        self.assertEqual(track["person_scope"], "person_zoe")
        with self.assertRaises(ValueError):
            core._client_track("nope", "emby", self.redis)
        # The fetch builds the provider with the Person's scope.
        seen = []

        class FakeProvider:
            provider_id = "emby"

            def artwork_url(self, _track):
                return "http://emby.local:8096/Items/9941/Images/Primary?api_key=ZKEY"

        def fake_provider(client=None, provider_id="", person_id=""):
            seen.append((provider_id, person_id))
            return FakeProvider()

        class FakeResponse:
            content = b"img"
            headers = {"Content-Type": "image/png"}

            def raise_for_status(self):
                pass

        original_provider = core._provider
        original_get = core.requests.get
        core._provider = fake_provider
        core.requests.get = lambda *args, **kwargs: FakeResponse()
        try:
            artwork = core._fetch_track_artwork(track, self.redis)
            self.assertEqual(artwork["content_type"], "image/png")
            self.assertEqual(seen[-1], ("emby", "person_zoe"))
        finally:
            core._provider = original_provider
            core.requests.get = original_get
            with core._artwork_cache_lock:
                core._artwork_cache.clear()

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
            self.assertEqual(data["ui"]["title"], "Personal Music Core")
            tab_keys = [tab["key"] for tab in data["ui"]["manager_tabs"]]
            self.assertIn("providers", tab_keys)
            self.assertIn("settings", tab_keys)
            item_ids = [item.get("id") for item in data["ui"]["item_forms"]]
            self.assertIn("provider:emby", item_ids)
        finally:
            self.core._provider = original_provider

    def test_core_system_tasks_shape(self):
        tasks = self.core.get_core_system_tasks()
        self.assertEqual(tasks["label"], "Personal Music Core")
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
        cls.core = load_personal_music_core()

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

    def test_album_nfo_fallback_and_m3u_playlists(self):
        core = self.core
        # An album whose files carry no AlbumArtist tag but ship an album.nfo:
        # the NFO's album artist outranks the track-artist path fallback.
        nfo_album = Path(self.share_root) / "NFO Folder" / "NFO Album"
        nfo_album.mkdir(parents=True)
        (nfo_album / "album.nfo").write_text(
            '<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>\n'
            "<album>\n  <title>NFO Album</title>\n  <albumartist>NFO Album Artist</albumartist>\n"
            "  <artist>Track Artist</artist>\n</album>\n",
            encoding="utf-8",
        )
        _write_wav(nfo_album / "01 - NFO Song.wav")
        # A malformed album.nfo must not break the sync.
        broken = Path(self.share_root) / "Broken NFO" / "Broken Album"
        broken.mkdir(parents=True)
        (broken / "album.nfo").write_text("<album><artist>Oops", encoding="utf-8")
        _write_wav(broken / "01 - Broken Song.wav")
        # User-made .m3u playlists: root-relative and folder-relative entries.
        (Path(self.share_root) / "Road Trip.m3u").write_text(
            "#EXTM3U\n"
            "Share Artist/Share Album/01 - Tagged Song.mp3\n"
            "./Share Artist/Share Album/02 - flac song.flac\n"
            "https://streams.example/remote.mp3\n",
            encoding="utf-8",
        )
        (Path(self.share_root) / "Share Artist" / "folder-list.m3u8").write_text(
            "Share Album/03 - ogg song.ogg\n",
            encoding="utf-8",
        )
        self.connect_share()
        payload = self.catalog()
        by_title = {track["title"]: track for track in payload["tracks"]}
        self.assertEqual(by_title["NFO Song"]["album_artist"], "NFO Album Artist")
        # With no usable NFO the track-artist fallback still applies.
        self.assertEqual(by_title["Broken Song"]["album_artist"], "Broken NFO")
        # The tagged MP3 keeps its real album artist (NFO never overrides it).
        self.assertEqual(by_title["Tagged Song"]["album_artist"], "Share Artist")
        playlists = {row["name"]: row for row in payload.get("playlists") or []}
        self.assertEqual(
            sorted(playlists), ["Road Trip", "folder-list"]
        )
        catalog = core._catalog(provider_id="network_share")
        by_id = {track["id"]: track for track in catalog["tracks"]}
        road_trip = playlists["Road Trip"]
        self.assertEqual(
            [by_id[track_id]["title"] for track_id in road_trip["track_ids"]],
            ["Tagged Song", "Flac Song"],  # remote URLs are skipped
        )
        self.assertEqual(
            [by_id[track_id]["title"] for track_id in playlists["folder-list"]["track_ids"]],
            ["Ogg Song"],
        )
        # Voice "play playlist" resolution finds the m3u playlist by name.
        playlist = core._find_named_playlist(self.redis, "", "road trip", provider_id="network_share")
        self.assertEqual(playlist.get("name"), "Road Trip")
        self.assertEqual(len(playlist["track_ids"]), 2)
        resolved = core._resolve_playlist_tracks(self.redis, "", playlist, provider_id="network_share")
        self.assertEqual([track["title"] for track in resolved], ["Tagged Song", "Flac Song"])

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
        cls.core = load_personal_music_core()

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
            "personal_music_core:catalog:v1:person_zoe",
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

    def test_link_save_folder_playlists_override(self):
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        self.core._PEOPLE_API_MODULE = people
        try:
            self.core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "network_share",
                    "person_link_share_root_path": self.share_root,
                    "person_link_folder_playlists": "Workout=Tunes/Workout, Jazz=Tunes/Jazz",
                },
                self.redis,
            )
            link = self.core._person_link("person_zoe", self.redis)
            self.assertEqual(
                link["folder_playlists"], "Workout=Tunes/Workout, Jazz=Tunes/Jazz"
            )
            # A blank value drops the override so the global list is inherited.
            self.core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "network_share",
                    "person_link_share_root_path": self.share_root,
                    "person_link_folder_playlists": "   ",
                },
                self.redis,
            )
            link = self.core._person_link("person_zoe", self.redis)
            self.assertNotIn("folder_playlists", link)
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


    def test_person_link_emby_test_action(self):
        class FakeEmby(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def _reply(self, payload, status=200):
                raw = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self):
                # The test also resolves the Person's library (and folder).
                if self.path.startswith("/Users/u-zoe/Views"):
                    self._reply(
                        {"Items": [{"Id": "view1", "Name": "Zoe Media", "CollectionType": "music"}]}
                    )
                    return
                if self.path.startswith("/Users/u-zoe/Items"):
                    query = parse_qs(urlparse(self.path).query)
                    if query.get("IncludeItemTypes", [""])[0] == "Folder":
                        self._reply({"Items": [{"Id": "music", "Name": "Music", "Path": "/media/zoe/Music"}]})
                    else:
                        self._reply({"Items": [], "TotalRecordCount": 0})
                    return
                self.send_response(404)
                self.end_headers()

            def do_POST(self):
                if self.path != "/Users/AuthenticateByName":
                    self.send_response(404)
                    self.end_headers()
                    return
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                if body.get("Username") == "zoe" and body.get("Pw") == "right-pw":
                    payload = {"AccessToken": "tok", "User": {"Id": "u-zoe"}}
                    self.send_response(200)
                else:
                    payload = {}
                    self.send_response(401)
                raw = json.dumps(payload).encode("utf-8")
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        upstream = HTTPServer(("127.0.0.1", 0), FakeEmby)
        threading.Thread(target=upstream.serve_forever, daemon=True).start()
        server_url = f"http://127.0.0.1:{upstream.server_address[1]}"
        original_people = self.core._PEOPLE_API_MODULE
        self.core._PEOPLE_API_MODULE = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        try:
            # A missing server URL fails fast.
            with self.assertRaises(ValueError):
                self.core._test_person_link_emby_action(
                    {"person_link_person_id": "person_zoe"}, self.redis
                )
            # Bad credentials are reported, not saved.
            with self.assertRaises(ValueError) as ctx:
                self.core._test_person_link_emby_action(
                    {
                        "person_link_person_id": "person_zoe",
                        "person_link_emby_server_url": server_url,
                        "person_link_emby_username": "zoe",
                        "person_link_emby_password": "wrong",
                    },
                    self.redis,
                )
            self.assertIn("rejected", str(ctx.exception))
            # Good credentials pass, and no link was saved by the test.
            result = self.core._test_person_link_emby_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_emby_server_url": server_url,
                    "person_link_emby_username": "zoe",
                    "person_link_emby_password": "right-pw",
                },
                self.redis,
            )
            self.assertTrue(result["ok"])
            self.assertIn("signed in", result["message"])
            self.assertEqual(self.core._linked_person_ids(self.redis), [])
        finally:
            upstream.shutdown()
            self.core._PEOPLE_API_MODULE = original_people

    def test_person_link_cards_offer_emby_test_button(self):
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [
                    {"id": "person_zoe", "display_name": "Zoe"},
                    {"id": "person_ama", "display_name": "Ama"},
                ]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        core = self.core
        self.core._PEOPLE_API_MODULE = people
        try:
            self.link_person()
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            cards = {item.get("id"): item for item in data["ui"]["item_forms"]}
            # Compact cards stay out of the way: Edit and Remove only.
            for card_id in ("person:person_zoe", "person:new"):
                actions = [a.get("action") for a in cards[card_id].get("actions") or []]
                self.assertIn("music_person_link_edit", actions)
                self.assertNotIn("music_person_link_test", actions)
                self.assertNotIn("fields", cards[card_id])
            # Editing opens the full form for that Person (or the add form).
            core._save_person_link_edit_target("person_zoe", self.redis)
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            cards = {item.get("id"): item for item in data["ui"]["item_forms"]}
            self.assertIn(
                "music_person_link_test",
                [a.get("action") for a in cards["person:person_zoe"]["actions"]],
            )
            core._save_person_link_edit_target("new", self.redis)
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            cards = {item.get("id"): item for item in data["ui"]["item_forms"]}
            self.assertIn(
                "music_person_link_test",
                [a.get("action") for a in cards["person:new"]["actions"]],
            )
        finally:
            self.core._PEOPLE_API_MODULE = original_people
            core._clear_person_link_edit_target(self.redis)

    def test_person_link_test_result_and_typed_values_survive_refetch(self):
        # The tab UI refetches after every action and only toasts errors, so the
        # core records the test outcome and the typed values for the cards to
        # prefill — otherwise a passing test looks like nothing happened and
        # the form empties itself.
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [
                    {"id": "person_zoe", "display_name": "Zoe"},
                    {"id": "person_ama", "display_name": "Ama"},
                ]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        original_provider = self.core.EmbyMusicProvider
        self.core._PEOPLE_API_MODULE = people

        class StubEmby:
            def __init__(self, **_kwargs):
                pass

            @property
            def connected(self):
                return True

            def authenticate(self, *_args, **_kwargs):
                return "tok", "u-zoe"

            def music_view(self, client=None):
                return {"Id": "view1", "Name": "Music"}

            def music_folder(self, view, client=None):
                return view

        self.core.EmbyMusicProvider = StubEmby
        try:
            self.link_person()
            # The editor card is what carries the form, so open it for Zoe.
            self.core._save_person_link_edit_target("person_zoe", self.redis)
            # A passing test is recorded and shown on the linked person's card,
            # with the password left blank (blank still means keep the saved one).
            draft = {
                "person_link_person_id": "person_zoe",
                "person_link_source": "emby",
                "person_link_emby_server_url": "http://emby.local:8096",
                "person_link_emby_username": "zoe",
                "person_link_emby_password": "right-pw",
                "person_link_emby_library_name": "Music",
            }
            result = self.core._test_person_link_emby_action(draft, self.redis)
            state = self.core._person_link_test_state(self.redis)
            self.assertEqual(state["status"], "ok")
            self.assertIn("signed in", result["message"])
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            cards = {item.get("id"): item for item in data["ui"]["item_forms"]}
            linked_rows = {
                row["label"]: row["value"]
                for row in cards["person:person_zoe"].get("summary_rows") or []
            }
            self.assertIn("passed", " ".join(linked_rows))
            linked_fields = {
                field["key"]: field["value"]
                for field in cards["person:person_zoe"]["fields"]
            }
            self.assertEqual(linked_fields["person_link_emby_server_url"], "http://emby.local:8096")
            self.assertEqual(linked_fields["person_link_emby_username"], "zoe")
            self.assertEqual(linked_fields["person_link_emby_password"], "")
            # A failing test for a not-yet-linked person prefills the new-link
            # editor, including the chosen Person and the typed password.
            self.core._save_person_link_edit_target("new", self.redis)
            with self.assertRaises(ValueError):
                self.core._test_person_link_emby_action(
                    {
                        "person_link_person_id": "person_ama",
                        "person_link_emby_server_url": "",
                        "person_link_emby_username": "ama",
                        "person_link_emby_password": "ama-pw",
                    },
                    self.redis,
                )
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            cards = {item.get("id"): item for item in data["ui"]["item_forms"]}
            new_rows = {
                row["label"]: row["value"]
                for row in cards["person:new"].get("summary_rows") or []
            }
            self.assertIn("failed", list(new_rows)[0])
            new_fields = {
                field["key"]: field["value"] for field in cards["person:new"]["fields"]
            }
            self.assertEqual(new_fields["person_link_person_id"], "person_ama")
            self.assertEqual(new_fields["person_link_emby_password"], "ama-pw")
            # Saving or removing the link clears the recorded test state.
            self.core._clear_person_link_test_state("person_ama", self.redis)
            self.assertEqual(self.core._person_link_test_state(self.redis), {})
        finally:
            self.core._PEOPLE_API_MODULE = original_people
            self.core.EmbyMusicProvider = original_provider
            self.core._clear_person_link_edit_target(self.redis)
        original_people = self.core._PEOPLE_API_MODULE
        self.core._PEOPLE_API_MODULE = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [
                    {"id": "person_zoe", "display_name": "Zoe"},
                    {"id": "person_ama", "display_name": "Ama"},
                ]
            }
        )
        try:
            self._run_view_as_switch_exit_and_validation()
        finally:
            self.core._PEOPLE_API_MODULE = original_people

    def _run_view_as_switch_exit_and_validation(self):
        # These Person links have never had their library synced; stub out the
        # background sync scheduler so no worker thread outlives the test.
        original_schedule = self.core._schedule_catalog_sync
        self.core._schedule_catalog_sync = lambda *args, **kwargs: False
        try:
            self._run_view_as_switch_exit_and_validation_inner()
        finally:
            self.core._schedule_catalog_sync = original_schedule

    def _run_view_as_switch_exit_and_validation_inner(self):
        self.assertEqual(self.core._webui_viewer_person_id(self.redis), "")
        # Only linked Persons can be viewed.
        with self.assertRaises(ValueError):
            self.core.handle_htmlui_tab_action(
                action="music_view_as_switch",
                payload={"values": {"view_as_person_id": "person_nobody"}},
                redis_client=self.redis,
            )
        self.link_person()
        result = self.core.handle_htmlui_tab_action(
            action="music_view_as_switch",
            payload={"values": {"view_as_person_id": "person_zoe"}},
            redis_client=self.redis,
        )
        self.assertTrue(result["ok"])
        self.assertIn("Zoe", result["message"])
        self.assertEqual(self.core._webui_viewer_person_id(self.redis), "person_zoe")
        # Switching back to the household clears the view.
        result = self.core.handle_htmlui_tab_action(
            action="music_view_as_exit",
            payload={"values": {}},
            redis_client=self.redis,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(self.core._webui_viewer_person_id(self.redis), "")
        # A stale saved viewer (link removed) falls back to the household view.
        self.core.handle_htmlui_tab_action(
            action="music_view_as_switch",
            payload={"values": {"view_as_person_id": "person_zoe"}},
            redis_client=self.redis,
        )
        self.core.handle_htmlui_tab_action(
            action="music_person_link_remove",
            payload={"values": {"person_link_person_id": "person_zoe"}},
            redis_client=self.redis,
        )
        self.assertEqual(self.core._webui_viewer_person_id(self.redis), "")

    def test_view_as_scopes_tab_data(self):
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
            self.core._sync_catalog(provider_id="network_share", person_id="person_zoe")
            person_catalog = self.core._catalog(self.redis, "network_share", "person_zoe")
            track_id = person_catalog["tracks"][0]["id"]
            person_track = dict(person_catalog["tracks"][0])
            self.core._save_json(
                self.redis,
                self.core._recommendations_key("person_zoe"),
                {
                    "provider": "network_share",
                    "generated_at": time.time(),
                    "summary": "Zoe's mixes",
                    "playlists": [
                        {
                            "id": "mix1",
                            "name": "Zoe Mix",
                            "track_ids": [track_id],
                            "items": [
                                {
                                    "type": "song",
                                    "candidate_id": track_id,
                                    "title": person_track["title"],
                                    "artist": person_track.get("artist") or "Share Artist",
                                    "album": person_track.get("album") or "Share Album",
                                    "image_track_id": track_id,
                                }
                            ],
                        }
                    ],
                },
            )
            person_track = dict(person_catalog["tracks"][0])
            self.core._save_player(
                {
                    "status": "paused",
                    "provider": "network_share",
                    "queue": [person_track],
                    "index": 0,
                    "current": person_track,
                    "targets": [],
                },
                self.redis,
                "person_zoe",
            )
            self.core.handle_htmlui_tab_action(
                action="music_view_as_switch",
                payload={"values": {"view_as_person_id": "person_zoe"}},
                redis_client=self.redis,
            )
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            self.assertIn(
                {"label": "Viewing", "value": "Zoe's music"},
                data["stats"],
            )
            cards = {item.get("id"): item for item in data["ui"]["item_forms"]}
            for card_id in ("view_as:search", "view_as:recommendations", "view_as:people"):
                self.assertIn(card_id, cards)
            self.assertEqual(
                [a["action"] for a in cards["view_as:search"]["actions"]],
                ["music_view_as_exit"],
            )
            # The playlist card now shows Zoe's queue, not the household's.
            player_card = next(
                item for item in data["ui"]["item_forms"] if item.get("group") == "player"
            )
            self.assertEqual(
                [row["title"] for row in player_card["track_list"]],
                [person_track["title"]],
            )
            # The recommendations tab lists Zoe's own AI mix.
            rec_cards = [
                item
                for item in data["ui"]["item_forms"]
                if item.get("group") == "recommendations"
            ]
            self.assertTrue(
                any(item.get("id") == "recommendation:mix1" for item in rec_cards),
                rec_cards,
            )
            # Exiting returns to the household view: her mix disappears.
            self.core.handle_htmlui_tab_action(
                action="music_view_as_exit",
                payload={"values": {}},
                redis_client=self.redis,
            )
            data = self.core.get_htmlui_tab_data(redis_client=self.redis)
            rec_cards = [
                item
                for item in data["ui"]["item_forms"]
                if item.get("group") == "recommendations"
            ]
            self.assertFalse(
                any(item.get("id") == "recommendation:mix1" for item in rec_cards)
            )
        finally:
            self.core._PEOPLE_API_MODULE = original_people

    def test_catalog_sync_state_shows_on_person_and_search_cards(self):
        # The tab UI drops success-path messages, so sync outcomes are recorded
        # in CATALOG_STATS_KEY and shown on the compact card's detail line (the
        # summary-row boxes ellipsis-truncate long text like sync errors).
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        self.core._PEOPLE_API_MODULE = people
        try:
            self.link_person()

            def card_detail():
                cards = {
                    item["id"]: item
                    for item in self.core._person_link_items(
                        self.core._settings(self.redis), self.redis
                    )
                }
                return cards["person:person_zoe"]["detail"]

            self.assertIn("not been synced yet", card_detail())
            # A recorded failure names the error so a silent save-time sync
            # failure is visible on the card.
            self.core._record_catalog_stats(
                self.redis,
                "person_zoe",
                {
                    "status": "error",
                    "error": "This Emby user cannot see any libraries.",
                    "failed_at": time.time(),
                },
            )
            self.assertIn(
                "This Emby user cannot see any libraries.",
                card_detail(),
            )
            # A successful sync shows counts plus the scan time.
            self.core._record_catalog_stats(
                self.redis,
                "person_zoe",
                {
                    "status": "ok",
                    "provider": "network_share",
                    "track_count": 42,
                    "artist_count": 7,
                    "album_count": 9,
                    "genre_count": 5,
                    "synced_at": time.time(),
                },
            )
            detail = card_detail()
            self.assertIn("42 tracks", detail)
            self.assertIn("7 artists", detail)
            self.assertIn("scanned ", detail)
            # While a background sync runs the card says so.
            self.core._record_catalog_stats(
                self.redis, "person_zoe", {"status": "syncing"}
            )
            self.assertEqual(card_detail(), "Syncing…")
            # The search card reports how many tracks a search covers.
            item = self.core._search_item(
                {
                    "tracks": [{} for _ in range(3)],
                    "artists": ["A"],
                    "albums": ["B"],
                    "genres": ["C"],
                }
            )
            self.assertEqual(
                item["summary_rows"][0],
                {"label": "Searchable Library", "value": "3 tracks · 1 artists · 1 albums · 1 genres"},
            )
            self.assertIn(
                "0 tracks",
                self.core._search_item({})["summary_rows"][0]["value"],
            )
        finally:
            self.core._PEOPLE_API_MODULE = original_people

    def test_edit_flow_opens_and_closes_the_link_editor(self):
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
            # Edit flips that Person's compact card into the full form.
            result = self.core.handle_htmlui_tab_action(
                action="music_person_link_edit",
                payload={"values": {}, "id": "person:person_zoe"},
                redis_client=self.redis,
            )
            self.assertTrue(result["ok"], result)
            self.assertEqual(self.core._person_link_edit_target(self.redis), "person_zoe")
            cards = {
                item["id"]: item
                for item in self.core._person_link_items(
                    self.core._settings(self.redis), self.redis
                )
            }
            self.assertIn("fields", cards["person:person_zoe"])
            self.assertNotIn("fields", cards["person:new"])
            # Cancel closes it; the add form opens with the "new" target.
            self.core.handle_htmlui_tab_action(
                action="music_person_link_edit_cancel",
                payload={"values": {}},
                redis_client=self.redis,
            )
            self.assertEqual(self.core._person_link_edit_target(self.redis), "")
            self.core.handle_htmlui_tab_action(
                action="music_person_link_edit",
                payload={"values": {"person_link_person_id": "new"}},
                redis_client=self.redis,
            )
            cards = {
                item["id"]: item
                for item in self.core._person_link_items(
                    self.core._settings(self.redis), self.redis
                )
            }
            self.assertIn("fields", cards["person:new"])
            self.assertNotIn("fields", cards["person:person_zoe"])
            # Only linked People (or the add form) can be opened for editing.
            self.core._clear_person_link_edit_target(self.redis)
            with self.assertRaises(ValueError):
                self.core.handle_htmlui_tab_action(
                    action="music_person_link_edit",
                    payload={"values": {"person_link_person_id": "person_ama"}},
                    redis_client=self.redis,
                )
            # Saving a link closes the editor as well.
            self.core.handle_htmlui_tab_action(
                action="music_person_link_edit",
                payload={"values": {"person_link_person_id": "person_zoe"}},
                redis_client=self.redis,
            )
            self.core.handle_htmlui_tab_action(
                action="music_person_link_save",
                payload={
                    "values": {
                        "person_link_person_id": "person_zoe",
                        "person_link_source": "",
                    }
                },
                redis_client=self.redis,
            )
            self.assertEqual(self.core._person_link_edit_target(self.redis), "")
        finally:
            self.core._PEOPLE_API_MODULE = original_people
            self.core._clear_person_link_edit_target(self.redis)

    def test_view_as_switch_syncs_empty_person_library(self):
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        original_people = self.core._PEOPLE_API_MODULE
        original_schedule = self.core._schedule_catalog_sync
        scheduled = []
        self.core._PEOPLE_API_MODULE = people
        self.core._schedule_catalog_sync = (
            lambda person_id="", client=None: scheduled.append(person_id) or True
        )
        try:
            self.link_person()
            result = self.core.handle_htmlui_tab_action(
                action="music_view_as_switch",
                payload={"values": {"view_as_person_id": "person_zoe"}},
                redis_client=self.redis,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(scheduled, ["person_zoe"])
            self.assertIn("syncing now", result["message"])
            # A Person whose library is already loaded is left alone.
            self.core._save_json(
                self.redis,
                self.core._catalog_key("person_zoe"),
                {"provider": "network_share", "tracks": [{"title": "Song"}]},
            )
            self.core._catalog_memory_cache.update(
                {"store": None, "payload": {}, "loaded_at": 0.0, "person": ""}
            )
            result = self.core.handle_htmlui_tab_action(
                action="music_view_as_switch",
                payload={"values": {"view_as_person_id": "person_zoe"}},
                redis_client=self.redis,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(scheduled, ["person_zoe"])
            self.assertNotIn("syncing", result["message"])
        finally:
            self.core._PEOPLE_API_MODULE = original_people
            self.core._schedule_catalog_sync = original_schedule

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
        cls.core = load_personal_music_core()

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
        self.assertEqual(core._player_key("p1"), "personal_music_core:player:p1")
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
                tool_id="personal_music_confirm",
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
                tool_id="personal_music_confirm",
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
                    tool_id="personal_music_control",
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

    # ---- group volume, mute all / unmute all ----

    def stub_group_volume(self):
        self.volume_calls = []
        self._originals["_set_target_volume"] = self.core._set_target_volume

        def fake_set_target_volume(player, volume_percent):
            self.volume_calls.append(volume_percent)
            return {
                "sent_count": len(player.get("targets") or []),
                "warnings": [],
            }

        self.core._set_target_volume = fake_set_target_volume

    def test_volume_action_sets_every_group_member_to_the_same_level(self):
        core = self.core
        self.stub_playback()
        self.stub_group_volume()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen", "voice_core:native:office"])
        # "Set all speakers 70%": one action, every member at the same level.
        result = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_control",
                args={"action": "volume", "volume_percent": 70},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self.volume_calls, [70])
        player = core._player(self.redis, "person_a")
        self.assertEqual(player["volume_percent"], 70)
        self.assertEqual(result["summary_for_user"], "Every speaker in the group is now at 70%.")
        # A missing percentage is asked for, not guessed.
        missing = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_control",
                args={"action": "volume"},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertFalse(missing.get("ok"))
        self.assertIn("percentage", missing["error"]["message"])

    def test_mute_all_and_unmute_all_round_trip(self):
        core = self.core
        self.stub_playback()
        self.stub_group_volume()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])  # volume 60
        muted = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_control",
                args={"action": "mute_all"},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(muted.get("ok"), muted)
        player = core._player(self.redis, "person_a")
        self.assertTrue(player.get("muted"))
        self.assertEqual(player["volume_percent"], 0)
        self.assertEqual(self.volume_calls[-1], 0)
        # Unmuting restores the pre-mute volume on every speaker.
        unmuted = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_control",
                args={"action": "unmute_all"},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(unmuted.get("ok"), unmuted)
        player = core._player(self.redis, "person_a")
        self.assertFalse(player.get("muted"))
        self.assertEqual(player["volume_percent"], 60)
        self.assertEqual(self.volume_calls[-1], 60)
        # Unmuting without a stored level falls back to the default volume.
        player = core._player(self.redis, "person_a")
        player.pop("pre_mute_volume", None)
        core._apply_player_mute(player, mute=False, client=self.redis)
        self.assertEqual(player["volume_percent"], 60)
        # Muting a paused queue needs no live speakers and still persists.
        core._pause_player(person_id="person_a", client=self.redis)
        result = core.handle_htmlui_tab_action(
            action="music_ui_mute_all", payload={}, redis_client=self.redis
        )
        self.assertTrue(result["ok"], result)
        # The dashboard acts on the shared (household) player; person_a's
        # paused queue keeps its own volume.
        shared = core._player(self.redis)
        self.assertTrue(shared.get("muted"))
        self.assertEqual(shared["volume_percent"], 0)
        self.assertEqual(core._player(self.redis, "person_a")["volume_percent"], 60)

    def test_player_card_offers_mute_all_buttons(self):
        core = self.core
        player = self.seed_playing_queue("", ["voice_core:native:kitchen"])
        core._save_player(player, self.redis, "")
        item = core._player_item(core._player(self.redis), [], "emby", core._settings(self.redis))
        actions = {row["action"] for row in item["actions"]}
        self.assertIn("music_ui_mute_all", actions)
        self.assertIn("music_ui_unmute_all", actions)
        self.assertNotIn("MUTED", [badge["label"] for badge in item["hero_badges"]])
        player = core._player(self.redis)
        player["muted"] = True
        item = core._player_item(player, [], "emby", core._settings(self.redis))
        self.assertIn("MUTED", [badge["label"] for badge in item["hero_badges"]])

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
                tool_id="personal_music_move",
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
                tool_id="personal_music_move",
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
        cls.core = load_personal_music_core()

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
                tool_id="personal_music_confirm",
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
                tool_id="personal_music_confirm",
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
        self.assertIn("personal_music_confirm", note)
        self.assertIn("'yes'", note)
        fragments = core.get_hydra_system_prompt_fragments(
            role="chat", redis_client=self.redis, origin=origin
        )
        self.assertTrue(any("personal_music_confirm" in m for m in fragments["chat"]))
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
            # The follow-me status shows on the compact card; the fields live
            # in the editor that the card's Edit action opens.
            self.assertIn("Follow-me: in Kitchen → Kitchen", cards["person:person_zoe"]["subtitle"])
            core._save_person_link_edit_target("person_zoe", self.redis)
            cards = {item["id"]: item for item in core._person_link_items(core._settings(self.redis), self.redis)}
            fields = {field["key"]: field for field in cards["person:person_zoe"]["fields"]}
            self.assertEqual(fields["person_link_follow_me_entity"]["value"], "person.zoe")
            self.assertEqual(fields["person_link_follow_me_room_overrides"]["value"], "The Kitchen=Kitchen")
            self.assertEqual(fields["person_link_follow_me_takeover_mode"]["value"], "ask")
            self.assertEqual(fields["person_link_follow_me_away_action"]["value"], "pause")
            self.assertIn("Follow-me: in Kitchen → Kitchen", cards["person:person_zoe"]["subtitle"])
            # The new-link editor carries the same fields, defaulted from settings.
            core._save_person_link_edit_target("new", self.redis)
            cards = {item["id"]: item for item in core._person_link_items(core._settings(self.redis), self.redis)}
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
            core._clear_person_link_edit_target(self.redis)

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


class ResumeDelayTests(unittest.TestCase):
    """Gaining-room resume delays: transfers, follow-me moves, away returns."""

    @classmethod
    def setUpClass(cls):
        cls.core = load_personal_music_core()

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

    def link(self, person_id, **extra):
        link = {"music_source": ""}
        link.update(extra)
        self.redis.hset(self.core.PERSON_LINKS_KEY, mapping={person_id: json.dumps(link)})

    def origin_for(self, person_id, selector="kitchen"):
        return {
            "people_resolution": {"master_user_id": person_id},
            "satellite_selector": selector,
        }

    # ---- delay resolution ----

    def test_delay_resolution_global_person_clamp_and_blank(self):
        core = self.core
        # Defaults: no delay anywhere.
        self.assertEqual(core._transfer_resume_delay("person_a", self.redis), 0.0)
        self.assertEqual(core._follow_me_move_resume_delay("person_a", self.redis), 0.0)
        self.assertEqual(core._follow_me_resume_delay("person_a", self.redis), 0.0)
        self.redis.hset(
            core.SETTINGS_KEY,
            mapping={
                "transfer_resume_delay_seconds": "45",
                "follow_me_move_resume_delay_seconds": "10",
                "follow_me_resume_delay_seconds": "5",
            },
        )
        self.assertEqual(core._transfer_resume_delay("person_a", self.redis), 45.0)
        self.assertEqual(core._follow_me_move_resume_delay("person_a", self.redis), 10.0)
        self.assertEqual(core._follow_me_resume_delay("person_a", self.redis), 5.0)
        # The three settings do not bleed into each other.
        self.assertEqual(core._transfer_resume_delay("", self.redis), 45.0)
        # Per-Person link overrides win over the global setting.
        self.link(
            "person_a",
            transfer_resume_delay_seconds="120",
            follow_me_move_resume_delay_seconds="0",
            follow_me_resume_delay_seconds="30",
        )
        self.assertEqual(core._transfer_resume_delay("person_a", self.redis), 120.0)
        # An explicit 0 is a real override ("resume immediately"), not inherit.
        self.assertEqual(core._follow_me_move_resume_delay("person_a", self.redis), 0.0)
        self.assertEqual(core._follow_me_resume_delay("person_a", self.redis), 30.0)
        # Blank means inherit; out-of-range clamps to 0-600.
        self.link("person_b", transfer_resume_delay_seconds="", follow_me_resume_delay_seconds="9999")
        self.assertEqual(core._transfer_resume_delay("person_b", self.redis), 45.0)
        self.assertEqual(core._follow_me_resume_delay("person_b", self.redis), 600.0)
        self.link("person_c", transfer_resume_delay_seconds="-7")
        self.assertEqual(core._transfer_resume_delay("person_c", self.redis), 0.0)

    def test_link_save_persists_resume_delay_fields(self):
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
                    "person_link_transfer_resume_delay_seconds": " 25 ",
                    "person_link_follow_me_move_resume_delay_seconds": "40",
                    "person_link_follow_me_resume_delay_seconds": "0",
                },
                self.redis,
            )
            self.assertTrue(result["ok"], result)
            link = core._person_link("person_zoe", self.redis)
            self.assertEqual(link["transfer_resume_delay_seconds"], 25)
            self.assertEqual(link["follow_me_move_resume_delay_seconds"], 40)
            # Explicit 0 persists as an override, not as "inherit".
            self.assertEqual(link["follow_me_resume_delay_seconds"], 0)
            # Blank fields are dropped from the link so they inherit the global;
            # unparsable values are treated the same way, out-of-range clamps.
            result = core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "",
                    "person_link_transfer_resume_delay_seconds": "",
                    "person_link_follow_me_move_resume_delay_seconds": "bogus",
                    "person_link_follow_me_resume_delay_seconds": "9999",
                },
                self.redis,
            )
            self.assertTrue(result["ok"], result)
            link = core._person_link("person_zoe", self.redis)
            self.assertNotIn("transfer_resume_delay_seconds", link)
            self.assertNotIn("follow_me_move_resume_delay_seconds", link)
            self.assertEqual(link.get("follow_me_resume_delay_seconds"), 600)
        finally:
            core._PEOPLE_API_MODULE = original_people

    # ---- the delayed handoff itself ----

    def test_route_targets_with_delay_pauses_then_resumes_at_position(self):
        core = self.core
        self.stub_playback()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        player = core._route_player_targets(
            ["voice_core:native:office"],
            resume_delay=15,
            person_id="person_a",
            client=self.redis,
        )
        # Nothing plays yet: the gaining room waits.
        self.assertEqual(self.played, [])
        self.assertEqual(player["status"], "paused")
        self.assertEqual(player["targets"], ["voice_core:native:office"])
        self.assertAlmostEqual(player["position_offset_seconds"], 40.0, delta=1.0)
        self.assertAlmostEqual(
            player["resume_delay_until"] - time.time(), 15.0, delta=2.0
        )
        self.assertAlmostEqual(player["resume_delay_position"], 40.0, delta=1.0)
        # Ticking before the delay elapses changes nothing.
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(self.played, [])
        # Once it elapses the music resumes where it left off.
        player = core._player(self.redis, "person_a")
        player["resume_delay_until"] = time.time() - 1.0
        core._save_player(player, self.redis, "person_a")
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(len(self.played), 1)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:native:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        resumed = core._player(self.redis, "person_a")
        self.assertEqual(resumed["status"], "playing")
        self.assertNotIn("resume_delay_until", resumed)
        self.assertNotIn("resume_delay_position", resumed)
        # The tick is a no-op once the pending resume is gone.
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(len(self.played), 1)

    def test_forced_restart_on_unchanged_targets_never_stalls(self):
        core = self.core
        self.stub_playback()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        player = core._route_player_targets(
            ["voice_core:native:kitchen"],
            force_restart=True,
            resume_delay=15,
            person_id="person_a",
            client=self.redis,
        )
        # A sync calibration restart replays immediately, even with a delay set.
        self.assertEqual(len(self.played), 1)
        self.assertEqual(player["status"], "playing")
        self.assertNotIn("resume_delay_until", player)

    def test_manual_play_pause_and_stop_cancel_the_pending_resume(self):
        core = self.core
        self.stub_playback()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        core._route_player_targets(
            ["voice_core:native:office"], resume_delay=60, person_id="person_a", client=self.redis
        )
        self.assertEqual(self.played, [])
        # Pressing play in the gaining room starts the music right away.
        core._resume_player(person_id="person_a", client=self.redis)
        self.assertEqual(len(self.played), 1)
        resumed = core._player(self.redis, "person_a")
        self.assertEqual(resumed["status"], "playing")
        self.assertNotIn("resume_delay_until", resumed)
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(len(self.played), 1)
        # Pause clears the pending resume too.
        core._pause_player(person_id="person_a", client=self.redis)
        core._route_player_targets(
            ["voice_core:native:den"], resume_delay=60, person_id="person_a", client=self.redis
        )
        # A paused queue is not restarted by the move, so no new delay is armed;
        # stop clears anything left over.
        core._stop_player(person_id="person_a", client=self.redis)
        self.assertNotIn("resume_delay_until", core._player(self.redis, "person_a"))
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(len(self.played), 1)

    def test_move_tool_applies_the_transfer_resume_delay(self):
        core = self.core
        self.stub_playback()
        self.redis.hset(core.SETTINGS_KEY, mapping={"transfer_resume_delay_seconds": "20"})
        self._originals["_resolve_targets"] = core._resolve_targets
        core._resolve_targets = (
            lambda requested="", room="", origin=None, client=None, provider_id="", person_id="": [
                "voice_core:native:office"
            ]
        )
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        result = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_move",
                args={"rooms": ["Office"]},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self.played, [])
        moved = core._player(self.redis, "person_a")
        self.assertEqual(moved["targets"], ["voice_core:native:office"])
        self.assertEqual(moved["status"], "paused")
        self.assertGreater(float(moved["resume_delay_until"]), time.time())
        self.assertIn("resumes there in 20 seconds", json.dumps(result))
        # A per-Person override changes both the wait and the wording.
        self.link("person_a", transfer_resume_delay_seconds=0)
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        result = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_move",
                args={"rooms": ["Office"]},
                origin=self.origin_for("person_a"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(len(self.played), 1)
        self.assertEqual(core._player(self.redis, "person_a")["status"], "playing")
        self.assertNotIn("resumes there in", json.dumps(result))

    def test_follow_me_move_and_away_return_use_their_delays(self):
        core = self.core
        self.stub_playback()
        self.link(
            "person_a",
            follow_me_person_entity="person.john",
            follow_me_move_resume_delay_seconds=20,
            follow_me_resume_delay_seconds=10,
        )
        # A zone move into the new room pauses with a pending resume.
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        result = core._follow_me_move(
            "person_a", "Office", ["voice_core:native:office"], self.redis
        )
        self.assertTrue(result["moved"], result)
        self.assertEqual(self.played, [])
        moved = core._player(self.redis, "person_a")
        self.assertEqual(moved["status"], "paused")
        self.assertEqual(moved["targets"], ["voice_core:native:office"])
        self.assertAlmostEqual(
            moved["resume_delay_until"] - time.time(), 20.0, delta=2.0
        )
        # Expiring the wait resumes at the same spot in the new room.
        moved["resume_delay_until"] = time.time() - 1.0
        core._save_player(moved, self.redis, "person_a")
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(len(self.played), 1)
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)

        # Away -> return: Follow-Me paused them, so the away-resume delay arms.
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=50.0, elapsed=5.0)
        self.played.clear()
        state = {"status": "paused_away", "paused_by_follow_me": True}
        core._save_follow_me_state("person_a", state, self.redis)
        result = core._follow_me_move(
            "person_a", "Office", ["voice_core:native:office"], self.redis
        )
        self.assertTrue(result["moved"], result)
        self.assertEqual(self.played, [])
        returned = core._player(self.redis, "person_a")
        self.assertEqual(returned["status"], "paused")
        self.assertAlmostEqual(
            returned["resume_delay_until"] - time.time(), 10.0, delta=2.0
        )
        self.assertAlmostEqual(returned["resume_delay_position"], 55.0, delta=1.0)
        returned["resume_delay_until"] = time.time() - 1.0
        core._save_player(returned, self.redis, "person_a")
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(len(self.played), 1)
        self.assertAlmostEqual(self.played[-1]["start_position"], 55.0, delta=1.0)

    def test_follow_me_tick_moves_and_waits_for_the_resume_delay(self):
        core = self.core
        self.stub_playback()
        self.redis.hset(
            core.SETTINGS_KEY,
            mapping={"follow_me_enabled": "1", "follow_me_move_delay_seconds": "0"},
        )
        self.link("person_a", follow_me_person_entity="person.john", follow_me_move_resume_delay_seconds=15)
        # Tater's built-in HA integration settings (reused, never rewritten).
        self.redis.hset(
            core.HA_SETTINGS_KEY,
            mapping={"HA_BASE_URL": "http://ha.local:8123", "HA_TOKEN": "secret"},
        )
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"], position=30.0, elapsed=10.0)
        self._originals["_ha_person_location"] = core._ha_person_location
        core._ha_person_location = lambda client, entity: {"state": "Office"}
        self._originals["_room_name_to_targets"] = core._room_name_to_targets
        core._room_name_to_targets = lambda name, client=None: {
            "Office": "voice_core:native:office"
        }.get(str(name), "")
        try:
            summary = core._follow_me_tick(self.redis)
            self.assertEqual(summary["moved"], 1)
            self.assertEqual(self.played, [])
            player = core._player(self.redis, "person_a")
            self.assertEqual(player["targets"], ["voice_core:native:office"])
            self.assertEqual(player["status"], "paused")
            self.assertGreater(float(player["resume_delay_until"]), time.time())
        finally:
            core._room_name_to_targets = self._originals["_room_name_to_targets"]
            core._ha_person_location = self._originals["_ha_person_location"]


class ResumeRoomTests(unittest.TestCase):
    """Resume in Another Room: what "resume my music" does from another room."""

    @classmethod
    def setUpClass(cls):
        cls.core = load_personal_music_core()

    def setUp(self):
        self.redis = FakeRedis()
        self.core.redis_client = self.redis
        self.core._shutdown_stream_server()
        self.played = []
        self.stopped = []
        self.spoken = []
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
        self._originals["_speak_follow_me_prompt"] = self.core._speak_follow_me_prompt

        def fake_speak(targets, text):
            self.spoken.append({"targets": list(targets), "text": text})
            return True

        self.core._speak_follow_me_prompt = fake_speak

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

    def seed_paused_queue(self, person_id, targets):
        self.seed_playing_queue(person_id, targets, position=30.0, elapsed=10.0)
        self.core._pause_player(person_id=person_id, client=self.redis)
        self.played.clear()

    def link(self, person_id, **extra):
        link = {"music_source": ""}
        link.update(extra)
        self.redis.hset(self.core.PERSON_LINKS_KEY, mapping={person_id: json.dumps(link)})

    def origin_for(self, person_id, selector):
        return {
            "people_resolution": {"master_user_id": person_id},
            "satellite_selector": selector,
        }

    def run_control(self, action, origin):
        return asyncio.run(
            self.core.run_hydra_kernel_tool(
                tool_id="personal_music_control",
                args={"action": action},
                origin=origin,
                redis_client=self.redis,
            )
        )

    # ---- mode resolution ----

    def test_resume_room_mode_resolution(self):
        core = self.core
        self.assertEqual(core._person_resume_room_mode("person_a", self.redis), "stay")
        self.redis.hset(core.SETTINGS_KEY, mapping={"resume_room_mode": "follow"})
        self.assertEqual(core._person_resume_room_mode("person_a", self.redis), "follow")
        self.assertEqual(core._person_resume_room_mode("", self.redis), "follow")
        # Per-Person override wins; invalid falls back to the global.
        self.link("person_a", resume_room_mode="ask")
        self.assertEqual(core._person_resume_room_mode("person_a", self.redis), "ask")
        self.link("person_b", resume_room_mode="yolo")
        self.assertEqual(core._person_resume_room_mode("person_b", self.redis), "follow")
        self.link("person_c", resume_room_mode="")
        self.assertEqual(core._person_resume_room_mode("person_c", self.redis), "follow")

    def test_link_save_persists_resume_room_mode(self):
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
                    "person_link_resume_room_mode": " Follow ",
                },
                self.redis,
            )
            self.assertTrue(result["ok"], result)
            self.assertEqual(
                core._person_link("person_zoe", self.redis)["resume_room_mode"], "follow"
            )
            # An invalid select value never wipes the saved choice.
            result = core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "",
                    "person_link_resume_room_mode": "teleport",
                },
                self.redis,
            )
            self.assertTrue(result["ok"], result)
            self.assertEqual(
                core._person_link("person_zoe", self.redis)["resume_room_mode"], "follow"
            )
        finally:
            core._PEOPLE_API_MODULE = original_people

    # ---- the resume itself ----

    def test_follow_mode_moves_the_paused_queue_to_the_speaking_room(self):
        core = self.core
        self.stub_playback()
        self.link("person_a", resume_room_mode="follow")
        self.seed_paused_queue("person_a", ["voice_core:native:kitchen"])
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(len(self.played), 1)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        player = core._player(self.redis, "person_a")
        self.assertEqual(player["targets"], ["voice_core:office"])
        self.assertEqual(player["status"], "playing")
        self.assertIn("same spot", result["summary_for_user"])

    def test_follow_mode_stays_put_when_the_speaking_room_is_busy(self):
        core = self.core
        self.stub_playback()
        self.link("person_a", resume_room_mode="follow")
        self.seed_playing_queue("person_b", ["voice_core:office"])
        self.seed_paused_queue("person_a", ["voice_core:native:kitchen"])
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        # The move was abandoned: person_a's music resumed where it was paused,
        # and person_b's queue in the office was untouched.
        self.assertEqual(len(self.played), 1)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:native:kitchen"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        self.assertEqual(core._player(self.redis, "person_a")["targets"], ["voice_core:native:kitchen"])
        self.assertEqual(core._player(self.redis, "person_b")["status"], "playing")
        self.assertIn("was still playing", result["summary_for_user"])
        # The busy room kept ownership: no takeover question, no interruption.
        self.assertEqual(self.spoken, [])
        self.assertEqual(core._load_pending_confirmation(self.redis, "person_a"), {})

    def test_stay_mode_keeps_the_nearby_room_wins_rule(self):
        core = self.core
        self.stub_playback()
        # Default mode: a foreign queue paused in the speaking room is acted on
        # first, exactly as before.
        self.seed_paused_queue("person_b", ["voice_core:office"])
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        self.core._pause_player(person_id="person_a", client=self.redis)
        self.played.clear()
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:office"])
        # With no queue near the speaker, their own queue resumes in place.
        self.core._stop_player(person_id="person_b", client=self.redis)
        self.played.clear()
        self.seed_playing_queue("person_a", ["voice_core:native:kitchen"])
        self.core._pause_player(person_id="person_a", client=self.redis)
        self.played.clear()
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:native:kitchen"])
        self.assertEqual(core._player(self.redis, "person_a")["targets"], ["voice_core:native:kitchen"])

    def test_ask_mode_asks_then_resumes_where_answered(self):
        core = self.core
        self.stub_playback()
        self.link("person_a", resume_room_mode="ask")
        self.seed_paused_queue("person_a", ["voice_core:native:kitchen"])
        # Nothing plays yet; the room gets the question instead.
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self.played, [])
        self.assertEqual(len(self.spoken), 1)
        self.assertIn("resume it here", self.spoken[-1]["text"])
        self.assertEqual(self.spoken[-1]["targets"], ["voice_core:office"])
        pending = core._load_pending_confirmation(self.redis, "person_a")
        self.assertEqual(pending.get("type"), "resume_room")
        self.assertEqual(pending.get("targets"), ["voice_core:office"])
        # "here" moves it into the speaking room at the same spot.
        result = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_confirm",
                args={"choice": "here"},
                origin=self.origin_for("person_a", "office"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(len(self.played), 1)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)
        # Again from the top; "there" leaves it where it was paused.
        self.seed_paused_queue("person_a", ["voice_core:native:kitchen"])
        self.run_control("resume", self.origin_for("person_a", "office"))
        result = asyncio.run(
            core.run_hydra_kernel_tool(
                tool_id="personal_music_confirm",
                args={"choice": "there"},
                origin=self.origin_for("person_a", "office"),
                redis_client=self.redis,
            )
        )
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:native:kitchen"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)

    def test_ask_mode_resumes_in_place_when_the_room_is_busy(self):
        core = self.core
        self.stub_playback()
        self.link("person_a", resume_room_mode="ask")
        self.seed_playing_queue("person_b", ["voice_core:office"])
        self.seed_paused_queue("person_a", ["voice_core:native:kitchen"])
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        # Asking would be pointless (this room is taken): it just resumes.
        self.assertEqual(self.spoken, [])
        self.assertEqual(core._load_pending_confirmation(self.redis, "person_a"), {})
        self.assertEqual(self.played[-1]["targets"], ["voice_core:native:kitchen"])

    def test_follow_mode_applies_the_transfer_resume_delay(self):
        core = self.core
        self.stub_playback()
        self.redis.hset(core.SETTINGS_KEY, mapping={"transfer_resume_delay_seconds": "15"})
        self.link("person_a", resume_room_mode="follow")
        self.seed_paused_queue("person_a", ["voice_core:native:kitchen"])
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(self.played, [])
        player = core._player(self.redis, "person_a")
        self.assertEqual(player["targets"], ["voice_core:office"])
        self.assertEqual(player["status"], "paused")
        self.assertAlmostEqual(player["resume_delay_until"] - time.time(), 15.0, delta=2.0)
        self.assertIn("resumes there in 15 seconds", result["summary_for_user"])
        # Once the wait elapses it starts at the saved spot in the new room.
        player["resume_delay_until"] = time.time() - 1.0
        core._save_player(player, self.redis, "person_a")
        core._delayed_resume_tick(self.redis, "person_a")
        self.assertEqual(len(self.played), 1)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)

    def test_follow_mode_never_reroutes_within_the_same_room(self):
        core = self.core
        self.stub_playback()
        self.link("person_a", resume_room_mode="follow")
        self.seed_paused_queue("person_a", ["voice_core:office"])
        result = self.run_control("resume", self.origin_for("person_a", "office"))
        self.assertTrue(result.get("ok"), result)
        self.assertEqual(len(self.played), 1)
        self.assertEqual(self.played[-1]["targets"], ["voice_core:office"])
        self.assertAlmostEqual(self.played[-1]["start_position"], 40.0, delta=1.0)


class UpstreamCoexistenceTests(unittest.TestCase):
    """Both this core and the upstream Music Core enabled side by side."""

    @classmethod
    def setUpClass(cls):
        cls.core = load_personal_music_core()
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
            if name.isupper() and isinstance(value, str) and "personal_music_core" in value
        }
        theirs = {
            value
            for name, value in vars(self.upstream).items()
            if name.isupper() and isinstance(value, str) and "music" in value
        }
        overlap = {
            key
            for key in ours & theirs
            if key.startswith(("music_core", "personal_music_core"))
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


def _share_track_row(number, title, root_tag="a"):
    """A share-provider track with ids unique to its share root."""
    core = sys.modules.get("personal_music_core_test_module")
    rel_path = f"{root_tag}/song{number}.mp3"
    row = _track_row(number, title)
    row["provider"] = "network_share"
    row["path"] = rel_path
    row["id"] = "track:" + f"{root_tag}{number}"
    if core is not None:
        row["provider_track_id"] = core._share_scoped_stream_id(rel_path, f"/mnt/music/{root_tag}")
    return row


class EndlessPlaybackTests(unittest.TestCase):
    """Endless Playback modes, Smart Shuffle, sleep timers, and multi-source links."""

    @classmethod
    def setUpClass(cls):
        cls.core = load_personal_music_core()

    def setUp(self):
        self.redis = FakeRedis()
        self.core.redis_client = self.redis
        self.core._shutdown_stream_server()
        self.played = []
        self.stopped = []
        self._originals = {}
        self.stub_playback()

    def tearDown(self):
        for name, value in self._originals.items():
            setattr(self.core, name, value)
        self.core._shutdown_stream_server()
        self.core.STREAMING_PROVIDER_CLASSES.pop("test_stream", None)

    def stub_playback(self):
        self._originals["_play_track"] = self.core._play_track

        def fake_play_track(track, targets, *, volume_percent, start_position_seconds=0.0, **_kwargs):
            self.played.append({"track_id": track.get("id"), "targets": list(targets)})
            return {"ok": True, "sent_count": len(targets), "voice_core_sessions": []}

        self.core._play_track = fake_play_track
        self._originals["_stop_target"] = self.core._stop_target

        def fake_stop_target(targets, *, expected_voice_core_sessions=None):
            self.stopped.append(list(targets))
            return []

        self.core._stop_target = fake_stop_target

    def seed_person_catalog(self, person_id, tracks, playlists=None):
        self.core._save_json(
            self.redis,
            self.core._catalog_key(person_id),
            {
                "provider": "emby",
                "tracks": tracks,
                "playlists": playlists or [],
                "synced_at": time.time(),
            },
        )

    def seed_playing_queue(self, person_id, tracks, *, index=0):
        player = {
            "status": "playing",
            "provider": "emby",
            "queue": [dict(track) for track in tracks],
            "queue_original": [dict(track) for track in tracks],
            "index": index,
            "current": tracks[index],
            "targets": ["voice_core:native:kitchen"],
            "person_id": person_id,
            "shuffle": False,
            "repeat": "off",
            "volume_percent": 60,
            "queue_session_id": "session-1",
            "continuous_radio": True,
            "continuation_pending": False,
            "radio_name": "Tater Continuous Radio",
            "started_at": time.time() - 5.0,
            "position_offset_seconds": 5.0,
            "duration_seconds": 180.0,
        }
        self.core._save_player(player, self.redis, person_id)
        return player

    # ---- Endless Playback modes ----

    def test_endless_mode_inherits_global_and_overrides_per_person(self):
        core = self.core
        self.save = lambda mapping: core._save_hash(self.redis, core.SETTINGS_KEY, mapping)
        # Default stays the original LLM radio behaviour.
        self.assertEqual(core._endless_mode_for_queue({"queue_id": ""}, self.redis), "llm_auto")
        self.save({"endless_playback_mode": "library_mix"})
        self.assertEqual(core._endless_mode_for_queue({"queue_id": ""}, self.redis), "library_mix")
        # A linked Person can pick their own mode.
        core._save_person_link(
            "p1", {"endless_playback_mode": "playlist_loop"}, self.redis
        )
        self.assertEqual(core._endless_mode_for_queue({"queue_id": "p1"}, self.redis), "playlist_loop")
        # An unknown mode falls back to the global setting.
        core._save_person_link("p2", {"endless_playback_mode": "bogus"}, self.redis)
        self.assertEqual(core._endless_mode_for_queue({"queue_id": "p2"}, self.redis), "library_mix")

    def test_library_mix_prioritises_least_played_and_nearby_genres(self):
        core = self.core
        tracks = [
            dict(_track_row(1, "Heard A", 180.0), artist="A", album_artist="A", genres=["reggae"], genre="reggae"),
            dict(_track_row(2, "Never Played B", 180.0), artist="B", album_artist="B", genres=["jazz"], genre="jazz"),
            dict(_track_row(3, "Never Played C", 180.0), artist="C", album_artist="C", genres=["reggae"], genre="reggae"),
            dict(_track_row(4, "Heard B", 180.0), artist="D", album_artist="D", genres=["reggae"], genre="reggae"),
        ]
        self.seed_person_catalog("p1", tracks)
        # Tracks 1 and 4 were already played; track 1 is a recent play.
        for number in (1, 4):
            core._record_listening_history(
                tracks[number - 1],
                client=self.redis,
                person_id="p1",
            )
        player = self.seed_playing_queue("p1", [tracks[0]], index=0)
        batch = core._library_mix_tracks(player, self.redis, count=4, person_id="p1")
        ids = [track["id"] for track in batch]
        # The track already playing is never appended again.
        self.assertEqual(set(ids), {"track:2", "track:3", "track:4"})
        # Genre relevance leads (the queue is listening to reggae), then the
        # least-played reggae track, and the jazz track (least-played but off
        # genre) fills the gap after them.
        self.assertEqual(ids, ["track:3", "track:4", "track:2"])

    def test_playlist_loop_mode_loops_the_chosen_mix(self):
        core = self.core
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {"endless_playback_mode": "playlist_loop", "endless_playback_playlist": "Chill Mix"},
        )
        tracks = [_track_row(1, "One"), _track_row(2, "Two"), _track_row(3, "Three")]
        self.seed_person_catalog("p1", tracks)
        core._save_json(
            self.redis,
            core._recommendations_key("p1"),
            {
                "provider": "emby",
                "playlists": [{"id": "mix1", "name": "Chill Mix", "track_ids": ["track:1", "track:2"]}],
            },
        )
        player = self.seed_playing_queue("p1", [tracks[0]], index=0)
        # A one-track refill skips what's already queued...
        batch, playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=1)
        self.assertEqual(playlist.get("name"), "Chill Mix")
        self.assertEqual([track["id"] for track in batch], ["track:2"])
        # ...and a two-track loop over a two-track playlist wraps to itself.
        batch, _playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=2)
        self.assertEqual({track["id"] for track in batch}, {"track:1", "track:2"})
        # A person-level pick overrides the global playlist name.
        core._save_person_link("p1", {"endless_playback_playlist": "Missing"}, self.redis)
        batch, _playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=2)
        self.assertEqual(batch, [])

    def test_playlist_loop_mode_loops_a_user_created_playlist(self):
        core = self.core
        tracks = [_track_row(1, "One"), _track_row(2, "Two"), _track_row(3, "Three")]
        self.seed_person_catalog(
            "p1",
            tracks,
            playlists=[{"id": "emby_playlist:pl1", "name": "Road Trip", "track_ids": ["track:1", "track:3"]}],
        )
        player = self.seed_playing_queue("p1", [tracks[0]], index=0)
        # An exact name match on a user-created (Emby) playlist loops it…
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {"endless_playback_mode": "playlist_loop", "endless_playback_playlist": "road trip"},
        )
        batch, playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=1)
        self.assertEqual(playlist.get("name"), "Road Trip")
        self.assertEqual([track["id"] for track in batch], ["track:3"])
        # …and with no pick configured the first user playlist is the fallback
        # when there are no AI mixes at all.
        core._save_hash(self.redis, core.SETTINGS_KEY, {"endless_playback_mode": "playlist_loop"})
        batch, playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=1)
        self.assertEqual(playlist.get("name"), "Road Trip")
        self.assertEqual([track["id"] for track in batch], ["track:3"])
        # Voice "add playlist" finds user playlists the same way.
        result = core._add_queue_tracks(
            {"playlist": "Road Trip"}, origin={"person_id": "p1"}, client=self.redis
        )
        self.assertEqual(result["added"], 1)
        # An unknown name still raises, naming both places it looked.
        with self.assertRaises(ValueError) as caught:
            core._add_queue_tracks(
                {"playlist": "Nowhere"}, origin={"person_id": "p1"}, client=self.redis
            )
        self.assertIn("Nowhere", str(caught.exception))

    def test_folder_playlists_are_built_from_library_folders(self):
        core = self.core
        tracks = [
            dict(_track_row(1, "Frosty"), path="/mnt/music/Christmas/Frosty.mp3"),
            dict(_track_row(2, "Jingle"), path="/mnt/music/Christmas/Kids/Jingle.mp3"),
            dict(_track_row(3, "Regular"), path="/mnt/music/Rock/Regular.mp3"),
            dict(_track_row(4, "Eve"), path="/mnt/music/Christmas Eve/Eve.mp3"),
        ]
        self.seed_person_catalog("p1", tracks)
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {"folder_playlists": "Christmas Music=Christmas, Empty=Nowhere, Bad"},
        )
        playlists = core._catalog_user_playlists(self.redis, "p1", "emby")
        self.assertEqual([row["name"] for row in playlists], ["Christmas Music"])
        christmas = playlists[0]
        # The folder's subfolders are included; a sibling folder whose name only
        # shares a prefix ("Christmas Eve") is not.
        self.assertEqual(christmas["track_ids"], ["track:1", "track:2"])
        # The endless loop and person-card options see it like any playlist.
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {
                "endless_playback_mode": "playlist_loop",
                "endless_playback_playlist": "Christmas Music",
                "folder_playlists": "Christmas Music=Christmas",
            },
        )
        player = self.seed_playing_queue("p1", [tracks[0]], index=0)
        batch, playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=1)
        self.assertEqual(playlist.get("name"), "Christmas Music")
        self.assertEqual([track["id"] for track in batch], ["track:2"])
        # Songs added to the folder join on the next lookup with no re-save.
        tracks.append(dict(_track_row(5, "New Song"), path="/mnt/music/Christmas/New.mp3"))
        self.seed_person_catalog("p1", tracks)
        with self.core._catalog_memory_cache_lock:
            self.core._catalog_memory_cache.clear()
        batch, playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=3)
        self.assertEqual(
            sorted(track["id"] for track in batch), ["track:1", "track:2", "track:5"]
        )

    def test_folder_playlist_person_override_replaces_global_list(self):
        core = self.core
        tracks = [
            dict(_track_row(1, "Frosty"), path="/mnt/music/Christmas/Frosty.mp3"),
            dict(_track_row(2, "Regular"), path="/mnt/music/Rock/Regular.mp3"),
            dict(_track_row(3, "Sweat"), path="/mnt/music/Workout/Sweat.mp3"),
        ]
        # Shared library (unlinked people) plus two linked People with the same
        # personal catalog.
        self.seed_person_catalog("", tracks)
        self.seed_person_catalog("p1", tracks)
        self.seed_person_catalog("p2", tracks)
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {"folder_playlists": "Christmas Music=Christmas"},
        )
        self.redis.hset(
            core.PERSON_LINKS_KEY,
            mapping={"p1": json.dumps({"music_source": "emby"})},
        )
        self.redis.hset(
            core.PERSON_LINKS_KEY,
            mapping={
                "p2": json.dumps(
                    {
                        "music_source": "emby",
                        "folder_playlists": "Workout Mix=Workout",
                    }
                )
            },
        )
        # The override replaces the global list for p2 alone and matches their
        # own catalog; p1 (blank override) and unlinked people keep the global.
        self.assertEqual(
            [row["name"] for row in core._catalog_user_playlists(self.redis, "p1", "emby")],
            ["Christmas Music"],
        )
        p2_playlists = core._catalog_user_playlists(self.redis, "p2", "emby")
        self.assertEqual([row["name"] for row in p2_playlists], ["Workout Mix"])
        self.assertEqual(p2_playlists[0]["track_ids"], ["track:3"])
        self.assertEqual(
            [row["name"] for row in core._catalog_user_playlists(self.redis, "", "emby")],
            ["Christmas Music"],
        )
        # The endless loop honors the override through the same path.
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {"endless_playback_mode": "playlist_loop", "endless_playback_playlist": "Workout Mix"},
        )
        player = self.seed_playing_queue("p2", [tracks[0]], index=0)
        batch, playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=2)
        self.assertEqual(playlist.get("name"), "Workout Mix")
        self.assertEqual([track["id"] for track in batch], ["track:3"])

    def test_playlist_order_setting_plays_mixes_in_a_fixed_order(self):
        core = self.core
        tracks = [
            dict(_track_row(1, "Beta"), album="Second", track_number=2, disc_number=1),
            dict(_track_row(2, "Alpha"), album="First", track_number=1, disc_number=1),
            dict(_track_row(3, "Gamma"), album="First", track_number=3, disc_number=1),
        ]
        self.seed_person_catalog("p1", tracks)
        core._save_json(
            self.redis,
            core._recommendations_key("p1"),
            {
                "provider": "emby",
                "playlists": [{"id": "mix1", "name": "Mix", "track_ids": ["track:1", "track:2", "track:3"]}],
            },
        )
        # The default stays shuffled (no fixed order applied at play time).
        core._save_hash(self.redis, core.SETTINGS_KEY, {"recommendation_playlist_order": "shuffle"})
        ordered = core._order_playlist_tracks(
            [dict(track) for track in tracks], core._playlist_order_value(core._settings(self.redis))
        )
        self.assertEqual(
            [track["id"] for track in ordered], ["track:1", "track:2", "track:3"]
        )
        # Track number ascending reorders by disc/track number.
        core._save_hash(self.redis, core.SETTINGS_KEY, {"recommendation_playlist_order": "track_asc"})
        ordered = core._order_playlist_tracks(
            [dict(track) for track in tracks], core._playlist_order_value(core._settings(self.redis))
        )
        self.assertEqual([track["id"] for track in ordered], ["track:2", "track:1", "track:3"])
        # Title descending, and the artist/album fields as tie-breakers.
        core._save_hash(self.redis, core.SETTINGS_KEY, {"recommendation_playlist_order": "title_desc"})
        ordered = core._order_playlist_tracks(
            [dict(track) for track in tracks], core._playlist_order_value(core._settings(self.redis))
        )
        self.assertEqual([track["id"] for track in ordered], ["track:3", "track:1", "track:2"])
        # The endless playlist loop follows the same fixed order.
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {
                "endless_playback_mode": "playlist_loop",
                "endless_playback_playlist": "Mix",
                "recommendation_playlist_order": "track_asc",
            },
        )
        player = self.seed_playing_queue("p1", [tracks[0]], index=0)
        batch, _playlist, _offset = core._playlist_loop_tracks(player, self.redis, count=3)
        self.assertEqual([track["id"] for track in batch], ["track:2", "track:1", "track:3"])
        # Playing the mix from the Recommendations tab queues it in that order
        # with shuffle off.
        self._originals["_resolve_targets"] = core._resolve_targets
        core._resolve_targets = lambda *args, **kwargs: ["voice_core:native:kitchen"]
        self._originals["_validate_catalog_provider_targets"] = core._validate_catalog_provider_targets
        core._validate_catalog_provider_targets = lambda targets: None
        core._save_hash(self.redis, core.SETTINGS_KEY, {"recommendation_playlist_order": "track_asc"})
        player = core._play_recommendation("recommendation:mix1", self.redis, person_id="p1")
        self.assertEqual([track["id"] for track in player["queue"]], ["track:2", "track:1", "track:3"])
        self.assertFalse(player["shuffle"])

    def test_continuation_impl_dispatches_modes_without_the_llm(self):
        core = self.core
        core._save_hash(self.redis, core.SETTINGS_KEY, {"endless_playback_mode": "library_mix"})
        tracks = [
            dict(_track_row(number, f"S{number}"), genres=["reggae"], genre="reggae")
            for number in range(1, 8)
        ]
        self.seed_person_catalog("p1", tracks)
        player = self.seed_playing_queue("p1", tracks[:3], index=2)
        loop = asyncio.new_event_loop()
        try:
            added = core._generate_continuation_impl(loop, None, player, "session-1", self.redis)
        finally:
            loop.close()
        self.assertGreater(added, 0)
        updated = core._player(self.redis, "p1")
        appended = [track["id"] for track in updated["queue"][3:]]
        self.assertTrue(appended)
        self.assertNotIn("track:1", appended)
        # The refill records which mode fed it.
        self.assertEqual(updated.get("radio_source"), "library_mix")

    def test_automatic_mode_falls_back_to_the_library_mix(self):
        core = self.core
        core._save_hash(self.redis, core.SETTINGS_KEY, {"endless_playback_mode": "automatic"})
        tracks = [
            dict(_track_row(number, f"S{number}"), genres=["jazz"], genre="jazz")
            for number in range(1, 6)
        ]
        self.seed_person_catalog("p1", tracks)
        player = self.seed_playing_queue("p1", tracks[:2], index=1)
        # No streaming provider is registered, so the library mix feeds the queue.
        fallback = core._fallback_continuation_tracks(player, self.redis, count=3)
        self.assertTrue(fallback)
        self.assertTrue(all(track["provider"] == "emby" for track in fallback))

    def test_streaming_provider_delivers_similar_tracks(self):
        core = self.core

        class FakeStreamProvider:
            provider_id = "test_stream"

            def __init__(self, settings):
                self.settings = settings

            @classmethod
            def from_settings(cls, settings):
                return cls(settings)

            @property
            def connected(self):
                return True

            def similar_tracks(self, seed_tracks, *, count=12):
                return [
                    {"id": f"stream:{index}", "title": f"Stream {index}", "provider": "test_stream"}
                    for index in range(1, count + 1)
                ]

        core.STREAMING_PROVIDER_CLASSES["test_stream"] = FakeStreamProvider
        core._save_hash(self.redis, core.SETTINGS_KEY, {"endless_playback_mode": "similar_played"})
        tracks = [_track_row(1, "Seed"), _track_row(2, "Filler")]
        self.seed_person_catalog("p1", tracks)
        player = self.seed_playing_queue("p1", tracks, index=0)
        loop = asyncio.new_event_loop()
        try:
            added = core._generate_continuation_impl(loop, None, player, "session-1", self.redis)
        finally:
            loop.close()
        self.assertGreater(added, 0)
        updated = core._player(self.redis, "p1")
        stream_tracks = [track for track in updated["queue"] if track["id"].startswith("stream:")]
        self.assertTrue(stream_tracks)
        self.assertEqual(updated.get("radio_source"), "streaming_provider")

    def test_streaming_provider_registry_empty_by_default(self):
        self.assertEqual(self.core.STREAMING_PROVIDER_CLASSES, {})
        # With no providers connected the scaffolding answers an empty list.
        self.assertEqual(
            self.core._streaming_similar_tracks([{"id": "track:1"}], client=self.redis),
            [],
        )

    # ---- Smart Shuffle ----

    def test_smart_shuffle_pushes_recent_to_back_and_windows_the_pool(self):
        core = self.core
        core._save_hash(self.redis, core.SETTINGS_KEY, {"smart_shuffle_enabled": True})
        tracks = [_track_row(number, f"Song {number}") for number in range(1, 41)]
        self.seed_person_catalog("p1", tracks)
        # The person recently played tracks 1-3; they must land at the back.
        for number in (1, 2, 3):
            core._record_listening_history(
                tracks[number - 1],
                client=self.redis,
                person_id="p1",
            )
        player = core._create_and_start_queue(
            [dict(track) for track in tracks],
            targets=["voice_core:native:kitchen"],
            shuffle=True,
            volume_percent=50,
            person_id="p1",
            client=self.redis,
        )
        self.assertTrue(player.get("smart_shuffle"))
        self.assertEqual(len(player["queue"]), core.SMART_SHUFFLE_QUEUE_WINDOW)
        self.assertEqual(len(player.get("smart_pool")), len(tracks) - core.SMART_SHUFFLE_QUEUE_WINDOW)
        # The full stored ordering (window + pool) keeps recent tracks last.
        ordering = [track["id"] for track in player["queue"]] + [
            track["id"] for track in player["smart_pool"]
        ]
        self.assertLess(ordering.index("track:1"), 40)
        # The recently played tracks fill the back (order within is shuffled).
        self.assertEqual(set(ordering[-3:]), {"track:1", "track:2", "track:3"})

    def test_smart_shuffle_off_keeps_full_queue(self):
        core = self.core
        tracks = [_track_row(number, f"Song {number}") for number in range(1, 41)]
        player = core._create_and_start_queue(
            [dict(track) for track in tracks],
            targets=["voice_core:native:kitchen"],
            shuffle=True,
            volume_percent=50,
            person_id="p1",
            client=self.redis,
        )
        self.assertFalse(player.get("smart_shuffle"))
        self.assertEqual(len(player["queue"]), len(tracks))
        self.assertEqual(player.get("smart_pool"), [])

    def test_smart_round_robin_alternates_sources(self):
        core = self.core
        pool = []
        for number in range(1, 7):
            pool.append({**_track_row(number, f"A{number}"), "source_label": "Album A"})
        for number in range(7, 13):
            pool.append({**_track_row(number, f"B{number}"), "source_label": "Playlist B"})
        selected, remaining = core._smart_round_robin(pool, 6)
        labels = [track["source_label"] for track in selected]
        self.assertEqual(
            labels,
            ["Album A", "Playlist B", "Album A", "Playlist B", "Album A", "Playlist B"],
        )
        self.assertEqual(len(remaining), 6)

    def test_add_action_pours_into_the_smart_pool(self):
        core = self.core
        core._save_hash(self.redis, core.SETTINGS_KEY, {"smart_shuffle_enabled": True})
        tracks = [_track_row(number, f"Song {number}") for number in range(1, 41)]
        self.seed_person_catalog("p1", tracks)
        player = core._create_and_start_queue(
            [dict(track) for track in tracks[:30]],
            targets=["voice_core:native:kitchen"],
            shuffle=True,
            volume_percent=50,
            person_id="p1",
            client=self.redis,
        )
        result = core._add_queue_tracks(
            {"album": "Exodus", "source_label": "Album: Exodus"},
            origin={"person_id": "p1"},
            client=self.redis,
        )
        # The 10 tracks the window had no room for wait in the pool; the top-up
        # pulls one in immediately to keep the window full.
        updated = core._player(self.redis, "p1")
        self.assertEqual(result["added"], 10)
        self.assertEqual(len(updated["smart_pool"]), 9)
        self.assertEqual(len(updated["queue"]), core.SMART_SHUFFLE_QUEUE_WINDOW + 1)
        # Without Smart Shuffle the same request appends plainly.
        core._save_hash(self.redis, core.SETTINGS_KEY, {"smart_shuffle_enabled": False})
        result = core._add_queue_tracks(
            {"album": "Exodus", "source_label": "Album: Exodus"},
            origin={"person_id": "p1"},
            client=self.redis,
        )
        self.assertEqual(result["added"], 0)  # already queued: no duplicates

    # ---- Sleep timers ----

    def test_sleep_timer_force_stops_at_zero_and_blocks_refills(self):
        core = self.core
        tracks = [_track_row(1, "One"), _track_row(2, "Two"), _track_row(3, "Three")]
        player = self.seed_playing_queue("p1", tracks, index=0)
        player["smart_pool"] = [dict(_track_row(9, "Pooled"))]
        core._save_player(player, self.redis, "p1")
        # Nothing happens before the countdown ends.
        core._set_sleep_timer(30, person_id="p1", client=self.redis)
        self.assertIsNone(core._sleep_timer_tick(self.redis, "p1"))
        self.assertEqual(core._player(self.redis, "p1")["status"], "playing")
        # At zero the stream is force-stopped and every refill path is closed.
        player = core._player(self.redis, "p1")
        player["sleep_timer_ends_at"] = time.time() - 1.0
        core._save_player(player, self.redis, "p1")
        stopped = core._sleep_timer_tick(self.redis, "p1")
        self.assertIsNotNone(stopped)
        updated = core._player(self.redis, "p1")
        self.assertEqual(updated["status"], "stopped")
        self.assertEqual(updated["sleep_timer_ends_at"], 0.0)
        self.assertFalse(updated.get("continuous_radio"))
        self.assertEqual(updated.get("smart_pool"), [])
        # A continuation worker still holding the old session token cannot append.
        stale_added = core._append_continuation_tracks(
            "session-1",
            [_track_row(5, "Late refill")],
            station_name="Radio",
            person_id="p1",
            client=self.redis,
        )
        self.assertEqual(stale_added, 0)

    def test_sleep_timer_cancel_and_voice_minutes(self):
        core = self.core
        core._set_sleep_timer(60, person_id="p1", client=self.redis)
        player = core._player(self.redis, "p1")
        self.assertTrue(core._sleep_timer_state(player)["active"])
        self.assertEqual(player["sleep_timer_minutes"], 60)
        core._set_sleep_timer(0, person_id="p1", client=self.redis)
        self.assertFalse(core._sleep_timer_state(core._player(self.redis, "p1"))["active"])
        # Requests above the cap clamp to 12 hours.
        core._set_sleep_timer(100000, person_id="p1", client=self.redis)
        self.assertEqual(core._player(self.redis, "p1")["sleep_timer_minutes"], core.SLEEP_TIMER_MAX_MINUTES)

    # ---- Multiple sources per Person ----

    def test_multi_source_link_resolution_and_merged_catalog(self):
        core = self.core
        core._save_person_link(
            "p1",
            {
                "music_source": "emby",
                "emby": {"server_url": "http://emby.local:8096", "username": "alex"},
                "extra_source": "network_share",
                "extra": {"root_path": "/mnt/music/alex-more"},
            },
            self.redis,
        )
        self.assertEqual(core._person_link_sources(core._person_link("p1", self.redis)), ["emby", "network_share"])
        # The primary and extra providers build from their own configs.
        primary = core._person_link_provider("p1", "emby", self.redis)
        extra = core._person_link_provider("p1", "network_share", self.redis)
        self.assertEqual(primary.server_url, "http://emby.local:8096")
        self.assertEqual(extra.root_path, "/mnt/music/alex-more")
        # The extra source streams (and syncs) under its own catalog slot.
        self.assertEqual(core._person_extra_slot("p1", "network_share"), "p1+network_share")
        # Merged catalog: both sources' tracks in one payload, per-track provider kept.
        self.seed_person_catalog("p1", [_track_row(1, "Emby Song")])
        core._save_json(
            self.redis,
            core._catalog_key(core._person_extra_slot("p1", "network_share")),
            {
                "provider": "network_share",
                "tracks": [
                    dict(_track_row(2, "Share Song"), provider="network_share"),
                ],
                "synced_at": time.time(),
            },
        )
        merged = core._person_catalog(self.redis, "emby", "p1")
        self.assertEqual(
            [track["title"] for track in merged["tracks"]],
            ["Emby Song", "Share Song"],
        )
        self.assertEqual([track["provider"] for track in merged["tracks"]], ["emby", "network_share"])
        # Search spans both sources.
        matches = core._search_tracks(query="share", client=self.redis, person_id="p1")
        self.assertEqual([track["title"] for track in matches], ["Share Song"])

    def test_single_source_persons_keep_their_catalog_semantics(self):
        core = self.core
        core._save_person_link(
            "p1",
            {"music_source": "network_share", "network_share": {"root_path": "/mnt/music/alex"}},
            self.redis,
        )
        self.assertEqual(core._person_catalog_source_ids("p1", self.redis), ["network_share"])
        self.assertEqual(core._person_catalog_source_ids("p2", self.redis), ["emby"])

    def test_share_stream_ids_carry_their_root(self):
        core = self.core
        scoped = core._share_scoped_stream_id("song1.mp3", "/mnt/music/alex")
        root, rel_path = core._share_root_and_relpath_from_id(scoped, "/mnt/global")
        self.assertEqual(root, "/mnt/music/alex")
        self.assertEqual(rel_path, "song1.mp3")
        # Legacy (unscoped) ids keep resolving against the global root.
        legacy = core._share_stream_id("song1.mp3")
        root, rel_path = core._share_root_and_relpath_from_id(legacy, "/mnt/global")
        self.assertEqual(root, "/mnt/global")
        self.assertEqual(rel_path, "song1.mp3")
        # Per-root artwork indexes stay separate from the household's.
        self.core._save_hash(self.redis, core.SETTINGS_KEY, {"share_root_path": "/mnt/global"})
        self.assertEqual(core._share_art_index_key("/mnt/global"), core.SHARE_ART_INDEX_KEY)
        self.assertNotEqual(core._share_art_index_key("/mnt/music/alex"), core.SHARE_ART_INDEX_KEY)

    def test_link_save_persists_the_second_source(self):
        core = self.core
        people = types.SimpleNamespace(
            load_store=lambda _client=None: {
                "people": [{"id": "person_zoe", "display_name": "Zoe"}]
            }
        )
        original_people = core._PEOPLE_API_MODULE
        core._PEOPLE_API_MODULE = people
        core._save_hash(
            self.redis,
            core.SETTINGS_KEY,
            {"emby_server_url": "http://emby.local:8096"},
        )
        try:
            result = core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "emby",
                    "person_link_emby_server_url": "http://emby.local:8096",
                    "person_link_emby_username": "alex",
                    "person_link_extra_source": "network_share",
                    "person_link_extra_share_root_path": "/mnt/music/alex-more",
                },
                self.redis,
            )
            self.assertTrue(result["ok"])
            link = core._person_link("person_zoe", self.redis)
            self.assertEqual(link.get("extra_source"), "network_share")
            self.assertEqual(link.get("extra", {}).get("root_path"), "/mnt/music/alex-more")
            # Clearing the second source removes both keys again.
            core._save_person_link_action(
                {
                    "person_link_person_id": "person_zoe",
                    "person_link_source": "emby",
                    "person_link_emby_server_url": "http://emby.local:8096",
                    "person_link_emby_username": "alex",
                    "person_link_extra_source": "",
                },
                self.redis,
            )
            link = core._person_link("person_zoe", self.redis)
            self.assertNotIn("extra_source", link)
            self.assertNotIn("extra", link)
        finally:
            core._PEOPLE_API_MODULE = original_people


if __name__ == "__main__":
    unittest.main()