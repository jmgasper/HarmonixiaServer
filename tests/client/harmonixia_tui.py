#!/usr/bin/env python3
"""Small curses client for browsing Harmonixia and playing tracks through ALSA."""

from __future__ import annotations

import argparse
import base64
import curses
import getpass
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from typing import Any


PAGE_SIZE = 200
PROGRESS_WRITE_INTERVAL_SECONDS = 15


class ApiError(RuntimeError):
    pass


def normalize_base_url(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("server URL is required")
    if "://" not in value:
        value = f"http://{value}"
    return value.rstrip("/")


def format_seconds(value: int | None) -> str:
    if value is None:
        return "--:--"
    value = max(0, int(value))
    hours, remainder = divmod(value, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:d}:{seconds:02d}"


def truncate(text: str, width: int) -> str:
    if width <= 0:
        return ""
    if len(text) <= width:
        return text
    if width <= 3:
        return text[:width]
    return f"{text[: width - 3]}..."


def addstr(screen: curses.window, y: int, x: int, text: str, width: int, attr: int = 0) -> None:
    if y < 0 or x < 0 or width <= 0:
        return
    try:
        screen.addstr(y, x, truncate(text, width), attr)
    except curses.error:
        pass


class HarmonixiaApi:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 30) -> None:
        self.base_url = normalize_base_url(base_url)
        self.username = username
        self.timeout = timeout
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        self.authorization = f"Basic {token}"

    def url(self, path: str, params: dict[str, Any] | None = None) -> str:
        path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{path}"
        if params:
            clean_params = {
                key: value
                for key, value in params.items()
                if value is not None and value != ""
            }
            if clean_params:
                url = f"{url}?{urllib.parse.urlencode(clean_params)}"
        return url

    def request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = None
        headers = {
            "Authorization": self.authorization,
            "Accept": "application/json",
        }
        if body is not None:
            payload = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(
            self.url(path, params),
            data=payload,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                data = response.read()
        except urllib.error.HTTPError as error:
            raise ApiError(self._format_http_error(error)) from error
        except urllib.error.URLError as error:
            raise ApiError(f"failed to connect to {self.base_url}: {error.reason}") from error

        if not data:
            return {}
        try:
            return json.loads(data.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise ApiError(f"server returned invalid JSON for {path}") from error

    def _format_http_error(self, error: urllib.error.HTTPError) -> str:
        message = error.reason
        try:
            payload = error.read()
            if payload:
                data = json.loads(payload.decode("utf-8"))
                message = data.get("message") or data.get("code") or message
        except Exception:
            pass
        return f"HTTP {error.code}: {message}"

    def auth_me(self) -> dict[str, Any]:
        return self.request_json("GET", "/api/v1/auth/me")

    def page_all(self, path: str, response_key: str, sort: str, page_size: int) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor = None
        while True:
            response = self.request_json(
                "GET",
                path,
                params={
                    "limit": max(1, min(PAGE_SIZE, page_size)),
                    "sort": sort,
                    "cursor": cursor,
                },
            )
            items.extend(response.get(response_key, []))
            page = response.get("page") or {}
            cursor = page.get("next_cursor")
            if not cursor:
                return items

    def track_stream_url(self, track_id: str, profile: str) -> str:
        track_id = urllib.parse.quote(track_id, safe="")
        profile = urllib.parse.quote(profile, safe="")
        return self.url(f"/api/v1/media/track/{track_id}/transcode/{profile}")

    def write_track_progress(
        self,
        track: dict[str, Any],
        position_seconds: int,
        completed: bool,
    ) -> None:
        duration = track.get("duration_seconds")
        duration_seconds = int(duration) if duration is not None else None
        position_seconds = max(0, int(position_seconds))
        if duration_seconds is not None:
            position_seconds = min(position_seconds, duration_seconds)

        self.request_json(
            "PUT",
            f"/api/v1/me/playback/progress/track/{urllib.parse.quote(str(track['id']), safe='')}",
            body={
                "position_seconds": position_seconds,
                "duration_seconds": duration_seconds,
                "completed": completed,
            },
        )


@dataclass
class Catalog:
    artists: list[dict[str, Any]]
    albums: list[dict[str, Any]]
    tracks: list[dict[str, Any]]
    albums_by_artist: dict[str, list[dict[str, Any]]]
    tracks_by_album: dict[str, list[dict[str, Any]]]

    @classmethod
    def load(cls, api: HarmonixiaApi, page_size: int) -> "Catalog":
        artists = api.page_all("/api/v1/catalog/artists", "artists", "name", page_size)
        albums = api.page_all("/api/v1/catalog/albums", "albums", "artist_title", page_size)
        tracks = api.page_all("/api/v1/catalog/tracks", "tracks", "album_position", page_size)

        albums_by_artist: dict[str, list[dict[str, Any]]] = defaultdict(list)
        tracks_by_album: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for album in albums:
            albums_by_artist[str(album["artist_id"])].append(album)
        for track in tracks:
            tracks_by_album[str(track["album_id"])].append(track)

        return cls(
            artists=artists,
            albums=albums,
            tracks=tracks,
            albums_by_artist=dict(albums_by_artist),
            tracks_by_album=dict(tracks_by_album),
        )


class AlsaPlayer:
    def __init__(self, api: HarmonixiaApi, profile: str, alsa_device: str) -> None:
        self.api = api
        self.profile = profile
        self.alsa_device = alsa_device
        self.track: dict[str, Any] | None = None
        self.started_at = 0.0
        self.last_progress_at = 0.0
        self.ffmpeg: subprocess.Popen[bytes] | None = None
        self.aplay: subprocess.Popen[bytes] | None = None
        self.last_error: str | None = None

    def start(self, track: dict[str, Any]) -> None:
        self.stop(write_progress=True, completed=False)
        self._require_playback_tools()

        ffmpeg_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-headers",
            f"Authorization: {self.api.authorization}\r\n",
            "-i",
            self.api.track_stream_url(str(track["id"]), self.profile),
            "-vn",
            "-f",
            "s16le",
            "-acodec",
            "pcm_s16le",
            "-ac",
            "2",
            "-ar",
            "44100",
            "-",
        ]
        aplay_cmd = [
            "aplay",
            "-q",
            "-D",
            self.alsa_device,
            "-f",
            "S16_LE",
            "-c",
            "2",
            "-r",
            "44100",
        ]

        try:
            self.ffmpeg = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            assert self.ffmpeg.stdout is not None
            self.aplay = subprocess.Popen(
                aplay_cmd,
                stdin=self.ffmpeg.stdout,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.ffmpeg.stdout.close()
        except OSError as error:
            self._terminate_processes()
            raise RuntimeError(f"failed to start playback: {error}") from error

        self.track = track
        self.started_at = time.monotonic()
        self.last_progress_at = 0.0
        self.last_error = None
        self.write_progress(completed=False, force=True)

    def stop(self, write_progress: bool, completed: bool) -> None:
        if self.track is not None and write_progress:
            self.write_progress(completed=completed, force=True)
        self._terminate_processes()
        self.track = None
        self.started_at = 0.0
        self.last_progress_at = 0.0

    def tick(self) -> tuple[dict[str, Any], bool] | None:
        if self.track is None:
            return None

        now = time.monotonic()
        if now - self.last_progress_at >= PROGRESS_WRITE_INTERVAL_SECONDS:
            self.write_progress(completed=False, force=True)

        if not self.finished:
            return None

        track = self.track
        completed = self.looks_complete
        self.write_progress(completed=completed, force=True)
        self._terminate_processes()
        self.track = None
        return track, completed

    def write_progress(self, completed: bool, force: bool = False) -> None:
        if self.track is None:
            return
        now = time.monotonic()
        if not force and now - self.last_progress_at < PROGRESS_WRITE_INTERVAL_SECONDS:
            return
        try:
            self.api.write_track_progress(self.track, self.position_seconds, completed)
            self.last_progress_at = now
        except ApiError as error:
            self.last_error = str(error)

    @property
    def position_seconds(self) -> int:
        if self.track is None or self.started_at == 0.0:
            return 0
        return max(0, int(time.monotonic() - self.started_at))

    @property
    def duration_seconds(self) -> int | None:
        if self.track is None:
            return None
        duration = self.track.get("duration_seconds")
        return int(duration) if duration is not None else None

    @property
    def finished(self) -> bool:
        return (
            self.ffmpeg is not None
            and self.aplay is not None
            and self.ffmpeg.poll() is not None
            and self.aplay.poll() is not None
        )

    @property
    def looks_complete(self) -> bool:
        duration = self.duration_seconds
        position = self.position_seconds
        if duration is None:
            return position > 3
        return position >= max(0, duration - 3)

    def _require_playback_tools(self) -> None:
        missing = [tool for tool in ("ffmpeg", "aplay") if shutil.which(tool) is None]
        if missing:
            raise RuntimeError(f"missing playback tool(s): {', '.join(missing)}")

    def _terminate_processes(self) -> None:
        processes = [self.aplay, self.ffmpeg]
        for process in processes:
            if process is not None and process.poll() is None:
                process.terminate()
        deadline = time.monotonic() + 1.5
        for process in processes:
            if process is None:
                continue
            while process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            if process.poll() is None:
                process.kill()
        for process in processes:
            if process is not None:
                try:
                    process.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    pass
        self.ffmpeg = None
        self.aplay = None


class HarmonixiaTui:
    def __init__(self, screen: curses.window, api: HarmonixiaApi, catalog: Catalog, player: AlsaPlayer) -> None:
        self.screen = screen
        self.api = api
        self.catalog = catalog
        self.player = player
        self.view = "artists"
        self.running = True
        self.status = ""
        self.selected_artist: dict[str, Any] | None = None
        self.selected_album: dict[str, Any] | None = None
        self.artist_index = 0
        self.album_index = 0
        self.track_index = 0
        self.offsets = {
            "artists": 0,
            "albums": 0,
            "tracks": 0,
            "playback": 0,
        }

    def run(self) -> None:
        curses.curs_set(0)
        self.screen.keypad(True)
        self.screen.nodelay(True)
        curses.use_default_colors()

        try:
            while self.running:
                self._tick()
                self._draw()
                key = self.screen.getch()
                if key != -1:
                    self._handle_key(key)
                time.sleep(0.08)
        finally:
            self.player.stop(write_progress=True, completed=False)

    def _tick(self) -> None:
        finished = self.player.tick()
        if finished is None:
            return
        track, completed = finished
        if completed:
            self._advance_track(1, automatic=True)
        else:
            self.status = f"Playback stopped early: {track_label(track)}"
            self.view = "tracks"

    def _handle_key(self, key: int) -> None:
        if key in (ord("q"), ord("Q")):
            self.running = False
            return
        if key in (ord("n"), ord("N")):
            self._advance_track(1, automatic=False)
            return
        if key in (ord("p"), ord("P")):
            self._advance_track(-1, automatic=False)
            return
        if key in (ord("b"), ord("B"), 27):
            self._go_back()
            return
        if key in (curses.KEY_UP, ord("k"), ord("K")):
            self._move_selection(-1)
            return
        if key in (curses.KEY_DOWN, ord("j"), ord("J")):
            self._move_selection(1)
            return
        if key in (curses.KEY_NPAGE,):
            self._move_selection(10)
            return
        if key in (curses.KEY_PPAGE,):
            self._move_selection(-10)
            return
        if key in (curses.KEY_ENTER, 10, 13):
            self._select_current()

    def _go_back(self) -> None:
        if self.view == "playback":
            self.player.stop(write_progress=True, completed=False)
            self.view = "tracks"
        elif self.view == "tracks":
            self.view = "albums"
        elif self.view == "albums":
            self.view = "artists"

    def _move_selection(self, delta: int) -> None:
        items = self._current_items()
        if not items:
            return
        attr = self._index_attr()
        current = getattr(self, attr)
        next_index = max(0, min(len(items) - 1, current + delta))
        setattr(self, attr, next_index)

    def _select_current(self) -> None:
        if self.view == "artists":
            artists = self.catalog.artists
            if not artists:
                return
            self.selected_artist = artists[self.artist_index]
            self.album_index = 0
            self.view = "albums"
            self.status = ""
            return

        if self.view == "albums":
            albums = self._albums_for_selected_artist()
            if not albums:
                self.status = "No albums for this artist"
                return
            self.selected_album = albums[self.album_index]
            self.track_index = 0
            tracks = self._tracks_for_selected_album()
            if not tracks:
                self.status = "No tracks for this album"
                self.view = "tracks"
                return
            self._play_track(0)
            return

        if self.view in ("tracks", "playback"):
            tracks = self._tracks_for_selected_album()
            if not tracks:
                self.status = "No tracks for this album"
                return
            self._play_track(self.track_index)

    def _advance_track(self, delta: int, automatic: bool) -> None:
        tracks = self._tracks_for_selected_album()
        if not tracks:
            return

        current_id = self.player.track.get("id") if self.player.track is not None else None
        if current_id is not None:
            for index, track in enumerate(tracks):
                if track.get("id") == current_id:
                    self.track_index = index
                    break

        next_index = (self.track_index + delta) % len(tracks)
        self._play_track(next_index)
        if automatic:
            self.status = "Started next track"

    def _play_track(self, index: int) -> None:
        tracks = self._tracks_for_selected_album()
        if not tracks:
            return
        self.track_index = max(0, min(len(tracks) - 1, index))
        track = tracks[self.track_index]
        try:
            self.player.start(track)
            self.view = "playback"
            self.status = f"Playing: {track_label(track)}"
        except (RuntimeError, ApiError) as error:
            self.status = str(error)

    def _draw(self) -> None:
        self.screen.erase()
        height, width = self.screen.getmaxyx()
        if height < 8 or width < 30:
            addstr(self.screen, 0, 0, "Terminal too small", width)
            self.screen.refresh()
            return

        addstr(
            self.screen,
            0,
            0,
            f"Harmonixia TUI - {self.api.username}@{self.api.base_url}",
            width,
            curses.A_BOLD,
        )
        addstr(self.screen, 1, 0, self._title(), width, curses.A_BOLD)
        if self.status:
            addstr(self.screen, 2, 0, self.status, width)
        elif self.player.last_error:
            addstr(self.screen, 2, 0, f"Progress update failed: {self.player.last_error}", width)

        if self.view == "playback":
            self._draw_playback(height, width)
        else:
            self._draw_list(height, width, top=4, bottom=2)

        footer = "Enter select/play | b back | n next | p previous | q quit"
        addstr(self.screen, height - 1, 0, footer, width, curses.A_REVERSE)
        self.screen.refresh()

    def _draw_playback(self, height: int, width: int) -> None:
        y = 4
        track = self.player.track
        if track is not None:
            addstr(self.screen, y, 0, track_label(track), width, curses.A_BOLD)
            y += 1
            position = self.player.position_seconds
            duration = self.player.duration_seconds
            meter = self._progress_meter(position, duration, max(10, width - 18))
            addstr(
                self.screen,
                y,
                0,
                f"{format_seconds(position)} {meter} {format_seconds(duration)}",
                width,
            )
            y += 2
        self._draw_list(height, width, top=y, bottom=2)

    def _draw_list(self, height: int, width: int, top: int, bottom: int) -> None:
        items = self._current_items()
        index = getattr(self, self._index_attr())
        offset_key = self.view
        visible_rows = max(1, height - top - bottom)
        offset = self.offsets[offset_key]
        if index < offset:
            offset = index
        elif index >= offset + visible_rows:
            offset = index - visible_rows + 1
        offset = max(0, offset)
        self.offsets[offset_key] = offset

        if not items:
            addstr(self.screen, top, 0, self._empty_message(), width)
            return

        for row in range(visible_rows):
            item_index = offset + row
            if item_index >= len(items):
                break
            item = items[item_index]
            attr = curses.A_REVERSE if item_index == index else 0
            addstr(
                self.screen,
                top + row,
                0,
                f"{item_index + 1:4d}  {self._label_for_item(item)}",
                width,
                attr,
            )

    def _progress_meter(self, position: int, duration: int | None, width: int) -> str:
        width = max(10, min(60, width))
        if duration is None or duration <= 0:
            filled = position % width
        else:
            filled = min(width, int(width * min(position, duration) / duration))
        return f"[{'#' * filled}{'-' * (width - filled)}]"

    def _current_items(self) -> list[dict[str, Any]]:
        if self.view == "artists":
            return self.catalog.artists
        if self.view == "albums":
            return self._albums_for_selected_artist()
        return self._tracks_for_selected_album()

    def _index_attr(self) -> str:
        if self.view == "artists":
            return "artist_index"
        if self.view == "albums":
            return "album_index"
        return "track_index"

    def _albums_for_selected_artist(self) -> list[dict[str, Any]]:
        if self.selected_artist is None:
            return []
        return self.catalog.albums_by_artist.get(str(self.selected_artist["id"]), [])

    def _tracks_for_selected_album(self) -> list[dict[str, Any]]:
        if self.selected_album is None:
            return []
        return self.catalog.tracks_by_album.get(str(self.selected_album["id"]), [])

    def _label_for_item(self, item: dict[str, Any]) -> str:
        if self.view == "artists":
            return artist_label(item)
        if self.view == "albums":
            return album_label(item)
        return track_label(item)

    def _title(self) -> str:
        if self.view == "artists":
            return f"Artists ({len(self.catalog.artists)})"
        if self.view == "albums":
            artist = artist_label(self.selected_artist) if self.selected_artist else ""
            return f"Albums for {artist} ({len(self._albums_for_selected_artist())})"
        if self.view == "tracks":
            album = album_label(self.selected_album) if self.selected_album else ""
            return f"Tracks on {album} ({len(self._tracks_for_selected_album())})"
        album = album_label(self.selected_album) if self.selected_album else ""
        return f"Playback - {album}"

    def _empty_message(self) -> str:
        if self.view == "artists":
            return "No artists returned by the server"
        if self.view == "albums":
            return "No albums for this artist"
        return "No tracks for this album"


def artist_label(artist: dict[str, Any] | None) -> str:
    if not artist:
        return ""
    return str(artist.get("name") or artist.get("id") or "Unknown artist")


def album_label(album: dict[str, Any] | None) -> str:
    if not album:
        return ""
    title = str(album.get("title") or album.get("id") or "Unknown album")
    year = album.get("release_year")
    if year:
        return f"{title} ({year})"
    return title


def track_label(track: dict[str, Any]) -> str:
    title = str(track.get("title") or track.get("id") or "Unknown track")
    disc = track.get("disc_number")
    number = track.get("track_number")
    if disc and number:
        prefix = f"{int(disc)}.{int(number):02d} "
    elif number:
        prefix = f"{int(number):02d} "
    else:
        prefix = ""
    duration = format_seconds(track.get("duration_seconds"))
    return f"{prefix}{title}  {duration}"


def prompt_credentials(server_arg: str | None) -> tuple[str, str, str]:
    server = server_arg or input("Server URL: ").strip()
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    return server, username, password


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Browse Harmonixia and play tracks through ALSA.",
    )
    parser.add_argument("server", nargs="?", help="Harmonixia server URL, for example http://127.0.0.1:3000")
    parser.add_argument("--profile", choices=("mobile", "standard", "high"), default="standard")
    parser.add_argument("--alsa-device", default=os.environ.get("ALSA_DEVICE", "default"))
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        server, username, password = prompt_credentials(args.server)
        api = HarmonixiaApi(server, username, password)
        print("Authenticating...")
        api.auth_me()
        print("Loading catalog...")
        catalog = Catalog.load(api, args.page_size)
        player = AlsaPlayer(api, args.profile, args.alsa_device)

        def wrapped(screen: curses.window) -> None:
            HarmonixiaTui(screen, api, catalog, player).run()

        curses.wrapper(wrapped)
    except KeyboardInterrupt:
        return 130
    except (ApiError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
