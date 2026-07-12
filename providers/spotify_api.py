import time
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

from . import MetadataProvider


class SpotifyAPIProvider(MetadataProvider):
    def __init__(self):
        self._sp = Spotify(auth_manager=SpotifyClientCredentials())

    def get_track(self, track_id: str) -> dict | None:
        for attempt in range(3):
            try:
                data = self._sp.track(track_id)
                if data:
                    return self._format_track(data)
            except SpotifyException as e:
                if e.http_status == 429:
                    backoff = min(2 ** attempt * 2, 10)
                    time.sleep(backoff)
                elif e.http_status == 403:
                    return None
                else:
                    if attempt < 2:
                        time.sleep(1)
            except Exception:
                if attempt < 2:
                    time.sleep(1)
        return None

    def get_playlist(self, playlist_id: str) -> list[dict] | None:
        try:
            data = self._sp.playlist(playlist_id, fields="tracks.items(track(id,name,artists,album,duration_ms))")
            items = data.get("tracks", {}).get("items", [])
            return [self._format_track(item["track"]) for item in items if item.get("track")]
        except SpotifyException:
            return None

    def get_tracks_batch(self, ids: list[str]) -> list[dict | None]:
        delays = [5, 10, 20]
        for attempt in range(3):
            try:
                result = self._sp.tracks(ids)
                tracks = result.get("tracks", [])
                return [self._format_track(t) if t else None for t in tracks]
            except SpotifyException as e:
                if e.http_status == 429:
                    if attempt < len(delays):
                        time.sleep(delays[attempt])
                    else:
                        break
                elif e.http_status == 403:
                    return [self.get_track(tid) for tid in ids]
                else:
                    raise
        return [self.get_track(tid) for tid in ids]

    def _format_track(self, data: dict) -> dict:
        artists = [a["name"] for a in data.get("artists", [])]
        album = data.get("album", {})
        return {
            "id": data["id"],
            "title": data.get("name", "Unknown"),
            "artist": artists,
            "album": album.get("name", ""),
            "year": (album.get("release_date", "") or "")[:4],
            "duration_ms": data.get("duration_ms", 0),
            "cover_url": album.get("images", [{}])[0].get("url", "") if album.get("images") else "",
            "source": "spotify_api",
            "search_query": f"{artists[0] if artists else 'Unknown'} - {data.get('name', 'Unknown')}"
        }
