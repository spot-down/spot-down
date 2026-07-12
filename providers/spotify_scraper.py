import re
import json
import requests

from . import MetadataProvider


class SpotifyScraperProvider(MetadataProvider):
    def __init__(self):
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })

    def get_track(self, track_id: str) -> dict | None:
        try:
            resp = self._session.get(
                f"https://open.spotify.com/embed/track/{track_id}",
                timeout=10
            )
            if not resp.ok:
                return None

            match = re.search(
                r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                resp.text, re.DOTALL
            )
            if not match:
                return None

            data = json.loads(match.group(1))
            entity = (
                data.get("props", {})
                .get("pageProps", {})
                .get("state", {})
                .get("data", {})
                .get("entity", {})
            )
            if not entity.get("name"):
                return None

            artists = [a["name"] for a in entity.get("artists", [])]
            rd = entity.get("releaseDate", {}) or {}
            iso = rd.get("isoString", "") if isinstance(rd, dict) else ""
            year = iso[:4] if iso else ""

            return {
                "id": track_id,
                "title": entity.get("name", "Unknown"),
                "artist": artists,
                "album": "",
                "year": year,
                "duration_ms": entity.get("duration", 0),
                "cover_url": "",
                "source": "spotify_scraper",
                "search_query": f"{artists[0] if artists else 'Unknown'} - {entity.get('name', 'Unknown')}"
            }
        except Exception:
            return None

    def get_playlist(self, playlist_id: str) -> list[dict] | None:
        tracks = []
        offset = 0
        limit = 100
        while True:
            try:
                resp = self._session.get(
                    f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?"
                    f"limit={limit}&offset={offset}&market=US",
                    timeout=10
                )
                if not resp.ok:
                    break
                data = resp.json()
            except Exception:
                break

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                t = item.get("track")
                if not t:
                    continue
                artists = [a["name"] for a in t.get("artists", [])]
                album = t.get("album", {}) or {}
                tracks.append({
                    "id": t["id"],
                    "title": t.get("name", "Unknown"),
                    "artist": artists,
                    "album": album.get("name", ""),
                    "year": (album.get("release_date", "") or "")[:4],
                    "duration_ms": t.get("duration_ms", 0),
                    "cover_url": (album.get("images", [{}])[0].get("url", "")
                                  if album.get("images") else ""),
                    "source": "spotify_scraper",
                    "search_query":
                        f"{artists[0] if artists else 'Unknown'} - {t.get('name', 'Unknown')}"
                })
            offset += len(items)
            if offset >= data.get("total", 0):
                break
        return tracks if tracks else None

    def get_tracks_batch(self, ids: list[str]) -> list[dict | None]:
        return [self.get_track(tid) for tid in ids]
