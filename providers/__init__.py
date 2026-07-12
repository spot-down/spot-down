from abc import ABC, abstractmethod

class MetadataProvider(ABC):
    @abstractmethod
    def get_track(self, track_id: str) -> dict | None:
        pass

    @abstractmethod
    def get_playlist(self, playlist_id: str) -> list[dict] | None:
        pass

    @abstractmethod
    def get_tracks_batch(self, ids: list[str]) -> list[dict | None]:
        pass


def extract_track_id(url_or_id: str) -> str | None:
    id_candidate = url_or_id.strip().split("/")[-1].split("?")[0]
    if id_candidate and len(id_candidate) == 22 and id_candidate.isalnum():
        return id_candidate
    return None


def extract_playlist_id(url: str) -> str | None:
    if "playlist" not in url:
        return None
    try:
        return url.split("playlist/")[1].split("?")[0]
    except IndexError:
        return None
