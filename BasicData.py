
from collections.abc import Set
from typing import Any, Dict, List
from bs4 import BeautifulSoup
import requests
from shazamio import Shazam, HTTPClient
from shazamio.exceptions import FailedDecodeJson
from aiohttp_retry import ExponentialRetry
import aiohttp
import asyncio
from prisma import Prisma
import random
import json
import spotipy
from datetime import datetime, timedelta
import traceback
import urllib3
import os
import re
import time
from spotipy.oauth2 import SpotifyClientCredentials
import spotipy
from collections import Counter
from urllib.parse import quote_plus
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from dotenv import load_dotenv
load_dotenv() 

christmas_songs = [
    'Jingle Bells',
    'Silent Night (Stille Nacht)',
    'O Holy Night',
    'Deck the Halls',
    'Hark! The Herald Angels Sing',
    'Joy to the World',
    'The First Noel',
    'Feliz Navidad',
    'Rudolph the Red-Nosed Reindeer',
    'Frosty the Snowman',
    'Have Yourself a Merry Little Christmas',
    'White Christmas',
    'It\'s Beginning to Look a Lot Like Christmas',
    'Rockin\' Around the Christmas Tree',
    'Santa Claus Is Coming to Town',
    'Let It Snow! Let It Snow! Let It Snow!',
    'All I Want for Christmas Is You',
    'The Christmas Song (Chestnuts Roasting on an Open Fire)',
    'Winter Wonderland',
    'Do They Know It\'s Christmas?',
    'O Tannenbaum',
    'Leise rieselt der Schnee',
    'Kling, Glöckchen, klingelingeling',
    'Es ist ein Ros entsprungen',
    'Ihr Kinderlein, kommet',
    'Weihnacht',
    'Christmas',
    'Christmas Tree',
    'Santa',
    'X-Mas',
    'X-mas',
    'Xmas',
    'xmas',
    'Glöckchen',
    'Schnee',
    'Snow',
    'Bells',
    'Rudolph'
]

halloween_songs = [
    'Thriller',
    'Ghostbusters',
    'Monster Mash',
    'Somebody\'s Watching Me',
    'This Is Halloween',
    'Highway to Hell',
    'Disturbia',
    'Spooky, Scary Skeletons',
    'Time Warp',
    'Superstition',
    'Black Magic Woman',
    'Bury a Friend',
    'Sympathy for the Devil',
    'People Are Strange',
    'Dead Man\'s Party',
    'Season of the Witch',
    'I Put a Spell on You',
    'Dragula',
    'Dracula',
    'Werewolves of London',
    'Bark at the Moon',
    'Die Hexen kommen',
    'Totentanz',
    'Mein Teil',
    'Geist',
    'Zombie',
    'Ghost',
    'Dracula',
    'Skeleton',
    'Halloween',
    'Witch',
    'Hexe'
]

easter_songs = [
    'Christ the Lord Is Risen Today',
    'Because He Lives',
    'Up from the Grave He Arose',
    'He Lives',
    'In Christ Alone',
    'The Old Rugged Cross',
    'Were You There?',
    'Glorious Day (Living He Loved Me)',
    'My Redeemer Lives',
    'Amazing Grace',
    'Hosanna',
    'At the Cross',
    'How Great Thou Art',
    'O Praise the Name (Anástasis)',
    'Via Dolorosa',
    'Lead Me to the Cross',
    'The Lion and the Lamb',
    'Easter Song',
    'Is He Worthy?',
    'Christ ist erstanden',
    'Erstanden ist der heilig Christ',
    'Wir wollen alle fröhlich sein',
    'Ostern',
    'Easter',
    'Osterhase',
    'Easter Bunny'
]

wedding_songs = [
    'At Last',
    'All of Me',
    'A Thousand Years',
    'Marry Me',
    'Perfect',
    'Canon in D',
    'Thinking Out Loud',
    'I Will Always Love You',
    'Can\'t Help Falling in Love',
    'Make You Feel My Love',
    'The Way You Look Tonight',
    'Endless Love',
    'You Are the Best Thing',
    'Here Comes the Sun',
    'Your Song',
    'I Don\'t Want to Miss a Thing',
    'How Long Will I Love You',
    'From This Moment On',
    'Love on Top',
    'I Choose You',
    ' Ja ',
    'Dir gehört mein Herz',
    'Liebe ist',
    'Du lässt mich sein, so wie ich bin',
    'Warum hast du nicht nein gesagt',
    'Hochzeit',
    'Wedding'
]

silvester_songs = [
    "Silvester",
    "Neujahr",
    "Neujahrs",
    "Guten Rutsch",
    "Happy New Year",
    "New Year",
    "New Year's",
    "Auld Lang Syne",
    "Countdown",
    "Jahreswechsel",
    "Feuerwerk",
    "Fireworks",
    "Mitternacht",
    "Midnight",
    "Champagne",
    "Sekt",
    "Prosit Neujahr",
]

valentines_songs = [
    "Valentinstag",
    "Valentine",
    "Valentines",
    "Valentine's",
    "Valentine’s",
    "Valentine's Day",
    "Be My Valentine",
    "My Funny Valentine",
    "Valentine Day",
]

fasching_songs = [
    "Fasching",
    "Karneval",
    "Fastnacht",
    "Rosenmontag",
    "Weiberfastnacht",
    "Helau",
    "Alaaf",
    "Jeck",
    "Narr",
    "Narren",
    "Mardi Gras",
    "Carnival",
    "Polonaise",
    "Schunkeln",
    "Viva Colonia",
    "Kölle Alaaf",
]

oktoberfest_songs = [
    "Oktoberfest",
    "Wiesn",
    "Wies'n",
    "Bierzelt",
    "Ein Prosit",
    "O'zapft",
    "O'zapft is",
    "Dirndl",
    "Lederhosen",
    "Maß",
    "Masskrug",
    "Maßkrug",
    "Beerfest",
]

async def get_shazam_tracks(song_data):

  # Robust Shazam lookup.
  # Uses shazamio HTTPClient retries (see shazam init below) and additionally
  # retries on FailedDecodeJson / ContentType errors which can happen after 429/5xx.

  title = (song_data.get('song_title') or "").replace("#", "").replace("%", "").strip()
  artist = (song_data.get('artist') or "").replace("#", "").replace("%", "").strip()

  if not title or not artist:
    return []

  query = f"{artist} {title}".strip()

  shazam_tracks = None
  try:
    shazam_tracks = await shazam.search_track(query, limit=5)
  except Exception as e:
    # shazamio sometimes bubbles decode problems as generic Exceptions
    msg = str(e).lower()
    print(f"[Shazam] Unexpected error for '{query}': {e}")
    return []

  hits = (((shazam_tracks or {}).get('tracks') or {}).get('hits') or [])
  return_tracks = []

  for track in hits:
    heading = track.get('heading') or {}
    stores = track.get('stores') or {}
    images = track.get('images') or {}

    current_track = {
      'artist': heading.get('subtitle', "") or "",
      'title': heading.get('title', "") or "",
      'apple_url': "",
      'image': images.get('default', "") or "",
    }

    apple = stores.get('apple')
    if apple:
      for action in (apple.get('actions') or []):
        if action.get('type') == 'uri' and action.get('uri'):
          current_track['apple_url'] = action['uri']
          break

    return_tracks.append(current_track)

  return return_tracks

_SPOTIFY = None
_ARTIST_GENRE_CACHE: dict[str, list[str]] = {}


def _get_spotify_client() -> spotipy.Spotify:
    global _SPOTIFY
    if _SPOTIFY is None:
        client_id = os.getenv("SPOTIPY_CLIENT_ID")
        client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError(
                "Missing SPOTIPY_CLIENT_ID / SPOTIPY_CLIENT_SECRET in env/.env"
            )
        auth = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        _SPOTIFY = spotipy.Spotify(auth_manager=auth)
    return _SPOTIFY


def _get_artist_genres(spotify: spotipy.Spotify, artist_id: str) -> list[str]:
    if not artist_id:
        return []
    cached = _ARTIST_GENRE_CACHE.get(artist_id)
    if cached is not None:
        return cached
    try:
        data = spotify.artist(artist_id)
        genres = data.get("genres") or []
    except Exception:
        genres = []
    _ARTIST_GENRE_CACHE[artist_id] = genres
    return genres


def _pick_primary_genre(genres: list[str]) -> str:
    # pick the most frequent genre across artists (or empty)
    if not genres:
        return ""
    counts = Counter([g.strip().lower() for g in genres if g and g.strip()])
    return counts.most_common(1)[0][0] if counts else ""


async def get_spotify_tracks(song_data, limit: int = 5):
    spotify = _get_spotify_client()

    title = (song_data.get("song_title") or song_data.get("title") or "").replace("#", "").replace("%", "").strip()
    artist = (song_data.get("artist") or "").replace("#", "").replace("%", "").strip()
    if not title or not artist:
        return []

    results = spotify.search(q=f"{artist} {title}", limit=limit, type="track")
    track_info_list = []

    for track in (results.get("tracks") or {}).get("items") or []:
        track_name = track.get("name") or ""
        track_url = ((track.get("external_urls") or {}).get("spotify")) or ""

        artists = track.get("artists") or []
        artist_names = [a.get("name") for a in artists if a.get("name")]
        artist_ids = [a.get("id") for a in artists if a.get("id")]

        # album art + release
        album = track.get("album") or {}
        images = album.get("images") or []
        image_url = images[0].get("url") if images else ""
        release = album.get("release_date") or ""

        # NEW: genres (from artists)
        all_genres: list[str] = []
        for aid in artist_ids:
            all_genres.extend(_get_artist_genres(spotify, aid))

        # normalize + dedupe (keep order)
        seen = set()
        genres_unique = []
        for g in all_genres:
            g2 = (g or "").strip().lower()
            if g2 and g2 not in seen:
                seen.add(g2)
                genres_unique.append(g2)

        track_info_list.append(
            {
                "title": track_name,
                "spotify_url": track_url,
                "artists": artist_names,
                "image": image_url,
                "release": release,
                "genres": genres_unique,                 # NEW
                "genre": _pick_primary_genre(genres_unique),  # NEW
            }
        )

    return track_info_list


def _norm_text(s):
  if s is None:
    return ""
  s = str(s).lower().strip()
  s = s.replace("’", "'")
  out = []
  for ch in s:
    if ch.isalnum() or ch.isspace():
      out.append(ch)
  return " ".join("".join(out).split())


_ITUNES_SEARCH_URL = "https://itunes.apple.com/search"

async def get_itunes_tracks(song_data, country="de", limit=5):

  title = (song_data.get('song_title') or song_data.get('title') or "").replace("#", "").replace("%", "").strip()
  artist = (song_data.get('artist') or "").replace("#", "").replace("%", "").strip()

  if not title or not artist:
    return []

  term = f"{artist} {title}".strip()
  params = {
    "term": term,
    "media": "music",
    "entity": "song",
    "limit": limit,
    "country": country,
  }

  timeout = aiohttp.ClientTimeout(total=20)
  data = None

  async with aiohttp.ClientSession(timeout=timeout) as session:
    for attempt in range(5):
      try:
        async with session.get(_ITUNES_SEARCH_URL, params=params) as resp:
          if resp.status in (429, 500, 502, 503, 504):
            if attempt == 4:
              print(f"[iTunes] Failed for '{term}': HTTP {resp.status}")
              return []
            continue

          data = await resp.json(content_type=None)
        break
      except (aiohttp.ClientError, aiohttp.ContentTypeError, asyncio.TimeoutError, ValueError) as e:
        if attempt == 4:
          print(f"[iTunes] Failed for '{term}': {e}")
          return []

  results = (data or {}).get("results") or []
  out = []

  for r in results:
    release_iso = r.get("releaseDate") or ""
    release = ""
    if isinstance(release_iso, str) and len(release_iso) >= 10:
      release = release_iso[:10]  # YYYY-MM-DD

    out.append(
      {
        "title": r.get("trackName") or "",
        "artist": r.get("artistName") or "",
        "itunes_url": r.get("trackViewUrl") or "",
        "preview_url": r.get("previewUrl") or "",
        "image": r.get("artworkUrl100") or "",
        "release": release,
        "track_id": r.get("trackId") or "",
        "genre": r.get("primaryGenreName") or "",
      }
    )

  return out


async def get_apple_music_tracks(song_data, storefront="de", limit=5):

  dev_token = os.getenv("APPLE_MUSIC_DEV_TOKEN")
  if not dev_token:
    # Apple Music API needs a developer token; keep it silent here so BasicData can still run without it.
    return []

  title = (song_data.get('song_title') or song_data.get('title') or "").replace("#", "").replace("%", "").strip()
  artist = (song_data.get('artist') or "").replace("#", "").replace("%", "").strip()

  if not title or not artist:
    return []

  term = f"{artist} {title}".strip()
  url = f"https://api.music.apple.com/v1/catalog/{storefront}/search"
  headers = {"Authorization": f"Bearer {dev_token}"}
  params = {
    "term": term,
    "types": "songs",
    "limit": limit,
  }

  timeout = aiohttp.ClientTimeout(total=20)
  data = None

  async with aiohttp.ClientSession(timeout=timeout) as session:
    for attempt in range(5):
      try:
        async with session.get(url, params=params, headers=headers) as resp:
          if resp.status in (401, 403):
            # invalid/expired token
            if attempt == 0:
              print(f"[AppleMusic] Token rejected (HTTP {resp.status}). Set APPLE_MUSIC_DEV_TOKEN.")
            return []

          if resp.status in (429, 500, 502, 503, 504):
            if attempt == 4:
              print(f"[AppleMusic] Failed for '{term}': HTTP {resp.status}")
              return []
            continue

          data = await resp.json(content_type=None)
        break
      except (aiohttp.ClientError, aiohttp.ContentTypeError, asyncio.TimeoutError, ValueError) as e:
        if attempt == 4:
          print(f"[AppleMusic] Failed for '{term}': {e}")
          return []

  songs = (((data or {}).get("results") or {}).get("songs") or {}).get("data") or []
  out = []

  for item in songs:
    attrs = item.get("attributes") or {}
    art = attrs.get("artwork") or {}
    img = art.get("url") or ""
    genre_names = attrs.get("genreNames") or []
    genre = genre_names[0] if genre_names else ""
    if img:
      img = img.replace("{w}", "500").replace("{h}", "500")

    release = attrs.get("releaseDate") or ""
    if isinstance(release, str) and len(release) >= 10:
      release = release[:10]
    else:
      release = ""

    out.append(
      {
        "title": attrs.get("name") or "",
        "artist": attrs.get("artistName") or "",
        "apple_music_url": attrs.get("url") or "",
        "image": img,
        "release": release,
        "id": item.get("id") or "",
        "genre": genre,
      }
    )

  return out

def _extract_yt_initial_data(html: str) -> dict | None:
    """
    Robust-ish extractor for ytInitialData JSON by brace matching.
    """
    # find the first occurrence of "ytInitialData"
    idx = html.find("ytInitialData")
    if idx == -1:
        return None

    # find the first '{' after that
    start = html.find("{", idx)
    if start == -1:
        return None

    depth = 0
    for i in range(start, len(html)):
        ch = html[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                blob = html[start : i + 1]
                try:
                    return json.loads(blob)
                except Exception:
                    return None
    return None

def _walk_video_renderers(data: dict) -> list[dict]:
    """
    Pull videoRenderer objects out of YouTube search results JSON.
    """
    out = []

    try:
        contents = data["contents"]["twoColumnSearchResultsRenderer"]["primaryContents"][
            "sectionListRenderer"
        ]["contents"]
    except Exception:
        return out

    # traverse possible structures
    for c in contents:
        item_section = c.get("itemSectionRenderer")
        if not item_section:
            continue
        for item in item_section.get("contents", []) or []:
            vr = item.get("videoRenderer")
            if vr:
                out.append(vr)
    return out

def _score_yt_title(title: str) -> int:
    t = (title or "").lower()

    score = 0
    # strongly prefer lyrics/lyric video
    if "lyric video" in t:
        score += 6
    if "lyrics" in t or "lyric" in t:
        score += 4
    if "songtext" in t or "lyrics deutsch" in t:
        score += 2

    # avoid mismatches
    if "live" in t:
        score -= 4
    if "cover" in t:
        score -= 4
    if "karaoke" in t:
        score -= 3
    if "reaction" in t:
        score -= 3

    # “official video” often is not what you want (but don’t hard-ban it)
    if "official video" in t:
        score -= 2

    return score

def find_youtube_lyrics_video_url(song_title: str, artist: str, timeout_s: int = 15) -> str:
  """
  Returns a YouTube URL pointing to a lyrics/lyric-video candidate for the song.
  NOTE: this scrapes YouTube HTML and can break if YouTube changes markup.
  """
  title = (song_title or "").strip()
  art = (artist or "").strip()
  if not title or not art:
    return ""

  query = f'{art} {title} "lyric video"'
  url = "https://www.youtube.com/results?search_query=" + quote_plus(query)

  headers = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
  }

  r = requests.get(url, headers=headers, timeout=timeout_s)
  if r.status_code != 200:
    return ""

  data = _extract_yt_initial_data(r.text)
  if not data:
    return ""

  videos = _walk_video_renderers(data)
  if not videos:
    return ""

  best = ("", -10**9)

  for vr in videos[:20]:
    vid = vr.get("videoId") or ""
    runs = (((vr.get("title") or {}).get("runs")) or [])
    title_text = runs[0].get("text") if runs else ""
    if not vid or not title_text:
      continue

    score = _score_yt_title(title_text)

    # small bonus if artist name appears
    if art.lower() in title_text.lower():
      score += 2

    if score > best[1]:
      best = (vid, score)

  video_id = best[0]
  return f"https://www.youtube.com/watch?v={video_id}" if video_id else ""

def _norm(s: str | None) -> str:
  if not s:
      return ""
  s = s.lower().strip()
  s = s.replace("’", "'")
  s = re.sub(r"\s+", " ", s)
  return s

def _song_key(song: dict) -> tuple[str, str]:
  return (_norm(song.get("title")), _norm(song.get("artist")))

def add_previous_placement(charts_data):

  # First chart: no previous
  for song in charts_data[0].get("songs", []):
    song["previous_position"] = -1

  for i in range(1, len(charts_data)):
    current_chart = charts_data[i]
    previous_chart = charts_data[i - 1]

    # Build lookup: (title, artist) -> position
    prev_lookup = {}
    for s in previous_chart.get("songs", []):
      key = _song_key(s)
      if key != ("", ""):
        prev_lookup[key] = s.get("chart_position", -1)

    for song in current_chart.get("songs", []):
      key = _song_key(song)
      song["previous_position"] = prev_lookup.get(key, -1)

  return charts_data

async def order_songs_by_rank(songs, avg_score=0.0, min_votes=10, alpha=0.6, beta=0.4):
    """
    Orders song objects based on the combined rank of normalized user score, votes, and dance ratings.
    
    Parameters:
        songs (list): List of song dictionaries with 'score', 'votes', and 'dances' (list of dicts with 'name' and 'rating').
        avg_score (float): The average score across all songs in the dataset (in -1 to +1 range). Default is 0.0.
        min_votes (int): The minimum number of votes required for credibility. Default is 10.
        alpha (float): Weight for the user score. Default is 0.6.
        beta (float): Weight for the average dance rating. Default is 0.4.

    Returns:
        list: List of song objects, ordered by their combined rank.
    """
    
    async def calculate_combined_rank(song):
        user_score = song['score']
        votes = song['votes']
        
        # Normalize the user score from -1 to 1 to a 0 to 1 scale
        normalized_user_score = (user_score + 1) / 2
        
        # Normalize the average score (avg_score) similarly
        normalized_avg_score = (avg_score + 1) / 2
        
        # Calculate weighted score using Bayesian average with normalized scores
        weighted_score = ((votes * normalized_user_score) + (normalized_avg_score * min_votes)) / (votes + min_votes)

        get_artist = await prisma.artist.find_first(
          where = {
            'name': song['artist']
          }
        )

        get_song = await prisma.song.find_first(
          where = {
            'title': song['title'],
            'artistId': get_artist.id
          }
        )

        get_dances = await prisma.dancesong.find_many(
          where = {
            'songId': get_song.id
          }
        )
        
        # Calculate normalized average dance rating
        avg_dance_rating = sum((dance.rating - 1) / 9 for dance in get_dances) / len(get_dances)
        
        # Calculate the combined rank using the weighted formula
        combined_rank = alpha * weighted_score + beta * avg_dance_rating
        return combined_rank
    
    # Add a 'rank' key to each song object based on the calculated combined rank
    for song in songs:
        song['chart_score'] = await calculate_combined_rank(song)
    
    # Sort the songs by the 'rank' in descending order (higher rank first)
    songs_sorted = sorted(songs, key=lambda x: x['chart_score'], reverse=True)

    for idx, song in enumerate(songs_sorted):
      song['chart_position'] = idx+1

    return songs_sorted

async def get_cat_songs(categories, date, tag_names=None, songs_per_cat: int = 3):
  """
  categories: list like [{"cat": "Weihnachten", "songs": []}, {"cat": "Cha Cha Cha", "songs": []}, ...]
  date: datetime
  tag_names: optional list/set of tag category names (defaults to tag_list)
  """

  tag_set = set(tag_names or tag_list)

  def _cat_name(x):
    # categories sometimes accidentally contain prisma objects -> normalize to string
    if isinstance(x, str):
      return x
    return getattr(x, "tag", None) or getattr(x, "name", None) or str(x)

  for category in categories:
    cat = _cat_name(category.get("cat"))
    category["cat"] = cat  # normalize in-place
    category["songs"] = []

    # allow songs with unknown release OR released before date
    song_release_filter = {"OR": [{"release": {"lte": date}}, {"release": None}]}

    if cat in tag_set:
      rows = await prisma.songtag.find_many(
        where={
          "tagName": cat,                 # ✅ category["cat"], not the whole dict
          "song": {"is": song_release_filter},
        },
        include={"song": {"include": {"artist": True}}},
      )
      candidates = [r.song for r in rows if r.song is not None]
    else:
      rows = await prisma.dancesong.find_many(
        where={
          "danceName": cat,               # ✅ category["cat"], not the whole dict
          "song": {"is": song_release_filter},
        },
        include={"song": {"include": {"artist": True}}},
      )
      candidates = [r.song for r in rows if r.song is not None]

    # pick unique songs
    seen = set()
    random.shuffle(candidates)
    for s in candidates:
      artist_name = (s.artist.name if getattr(s, "artist", None) else "").strip()
      key = (s.title.strip().lower(), artist_name.lower())
      if key in seen:
        continue
      seen.add(key)
      category["songs"].append({"title": s.title, "artist": artist_name})
      if len(category["songs"]) >= songs_per_cat:
        break

  return categories

  
async def get_all_tag_names() -> Set[str]:
    # Prefer DB instead of hardcoded tag_list
    tags = await prisma.tag.find_many()
    return {t.tag for t in tags}

async def get_applicable_seasonal_tag_names(date: datetime) -> List[str]:
    # Uses Tag.season like "11-12" or "2" etc.
    tags = await prisma.tag.find_many()
    out: List[str] = []
    for t in tags:
        if not t.season:
            continue
        try:
            months = [int(x) for x in t.season.split("-") if x.strip().isdigit()]
        except Exception:
            continue
        if date.month in months:
            out.append(t.tag)
    # stable order (optional): keep DB order; de-dupe while preserving order
    seen = set()
    deduped = []
    for x in out:
        if x not in seen:
            seen.add(x)
            deduped.append(x)
    return deduped

async def build_week_categories(
    date: datetime,
    all_types: List[str],
    base_dance_categories: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
  """
  Policy:
    - If seasonal tags apply: 2 dance categories + ALL applicable seasonal categories
    - Else: 3 dance categories
  """
  seasonals = await get_applicable_seasonal_tag_names(date)
  dance_needed = 2 if seasonals else 3

  categories: List[Dict[str, Any]] = []
  used_dance_names: Set[str] = set()
  used_types: Set[str] = set()

  # 1) Take up to N dance categories from website recommendation (if provided)
  if base_dance_categories:
    for c in base_dance_categories:
      name = (c.get("cat") or "").strip()
      if not name or name in used_dance_names:
        continue

      d = await prisma.dance.find_first(where={"name": name})
      if not d:
        continue  # skip unknown categories

      categories.append({"cat": name, "songs": []})
      used_dance_names.add(name)
      used_types.add(d.type)

      if len(categories) >= dance_needed:
        break

  # 2) Fill missing dance categories, prefer types not used yet
  remaining_types = [t for t in all_types if t not in used_types]
  random.shuffle(remaining_types)

  while len(categories) < dance_needed:
    pick_type = remaining_types.pop(0) if remaining_types else random.choice(all_types)
    dances = await prisma.dance.find_many(where={"type": pick_type})
    if not dances:
      continue
    pick = random.choice(dances).name
    if pick in used_dance_names:
      continue
    categories.append({"cat": pick, "songs": []})
    used_dance_names.add(pick)
    used_types.add(pick_type)

  # 3) Add ALL applicable seasonal categories (can exceed 3 total)
  for tag in seasonals:
    categories.append({"cat": tag, "songs": []})

  return categories

async def get_dances():

  with open('danceDB.json') as f:
    danceDB = json.load(f)
    print("Loaded all Dances")
    print()
  
  for dance in danceDB['danceDB']:
    create_dance = await prisma.dance.create(
      data = {
        "name": dance['nameDe'],
        "meter": dance['meter'],
        "bpm": dance['bpm'],
        "mpm": dance['mpm'],
        "type": dance['typeDe']
      }
    )
    print(f"Successfully added Dance {dance['nameDe']}")
  print()

async def get_tags():

  for i in range(len(tag_list)):
    create_tag = await prisma.tag.create(
      data = {
        'tag': tag_list[i],
        'season': season_list[i]
      }
    )
    print(f"Successfully added Tag {tag_list[i]}")
  print()

async def get_songs():
  
  start_url = "https://www.tanzmusik-online.de/music"

  page = requests.get(start_url, verify=False)

  if page.status_code == 200:
    content = page.content
  else:
    print("Gesamt Webseite nicht erreichbar")
    exit()

  soup = BeautifulSoup(content, 'html.parser')

  interpreten_divs = soup.find_all('div', class_='col-lg-3 col-md-4 col-sm-6 col-xs-offset-1 col-xs-12')

  interpreten = [div.find('a').get_text() for div in interpreten_divs if div.find('a')]
  for interpret in interpreten:
    create_artist = await prisma.artist.create(
        data = {
            'name': interpret
        }
      )
    print(f"Successfully added Artist {interpret}")
  print()

  links = [div.find('a')['href'] for div in interpreten_divs if div.find('a')]

  try:
    for idx in range(len(links)):

      link = links[idx]
      
      page = requests.get(link, verify=False)
      if page.status_code == 200:
        content = page.content
      else:
        print("Interpreten Webseite nicht erreichbar")
        continue
      
      soup = BeautifulSoup(content, 'html.parser')

      song_rows = []

      page_idx = 0
      pages = 1
        
      pull_rights = soup.find_all('div', class_='pull-right')
      for div in pull_rights:
        if "Seite" in div.get_text():
          pages = int(div.get_text().split("/")[1])
          break
        
      while page_idx < pages:
        page_idx += 1
        page_link = f"{link}?page={page_idx}"
        
        new_page = requests.get(page_link, verify=False)
        if new_page.status_code == 200:
          page_content = new_page.content
        else:
          break

        page_soup = BeautifulSoup(page_content, 'html.parser')

        temp_song_rows = page_soup.find_all('div', class_='songRow')

        for song_row in temp_song_rows:
          song_rows.append(song_row)

      songs = []

      # Suche nach jedem Song-Container
      for song_row in song_rows:
        song_data = {}

        # Songtitel extrahieren
        title_tag = song_row.find('div', class_='songTitle').find('a')
        song_data['song_title'] = title_tag.get_text() if title_tag else None
        song_data['song_url'] = title_tag['href'] if title_tag else None
        
        # Künstlername extrahieren
        artist_tag = song_row.find('span', class_='artist').find('a')
        song_data['artist'] = artist_tag.get_text() if artist_tag else None

        by_hand_checked = song_row.find('span', class_='byHandChecked')['title']
        song_data['expert_checked'] = True if "Experten" in by_hand_checked or "100" in by_hand_checked else False
        
        # Tanzarten und deren Bewertungen extrahieren
        dances = []
        for dance_div in song_row.find_all('div', class_='dances'):
            for dance in dance_div.find_all('div'):
                dance_name_tag = dance.find('a')
                if dance_name_tag:
                    dance_name = dance_name_tag.get_text()
                    # Extrahiere die Anzahl der aktiven Sterne
                    active_stars = dance.find_all('i', class_='fa fa-star active')
                    rating = len(active_stars) * 2
                    if rating < 8 and rating > 2 and not song_data['expert_checked']:
                      rating += random.randint(-2, 3)
                    dances.append({'dance': dance_name, 'rating': rating})

        song_data['dances'] = dances
        
        try:
            result = await get_shazam_tracks(song_data)
        except Exception as e:
            print(f"[Shazam] failed for {song_data['artist']} - {song_data['song_title']}: {e}")
            traceback.print_exc()  # uncomment once to see the real source
            result = []


        song_data['shazam'] = False
        song_data['apple_url'] = ""
        song_data['image'] = ""
        song_data['genre'] = ""
        if result:
          for res_song in result:
            if song_data['artist'].lower() == res_song['artist'].lower() and song_data['song_title'].lower() == res_song['title'].lower():
              song_data['shazam'] = True
              song_data['apple_url'] = res_song['apple_url']
              song_data['image'] = res_song['image']
              song_data['genre'] = res_song['genre']
              break

        try:
            result = await get_spotify_tracks(song_data)
        except Exception as e:
            print(f"[Spotify] failed for {song_data['artist']} - {song_data['song_title']}: {e}")
            # traceback.print_exc()
            result = []


        song_data['spotify_url'] = ""
        song_data['release'] = None
        if result:
          for res_song in result:
            if song_data['song_title'].lower() == res_song['title'].lower():
              for artist in res_song['artists']:
                if song_data['artist'].lower() == artist.lower():
                  song_data['spotify_url'] = res_song['spotify_url']

                  if song_data['image'] == "" and res_song['image'] != "":
                    song_data['image'] = res_song['image']
                  if song_data['genre'] == "" and res_song['genre'] != "":
                    song_data['genre'] = res_song['genre']

                  song_data['release'] = res_song['release']
                  if len(song_data['release'].split("-")) == 1:
                    song_data['release'] = f"{song_data['release']}-1-1"
                  elif len(song_data['release'].split("-")) == 2:
                    song_data['release'] = f"{song_data['release']}-1"
              
                  break

        # iTunes Search API (public, no auth)
        song_data['itunes_url'] = ""
        try:
            itunes_results = await get_itunes_tracks(song_data)
        except Exception as e:
            print(f"[iTunes] failed for {song_data['artist']} - {song_data['song_title']}: {e}")
            itunes_results = []

        if itunes_results:
          for res_song in itunes_results:
            if _norm_text(song_data['artist']) == _norm_text(res_song.get('artist')) and _norm_text(song_data['song_title']) == _norm_text(res_song.get('title')):
              song_data['itunes_url'] = res_song.get('itunes_url', "") or ""
              if song_data['image'] == "" and res_song.get('image'):
                song_data['image'] = res_song['image']
              if song_data['genre'] == "" and res_song.get('genre'):
                song_data['genre'] = res_song['genre']
              if song_data['release'] is None and res_song.get('release'):
                song_data['release'] = res_song['release']
                if len(song_data['release'].split("-")) == 1:
                  song_data['release'] = f"{song_data['release']}-1-1"
                elif len(song_data['release'].split("-")) == 2:
                  song_data['release'] = f"{song_data['release']}-1"
              break

        # Apple Music API (requires APPLE_MUSIC_DEV_TOKEN)
        song_data['apple_music_url'] = ""
        try:
            apple_music_results = await get_apple_music_tracks(song_data)
        except Exception as e:
            print(f"[AppleMusic] failed for {song_data['artist']} - {song_data['song_title']}: {e}")
            apple_music_results = []

        if apple_music_results:
          for res_song in apple_music_results:
            if _norm_text(song_data['artist']) == _norm_text(res_song.get('artist')) and _norm_text(song_data['song_title']) == _norm_text(res_song.get('title')):
              song_data['apple_music_url'] = res_song.get('apple_music_url', "") or ""
              if song_data['image'] == "" and res_song.get('image'):
                song_data['image'] = res_song['image']
              if song_data['genre'] == "" and res_song.get('genre'):
                song_data['genre'] = res_song['genre']
              if song_data['release'] is None and res_song.get('release'):
                song_data['release'] = res_song['release']
                if len(song_data['release'].split("-")) == 1:
                  song_data['release'] = f"{song_data['release']}-1-1"
                elif len(song_data['release'].split("-")) == 2:
                  song_data['release'] = f"{song_data['release']}-1"
              break
        

        song_data["youtube_lyrics_url"] = await asyncio.to_thread(
          find_youtube_lyrics_video_url,
          song_data["song_title"],
          song_data["artist"],
        )

        if not song_data['shazam'] and (song_data.get('apple_music_url') != "" or song_data.get('apple_url') != "" or song_data.get('itunes_url','') != ""):
          song_data['shazam'] = True

        if song_data['release'] is not None and (song_data['release'].split("-")[0] == "0000" or song_data['release'].split("-")[0] == "0"):
          song_data['release'] = None
        
        # Füge den extrahierten Song zu der Liste hinzu
        songs.append(song_data)

      # Ausgabe der extrahierten Daten
      for song in songs:
        print("------")
        print(f"Song: {song['song_title']} ({song['song_url']})")
        print(f"Künstler: {song['artist']}")
        print(f"Release: {song['release']}")
        print(f"Bild: {song['image']}")
        print(f"Geprüft: {song['expert_checked']}")
        for dance in song['dances']:
          print(f"Tanz: {dance['dance']} - Bewertung: {dance['rating']} Sterne")
        print(f"In Shazam: {song['shazam']}")
        print(f"Apple URL: {song['apple_url']}")
        print(f"Apple Music URL: {song.get('apple_music_url', '')}")
        print(f"iTunes URL: {song.get('itunes_url', '')}")
        print(f"Spotify URL: {song['spotify_url']}")
        print(f"Genre: {song['genre']}")
        print(f"YouTube Lyrics URL: {song['youtube_lyrics_url']}")
        print("------")

        a_artist = await prisma.artist.find_first(
          where = {
            'name': song['artist']
          }
        )

        if song['release'] is not None :
          create_song = await prisma.song.create(
            data = {
              'title': song['song_title'],
              'image': song['image'],
              'artist': {
                'connect': {
                  'id': a_artist.id,
                },
              },
              'release': datetime.combine(datetime.strptime(song['release'], '%Y-%m-%d').date(), datetime.min.time()).isoformat() + 'Z',
              'checked': song['expert_checked'],
              'appleMusicUrl': song.get('apple_music_url') or song['apple_url'] or song.get('itunes_url',''),
              'spotifyUrl': song['spotify_url'],
              'youtubeUrl': song['youtube_lyrics_url'],
              'shazam': song['shazam'],
              'genre': song['genre']
            }
          )
        else:
          create_song = await prisma.song.create(
            data = {
              'title': song['song_title'],
              'image': song['image'],
              'artist': {
                'connect': {
                  'id': a_artist.id,
                },
              },
              'checked': song['expert_checked'],
              'appleMusicUrl': song.get('apple_music_url') or song['apple_url'] or song.get('itunes_url',''),
              'spotifyUrl': song['spotify_url'],
              'youtubeUrl': song['youtube_lyrics_url'],
              'shazam': song['shazam'],
              'genre': song['genre']
            }
          )

        print(f"Successfully added Song {song['song_title']} - {song['artist']}")
        print("------")

        tags = []
        for title in christmas_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Weihnachten")
            break

        for title in halloween_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Halloween")
            break
        
        for title in easter_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Ostern")
            break
        
        for title in wedding_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Hochzeit")
            break
        
        for title in silvester_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Silvester")
            break
        
        for title in valentines_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Valentinstag")
            break

        for title in fasching_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Fasching")
            break

        for title in oktoberfest_songs:
          if song['song_title'] == title or title in song['song_title']:
            tags.append("Oktoberfest")
            break
          
        for tag in tags:
          create_songtag = await prisma.songtag.create(
              data = {
                'tag': {
                  'connect': {
                    'tag': tag
                  }
                },
                'song': {
                  'connect': {
                    'id': create_song.id
                  }
                }
              }
            )
          print(f"Added Tag {tag} to Song {create_song.title}")


        for dance in song['dances']:
          create_dancesongs = await prisma.dancesong.create(
            data = {
                'song': {
                  'connect': {
                    'id': create_song.id,
                  }
                },
                'dance': {
                  'connect': {
                    'name': dance['dance'],
                  }
                },
                'rating': dance['rating'],
            }
          )

          print(f"Successfully added DanceSong {song['song_title']} = {dance['dance']}: {dance['rating']}")
        print()
  
  except Exception as e:
    print(e)
    print(f"\n\nStopped at {link}")

async def get_charts():
  
  year = 2020
  week = 12
  end_year = 2026
  end_week = 8

  all_charts = []

  while not (year == end_year and week == end_week):

    chart = {}
    
    if week < 10:
      temp_week = f"0${week}"
      date = f"${year}${temp_week}"
    else:
      date = f"${year}${week}" 

    chart['week'] = week
    chart['year'] = year
    
    chart_url = f"https://www.tanzmusik-online.de/charts/${date}"

    page = requests.get(chart_url, verify=False)
    if page.status_code == 200:
      content = page.content
    else:
      print("Chart Webseite nicht erreichbar")
      break
    
    soup = BeautifulSoup(content, 'html.parser')

    chart_songs = []

    for song_row in soup.find_all('div', class_='row songRow visibleTrigger'):
      
      song_data = {}

      title_tag = song_row.find('div', class_='songTitle').find('a')
      song_data['title'] = title_tag.get_text() if title_tag else None
      artist_tag = song_row.find('span', class_='artist').find('a')
      song_data['artist'] = artist_tag.get_text() if artist_tag else None

      song_data['score'] = int(song_row.find('div', class_='ratyBar')['data-initial-score'])
      song_data['votes'] = int(song_row.find('div', class_='votesText').find('span', class_='number').text.strip())

      chart_songs.append(song_data)
    
    chart['songs'] = chart_songs

    all_charts.append(chart)

    if week+1 > 52:
      week = 1
      year += 1
    else:
      week += 1
  
  true_charts = []

  for i in range(0, len(all_charts)):

    chart = all_charts[i]
    
    new_chart = {}
    dt = datetime.fromisocalendar(chart['year'], chart['week'], 1)  # Monday of that ISO week
    month = dt.month
    new_chart['month'] = month
    new_chart['year'] = chart['year']

    ordered_songs = await order_songs_by_rank(chart['songs'])
    new_chart['songs'] = ordered_songs

    true_charts.append(new_chart)

  true_charts = add_previous_placement(true_charts)

  for chart in true_charts:
    print("------")
    print("Chart:")
    print(f"Monat: {chart['month']}")
    print(f"Jahr: {chart['year']}")
    for song in chart['songs']:
      print(f"Song: {song['title']}")
      print(f"Künstler: {song['artist']}")
      print(f"Position: {song['chart_position']}")
      print(f"Vorherige: {song.get('previous_position', -1)}")
      print(f"Score: {song['score']}")
      print(f"Votes: {song['votes']}")

      db_artist = await prisma.artist.find_first(
        where = {
          'name': song['artist']
        }
      )

      db_song = await prisma.song.find_first(
        where = {
          'title': song['title'],
          'artistId': db_artist.id
        }
      )

      create_chart = await prisma.chart.create(
        data = {
            'year': chart['year'],
            'month': chart['month'],
            'song': {
              'connect': {
                'id': db_song.id
              }
            },
            'placement': song['chart_position'],
            'previous': song.get('previous_position', -1)
        }
      )

    print("------")

    print("Successfully added Chart")

async def get_recs():
  year, month = 2020, 3
  end_year, end_month = 2026, 2

  all_recs = []

  while (year, month) <= (end_year, end_month):
    date_str = f"{year}-{month:02d}"
    rec_url = f"https://www.tanzmusik-online.de/recommendation/{date_str}"

    page = requests.get(rec_url, verify=False)
    if page.status_code != 200:
        print(f"Recommendation page not reachable: {rec_url}")
        break

    soup = BeautifulSoup(page.content, "html.parser")

    categories = []
    for h2 in soup.find_all("h2"):
      songlist = h2.find_next_sibling("div", class_="songlist")
      if not songlist:
        continue

      cat_name = h2.get_text(strip=True)
      song_rows = songlist.find_all("div", class_="songRow")

      cat_data = {"cat": cat_name, "songs": []}
      for row in song_rows:
        title = row.find("div", class_="songTitle")
        artist = row.find("span", class_="artist")
        if not title or not artist:
          continue
        cat_data["songs"].append({
          "title": title.get_text(strip=True),
          "artist": artist.get_text(strip=True),
        })

      if cat_data["songs"]:
        categories.append(cat_data)

    all_recs.append({"month": month, "year": year, "categories": categories})

    month += 1
    if month == 13:
      month = 1
      year += 1

  tag_names = await get_all_tag_names()
  # build weekly recs (4 per month)
  date = datetime(2020, 3, 2)
  new_recs = []

  all_types = ["Latein", "Walzer", "Swing", "Tango", "Foxtrott"]

  for recommendation in all_recs:
    # week 1: based on website cats but enforce policy
    new_rec = {
      "week": date.isocalendar()[1],
      "year": date.year,
    }

    cats = await build_week_categories(
      date=date,
      all_types=all_types,
      base_dance_categories=recommendation.get("categories") or [],
    )
    cats = await get_cat_songs(cats, date, tag_names)
    new_rec["categories"] = cats
    new_recs.append(new_rec)

    # next 3 weeks: generated but policy still applies
    for _ in range(3):
      date = date + timedelta(days=7)
      week_rec = {
        "week": date.isocalendar()[1],
        "year": date.year,
      }
      cats = await build_week_categories(date=date, all_types=all_types, base_dance_categories=None)
      cats = await get_cat_songs(cats, date, tag_names)
      week_rec["categories"] = cats
      new_recs.append(week_rec)

    date = date + timedelta(days=7)

  # write to DB
  for recommendation in new_recs:
    for category in recommendation["categories"]:
      for song in category["songs"]:
        artist_name = song["artist"].replace("\n", "").strip()
        title = song["title"].replace("\n", "").strip()

        db_artist = await prisma.artist.find_first(where={"name": artist_name})
        if not db_artist:
          continue

        db_song = await prisma.song.find_first(where={"title": title, "artistId": db_artist.id})
        if not db_song:
          continue

        if category["cat"] in tag_list:
          await prisma.recommendation.create(
            data={
              "year": recommendation["year"],
              "week": recommendation["week"],
              "catTag": {"connect": {"tag": category["cat"]}},  # IMPORTANT: connect by tag
              "song": {"connect": {"id": db_song.id}},
            }
          )
        else:
          await prisma.recommendation.create(
            data={
              "year": recommendation["year"],
              "week": recommendation["week"],
              "catDance": {"connect": {"name": category["cat"]}},
              "song": {"connect": {"id": db_song.id}},
            }
          )

prisma = Prisma()
shazam = Shazam(
  http_client=HTTPClient(
    retry_options=ExponentialRetry(
      attempts=12,
      max_timeout=204.8,
      statuses={500, 502, 503, 504, 429},
    ),
  ),
)

tag_list = ["Weihnachten", "Halloween", "Ostern", "Hochzeit", "Silvester", "Valentinstag", "Fasching", "Oktoberfest"]
season_list = ["11-12", "10", "4", "5-6", "12-1", "2", "2-3", "9-10"]

async def main():

  await prisma.connect()

  await prisma.chart.delete_many()
  await prisma.recommendation.delete_many()
  await prisma.dancesong.delete_many()
  await prisma.songtag.delete_many()
  await prisma.song.delete_many()
  await prisma.tag.delete_many()
  await prisma.dance.delete_many()
  await prisma.artist.delete_many()

  await prisma.query_raw("SET FOREIGN_KEY_CHECKS = 0;")
  await prisma.query_raw("TRUNCATE TABLE artists;")
  await prisma.query_raw("TRUNCATE TABLE songs;")
  await prisma.query_raw("TRUNCATE TABLE tags;")
  await prisma.query_raw("TRUNCATE TABLE dances;")
  await prisma.query_raw("TRUNCATE TABLE dancesongs;")
  await prisma.query_raw("TRUNCATE TABLE songtags;")
  await prisma.query_raw("TRUNCATE TABLE charts;")
  await prisma.query_raw("TRUNCATE TABLE recommendations;")
  await prisma.query_raw("SET FOREIGN_KEY_CHECKS = 1;") 
  
  await get_tags()
  await get_dances()
  
  await get_songs()

  await get_charts()
  await get_recs()
  
  await prisma.disconnect()

if __name__ == '__main__':
  asyncio.run(main())
