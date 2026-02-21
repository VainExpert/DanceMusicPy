from __future__ import annotations

import hashlib
import os
import random
import re
import string
from datetime import datetime
from typing import Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from prisma import Prisma
from prisma.errors import RecordNotFoundError, UniqueViolationError


# ----------------------------
# App + DB
# ----------------------------

app = FastAPI(title="DanceMusic API", version="1.0")
prisma = Prisma()

# CORS (lock this down in production)
_allow_origins = os.getenv("CORS_ALLOW_ORIGINS", "*")
allow_origins = [o.strip() for o in _allow_origins.split(",") if o.strip()]

app.add_middleware(
  CORSMiddleware,
  allow_origins=allow_origins,
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)


# ----------------------------
# Models
# ----------------------------

class UserCreate(BaseModel):
  logging: bool
  recommend: bool
  recordDuration: int = Field(ge=1, le=120)
  language: str


class LogCreate(BaseModel):
  title: str
  artist: str


class ScoreData(BaseModel):
  score: int
  title: str
  artist: str


class SongFeedbackData(BaseModel):
  value: int  # 1 like, -1 dislike
  title: str
  artist: str


class DanceFeedbackData(BaseModel):
  value: int  # 1 like, -1 dislike
  danceName: str


# ----------------------------
# Errors
# ----------------------------

@app.exception_handler(RecordNotFoundError)
async def handle_record_not_found(_: Request, __: RecordNotFoundError):
  return JSONResponse(status_code=404, content={"detail": "Not found"})


@app.exception_handler(UniqueViolationError)
async def handle_unique_violation(_: Request, __: UniqueViolationError):
  return JSONResponse(status_code=409, content={"detail": "Conflict"})


@app.exception_handler(Exception)
async def handle_unhandled(_: Request, __: Exception):
  # Avoid leaking internals.
  return JSONResponse(status_code=500, content={"detail": "Internal server error"})


# ----------------------------
# Helpers
# ----------------------------

_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,64}$")


def validate_username(username: str) -> None:
  if not _USERNAME_RE.match(username):
    raise HTTPException(status_code=400, detail="Invalid username")


def sha256_hex(s: str) -> str:
  return hashlib.sha256(s.encode("utf-8")).hexdigest()


async def require_api_key(
  x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> None:
  """
  Validates an API key against the Keys table.

  Expected DB: Keys(service='client_api', key=<sha256(token)> or raw token, expireDate>now).

  For best security: store ONLY the sha256(token) in keys.key, not the raw token.
  """
  if not x_api_key:
    raise HTTPException(status_code=401, detail="Missing API key")

  now = datetime.utcnow()
  token = x_api_key.strip()
  token_hash = sha256_hex(token)

  service = os.getenv("API_KEY_SERVICE", "client_api")

  # Allow either hashed or raw token (raw only for transition).
  row = await prisma.keys.find_first(
    where={
      "service": service,
      "expireDate": {"gt": now},
      "OR": [{"key": token_hash}, {"key": token}],
    }
  )

  if row is None:
    raise HTTPException(status_code=401, detail="Invalid or expired API key")


async def ensure_user_exists(username: str) -> None:
  validate_username(username)
  u = await prisma.user.find_unique(where={"username": username})
  if u is None:
    raise HTTPException(status_code=404, detail="User not found")


async def resolve_song_id(title: str, artist_name: str) -> int:
  artist_name = (artist_name or "").strip()
  title = (title or "").strip()

  if not artist_name or not title:
    raise HTTPException(status_code=400, detail="title and artist are required")

  artist = await prisma.artist.find_first(where={"name": artist_name})
  if artist is None:
    raise HTTPException(status_code=404, detail="Artist not found")

  song = await prisma.song.find_first(where={"title": title, "artistId": artist.id})
  if song is None:
    raise HTTPException(status_code=404, detail="Song not found")

  return int(song.id)


async def apply_song_feedback(username: str, title: str, artist: str, value: int) -> dict[str, Any]:
  if value not in (1, -1):
    raise HTTPException(status_code=400, detail="value must be 1 or -1")

  await ensure_user_exists(username)
  song_id = await resolve_song_id(title, artist)

  existing = await prisma.songfeedback.find_unique(
    where={"username_songId": {"username": username, "songId": song_id}}
  )

  if existing is None:
    await prisma.songfeedback.create(data={"username": username, "songId": song_id, "value": value})
    await prisma.song.update(
      where={"id": song_id},
      data={"votes": {"increment": 1}, "avgScore": {"increment": value}},
    )
    return {"ok": True, "created": True, "delta": value}

  delta = value - int(existing.value)
  if delta == 0:
    return {"ok": True, "created": False, "delta": 0}

  await prisma.songfeedback.update(
    where={"username_songId": {"username": username, "songId": song_id}},
    data={"value": value},
  )
  await prisma.song.update(where={"id": song_id}, data={"avgScore": {"increment": delta}})
  return {"ok": True, "created": False, "delta": delta}


async def apply_dance_feedback(username: str, dance_name: str, value: int) -> dict[str, Any]:
  if value not in (1, -1):
    raise HTTPException(status_code=400, detail="value must be 1 or -1")

  await ensure_user_exists(username)

  dance_name = (dance_name or "").strip()
  if not dance_name:
    raise HTTPException(status_code=400, detail="danceName is required")

  d = await prisma.dance.find_unique(where={"name": dance_name})
  if d is None:
    raise HTTPException(status_code=404, detail="Dance not found")

  existing = await prisma.dancefeedback.find_unique(
    where={"username_danceName": {"username": username, "danceName": dance_name}}
  )

  if existing is None:
    await prisma.dancefeedback.create(data={"username": username, "danceName": dance_name, "value": value})
    await prisma.dance.update(
      where={"name": dance_name},
      data={"votes": {"increment": 1}, "avgScore": {"increment": value}},
    )
    return {"ok": True, "created": True, "delta": value}

  delta = value - int(existing.value)
  if delta == 0:
    return {"ok": True, "created": False, "delta": 0}

  await prisma.dancefeedback.update(
    where={"username_danceName": {"username": username, "danceName": dance_name}},
    data={"value": value},
  )
  await prisma.dance.update(where={"name": dance_name}, data={"avgScore": {"increment": delta}})
  return {"ok": True, "created": False, "delta": delta}


# ----------------------------
# Lifecycle
# ----------------------------

@app.on_event("startup")
async def startup() -> None:
  await prisma.connect()


@app.on_event("shutdown")
async def shutdown() -> None:
  await prisma.disconnect()


# ----------------------------
# Health
# ----------------------------

@app.get("/health")
async def health() -> dict[str, str]:
  return {"status": "ok"}


# ----------------------------
# Users
# ----------------------------

@app.get("/user/{username}")
async def get_user(username: str, _: None = Depends(require_api_key)):
  validate_username(username)
  user = await prisma.user.find_unique(where={"username": username})
  if user is None:
    raise HTTPException(status_code=404, detail="User not found")
  return user


@app.post("/user")
async def create_user(_: None = Depends(require_api_key)):
  characters = string.ascii_letters + string.digits + "_"

  # Try-create loop (no need to read all users)
  for _ in range(20):
    name = "".join(random.choice(characters) for _ in range(10))
    try:
      user = await prisma.user.create(data={"username": name})
      return user
    except UniqueViolationError:
      continue

  raise HTTPException(status_code=500, detail="Could not generate unique username")


@app.put("/user/{username}")
async def update_user(username: str, user_data: UserCreate, _: None = Depends(require_api_key)):
  validate_username(username)
  user = await prisma.user.update(
    where={"username": username},
    data={
      "logging": user_data.logging,
      "recommend": user_data.recommend,
      "recordDuration": user_data.recordDuration,
      "language": user_data.language,
    },
  )
  return user


@app.delete("/user/{username}")
async def delete_user(username: str, _: None = Depends(require_api_key)):
  validate_username(username)
  user = await prisma.user.delete(where={"username": username})
  return user


# ----------------------------
# Feedback
# ----------------------------

@app.post("/feedback/song/{username}")
async def set_song_feedback(username: str, data: SongFeedbackData, _: None = Depends(require_api_key)):
  return await apply_song_feedback(username, data.title, data.artist, data.value)


@app.post("/feedback/dance/{username}")
async def set_dance_feedback(username: str, data: DanceFeedbackData, _: None = Depends(require_api_key)):
  return await apply_dance_feedback(username, data.danceName, data.value)

# ----------------------------
# Logs
# ----------------------------

@app.post("/log/{username}")
async def create_log(username: str, log_data: LogCreate, _: None = Depends(require_api_key)):
  await ensure_user_exists(username)
  song_id = await resolve_song_id(log_data.title, log_data.artist)

  logentry = await prisma.logentry.create(
    data={
      "user": {"connect": {"username": username}},
      "song": {"connect": {"id": song_id}},
    }
  )
  return logentry


@app.get("/logs/{username}")
async def get_logs(username: str, _: None = Depends(require_api_key)):
  await ensure_user_exists(username)
  logs = await prisma.logentry.find_many(
    where={"username": username},
    include={"song": {"include": {"artist": True}}},
  )
  return logs


# ----------------------------
# Catalog
# ----------------------------

@app.get("/dances")
async def get_dances(_: None = Depends(require_api_key)):
  return await prisma.dance.find_many()


@app.get("/artists")
async def get_artists(_: None = Depends(require_api_key)):
  return await prisma.artist.find_many()


@app.get("/songs")
async def get_songs(_: None = Depends(require_api_key)):
  return await prisma.song.find_many(include={"artist": True, "danceSongs": True})


@app.get("/songs/{artist_name}")
async def get_songs_by_artist(artist_name: str, _: None = Depends(require_api_key)):
  artist_name = (artist_name or "").strip()
  if not artist_name:
    raise HTTPException(status_code=400, detail="artist_name is required")

  artist = await prisma.artist.find_first(where={"name": artist_name})
  if artist is None:
    raise HTTPException(status_code=404, detail="Artist not found")

  return await prisma.song.find_many(
    where={"artistId": artist.id},
    include={"artist": True, "danceSongs": True},
  )


@app.get("/song/{artist_name}/{title}")
async def get_song_by_title_artist(artist_name: str, title: str, _: None = Depends(require_api_key)):
  artist_name = (artist_name or "").strip()
  title = (title or "").strip()
  if not artist_name or not title:
    raise HTTPException(status_code=400, detail="artist_name and title are required")

  artist = await prisma.artist.find_first(where={"name": artist_name})
  if artist is None:
    raise HTTPException(status_code=404, detail="Artist not found")

  song = await prisma.song.find_first(
    where={"artistId": artist.id, "title": title},
    include={"artist": True, "danceSongs": True},
  )
  if song is None:
    raise HTTPException(status_code=404, detail=f"Song {title} by {artist_name} not found")
  return song


@app.get("/inspiration")
async def get_inspiration(n: int = 10, _: None = Depends(require_api_key)):
  n = max(1, min(int(n), 50))
  all_songs = await prisma.song.find_many(include={"danceSongs": True, "artist": True})
  if not all_songs:
    return []
  return [random.choice(all_songs) for _ in range(n)]


# ----------------------------
# Charts
# ----------------------------

@app.get("/charts")
async def get_charts(_: None = Depends(require_api_key)):
  return await prisma.chart.find_many(
    include={"song": {"include": {"artist": True, "danceSongs": True}}},
  )


@app.get("/charts/{year}")
async def get_charts_by_year(year: int, _: None = Depends(require_api_key)):
  return await prisma.chart.find_many(
    where={"year": int(year)},
    include={"song": {"include": {"artist": True, "danceSongs": True}}},
  )


@app.get("/charts/{year}/{month}")
async def get_charts_by_year_month(year: int, month: int, _: None = Depends(require_api_key)):
  y = int(year)
  m = int(month)
  if m < 1 or m > 12:
    raise HTTPException(status_code=400, detail="month must be 1..12")

  return await prisma.chart.find_many(
    where={"year": y, "month": m},
    include={"song": {"include": {"artist": True, "danceSongs": True}}},
  )


# ----------------------------
# Recommendations
# ----------------------------

@app.get("/recommendations/{username}")
async def get_recommendations_by_user(username: str, _: None = Depends(require_api_key)):
  validate_username(username)
  return await prisma.recommendation.find_many(
    where={"username": username},
    include={
      "song": {"include": {"artist": True, "danceSongs": True}},
      "catDance": True,
      "catTag": True,
    },
  )


@app.get("/recommendations/{username}/{year}")
async def get_recommendations_by_user_year(username: str, year: int, _: None = Depends(require_api_key)):
  validate_username(username)
  return await prisma.recommendation.find_many(
    where={"username": username, "year": int(year)},
    include={
      "song": {"include": {"artist": True, "danceSongs": True}},
      "catDance": True,
      "catTag": True,
    },
  )


@app.get("/recommendations/{username}/{year}/{week}")
async def get_recommendations_by_user_year_week(username: str, year: int, week: int, _: None = Depends(require_api_key)):
  validate_username(username)
  return await prisma.recommendation.find_many(
    where={"username": username, "year": int(year), "week": int(week)},
    include={
      "song": {"include": {"artist": True, "danceSongs": True}},
      "catDance": True,
      "catTag": True,
    },
  )


@app.get("/recommendations/global/{year}")
async def get_recommendations_global_year(year: int, _: None = Depends(require_api_key)):
  return await prisma.recommendation.find_many(
    where={"username": None, "year": int(year)},
    include={
      "song": {"include": {"artist": True, "danceSongs": True}},
      "catDance": True,
      "catTag": True,
    },
  )


@app.get("/recommendations/global/{year}/{week}")
async def get_recommendations_global_year_week(year: int, week: int, _: None = Depends(require_api_key)):
  return await prisma.recommendation.find_many(
    where={"username": None, "year": int(year), "week": int(week)},
    include={
      "song": {"include": {"artist": True, "danceSongs": True}},
      "catDance": True,
      "catTag": True,
    },
  )