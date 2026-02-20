from __future__ import annotations

import argparse
import asyncio
import random
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Dict, List, Optional, Sequence, Set, Tuple

from prisma import Prisma

from .common import (
  BERLIN,
  chunked,
  iso_year_week,
  is_tag_applicable_for_date,
  parse_date,
)

@dataclass(frozen=True)
class Category:
  kind: str  # "dance" | "tag"
  name: str

_GENRE_SPLIT = re.compile(r"[,/;|]+")

def norm_genre(g: Optional[str]) -> str:
    if not g:
        return ""
    g = g.strip().lower()
    # if you ever store multiple genres, keep primary
    parts = [p.strip() for p in _GENRE_SPLIT.split(g) if p.strip()]
    return parts[0] if parts else ""

def norm_genre_set(g: Optional[str]) -> Set[str]:
    if not g:
        return set()
    parts = [p.strip().lower() for p in _GENRE_SPLIT.split(g) if p.strip()]
    return set(parts)

def parse_args() -> argparse.Namespace:
  p = argparse.ArgumentParser(description="Generate USER weekly recommendations from DB only.")
  p.add_argument("--date", type=str, default="", help="Any date inside target week (YYYY-MM-DD). Default: today (Europe/Berlin)")
  p.add_argument("--username", type=str, default="", help="Generate recs for a single user")
  p.add_argument("--all-users", action="store_true", help="Generate recs for all users with recommend=true")
  p.add_argument("--songs-per-category", type=int, default=2)
  p.add_argument("--min-dance-rating", type=int, default=1)
  p.add_argument("--force", action="store_true", help="Overwrite existing user recs for that week")
  p.add_argument("--dry-run", action="store_true")
  p.add_argument("--seed", type=int, default=0)
  return p.parse_args()

def _dt_lte(d: date) -> datetime:
  # Song.release is DateTime; compare as datetime (naive-ish, but consistent).
  return datetime.combine(d, time.min).replace(tzinfo=BERLIN)

async def get_applicable_seasonal_tags(prisma: Prisma, d: date) -> List[str]:
  tags = await prisma.tag.find_many(select={"tag": True, "season": True, "type": True})
  out: List[str] = []
  for t in tags:
    if (t.type or "Seasonal").lower() != "seasonal":
      continue
    if is_tag_applicable_for_date(t.season or "", d):
      out.append(str(t.tag))
  return out

async def get_user_profile(prisma: Prisma, username: str) -> Dict[str, Set]:
  """
  Returns sets used for scoring + filtering.
  """
  song_fb = await prisma.songfeedback.find_many(
    where={"username": username},
    select={"songId": True, "value": True},
  )
  liked_song_ids = {int(r.songId) for r in song_fb if int(r.value) == 1}
  disliked_song_ids = {int(r.songId) for r in song_fb if int(r.value) == -1}
  rated_song_ids = {int(r.songId) for r in song_fb}

  dance_fb = await prisma.dancefeedback.find_many(
    where={"username": username},
    select={"danceName": True, "value": True},
  )
  liked_dances = {str(r.danceName) for r in dance_fb if int(r.value) == 1}
  disliked_dances = {str(r.danceName) for r in dance_fb if int(r.value) == -1}

  liked_artist_ids: Set[int] = set()
  disliked_artist_ids: Set[int] = set()

  liked_genres: Set[str] = set()
  disliked_genres: Set[str] = set()

  if liked_song_ids:
    liked_songs = await prisma.song.find_many(
      where={"id": {"in": list(liked_song_ids)}},
      select={"artistId": True, "genre": True},
    )
    liked_artist_ids = {int(s.artistId) for s in liked_songs}
    for s in liked_songs:
      liked_genres |= norm_genre_set(getattr(s, "genre", None))

  if disliked_song_ids:
    bad_songs = await prisma.song.find_many(
      where={"id": {"in": list(disliked_song_ids)}},
      select={"artistId": True, "genre": True},
    )
    disliked_artist_ids = {int(s.artistId) for s in bad_songs}
    for s in bad_songs:
      disliked_genres |= norm_genre_set(getattr(s, "genre", None))

    """
    # genre tags: Tag.type == "Genre" (or any convention you pick)
    rows = await prisma.songtag.find_many(
      where={"songId": {"in": list(liked_song_ids)}},
      include={"tag": True},
    )
    for r in rows:
      if r.tag and (r.tag.type or "").lower() == "genre":
        liked_genre_tags.add(str(r.tagName))
    """

  return {
    "liked_song_ids": liked_song_ids,
    "disliked_song_ids": disliked_song_ids,
    "rated_song_ids": rated_song_ids,
    "liked_dances": liked_dances,
    "disliked_dances": disliked_dances,
    "liked_artist_ids": liked_artist_ids,
    "disliked_artist_ids": disliked_artist_ids,
    "liked_genres": liked_genres,
    "disliked_genres": disliked_genres,
    #"liked_genre_tags": liked_genre_tags,
  }

async def pick_personal_dance_categories(
  prisma: Prisma,
  rng: random.Random,
  liked_dances: Set[str],
  disliked_dances: Set[str],
  k: int,
) -> List[str]:
  # Prefer liked dances, avoid disliked
  liked = [d for d in liked_dances if d not in disliked_dances]
  if liked:
    # diversify by type
    liked_rows = await prisma.dance.find_many(
      where={"name": {"in": liked}},
      select={"name": True, "type": True},
    )
    by_type: Dict[str, List[str]] = {}
    for d in liked_rows:
      by_type.setdefault(str(d.type), []).append(str(d.name))

    types = list(by_type.keys())
    rng.shuffle(types)

    picked: List[str] = []
    for t in types:
      if len(picked) >= k:
        break
      picked.append(rng.choice(by_type[t]))

    # fill from remaining liked
    rng.shuffle(liked)
    for d in liked:
      if len(picked) >= k:
        break
      if d not in picked:
        picked.append(d)
    if len(picked) >= k:
      return picked[:k]

  # fallback: global diverse picks, excluding disliked
  dances = await prisma.dance.find_many(select={"name": True, "type": True})
  by_type: Dict[str, List[str]] = {}
  for d in dances:
    name = str(d.name)
    if name in disliked_dances:
      continue
    by_type.setdefault(str(d.type), []).append(name)

  types = list(by_type.keys())
  rng.shuffle(types)
  picked: List[str] = []
  for t in types:
    if len(picked) >= k:
      break
    if by_type[t]:
      picked.append(rng.choice(by_type[t]))

  # fill from remaining
  all_names = [str(d.name) for d in dances if str(d.name) not in disliked_dances]
  rng.shuffle(all_names)
  for n in all_names:
    if len(picked) >= k:
      break
    if n not in picked:
      picked.append(n)

  return picked[:k]

def build_categories(seasonal_tags: Sequence[str]) -> List[Category]:
  # same policy as before: add all seasonals (0..n), then add dances until >= 3
  return [Category(kind="tag", name=t) for t in seasonal_tags]

def _song_similarity_bonus(
  song,
  song_tag_names: Set[str],
  profile: Dict[str, Set],
  song_dance_names: Set[str],
) -> float:
  bonus = 0.0
  if int(song.artistId) in profile["liked_artist_ids"]:
    bonus += 500.0
  if int(song.artistId) in profile["disliked_artist_ids"]:
    bonus -= 500.0
  
  gset = norm_genre_set(getattr(song, "genre", None))
  if gset:
    shared = gset.intersection(profile["liked_genres"])
    bonus += 250.0 * float(len(shared))

    bad = gset.intersection(profile["disliked_genres"])
    bonus -= 400.0 * float(len(bad))

  """
  # boost by shared genre tags (Tag.type == "Genre")
  shared = song_tag_names.intersection(profile["liked_genre_tags"])
  bonus += 200.0 * float(len(shared))
  """

  # avoid dances the user dislikes
  bad_dances = song_dance_names.intersection(profile["disliked_dances"])
  bonus -= 300.0 * float(len(bad_dances))

  return bonus

async def fetch_songs_for_dance_personal(
  prisma: Prisma,
  dance_name: str,
  limit: int,
  min_rating: int,
  release_lte: date,
  exclude_ids: Set[int],
  profile: Dict[str, Set],
) -> List[int]:
  rows = await prisma.dancesong.find_many(
    where={
      "danceName": dance_name,
      "rating": {"gte": min_rating},
      "song": {
        "is": {
          "checked": True,
          "OR": [{"release": None}, {"release": {"lte": _dt_lte(release_lte)}}],
        }
      },
    },
    include={
      "song": {
        "include": {
          "songTags": {"include": {"tag": True}},
          "danceSongs": {"select": {"danceName": True}},
        }
      }
    },
  )

  scored: List[Tuple[float, int]] = []
  for r in rows:
    s = r.song
    if not s:
      continue
    sid = int(s.id)
    if sid in exclude_ids:
      continue
    if sid in profile["disliked_song_ids"]:
      continue
    if sid in profile["rated_song_ids"]:
      continue  # don't recommend already rated songs

    votes = int(s.votes or 0)
    avg = float(s.avgScore or 0)
    rating = float(r.rating or 0)

    # tags for genre similarity
    tag_names = {str(st.tagName) for st in (s.songTags or []) if st.tag and (st.tag.type or "").lower() == "genre"}
    dance_names = {str(ds.danceName) for ds in (s.danceSongs or [])}

    base = rating * 100.0 + votes * 10.0 + avg
    bonus = _song_similarity_bonus(s, tag_names, profile, dance_names)

    scored.append((base + bonus, sid))

  scored.sort(reverse=True)
  return [sid for _, sid in scored[:limit]]

async def fetch_songs_for_tag_personal(
  prisma: Prisma,
  tag: str,
  limit: int,
  release_lte: date,
  exclude_ids: Set[int],
  profile: Dict[str, Set],
) -> List[int]:
  rows = await prisma.songtag.find_many(
    where={
      "tagName": tag,
      "song": {
        "is": {
          "checked": True,
          "OR": [{"release": None}, {"release": {"lte": _dt_lte(release_lte)}}],
        }
      },
    },
    include={
      "song": {
        "include": {
          "songTags": {"include": {"tag": True}},
          "danceSongs": {"select": {"danceName": True}},
        }
      }
    },
  )

  scored: List[Tuple[float, int]] = []
  for r in rows:
    s = r.song
    if not s:
      continue
    sid = int(s.id)
    if sid in exclude_ids:
      continue
    if sid in profile["disliked_song_ids"]:
      continue
    if sid in profile["rated_song_ids"]:
      continue

    votes = int(s.votes or 0)
    avg = float(s.avgScore or 0)

    tag_names = {str(st.tagName) for st in (s.songTags or []) if st.tag and (st.tag.type or "").lower() == "genre"}
    dance_names = {str(ds.danceName) for ds in (s.danceSongs or [])}

    base = votes * 10.0 + avg
    bonus = _song_similarity_bonus(s, tag_names, profile, dance_names)

    scored.append((base + bonus, sid))

  scored.sort(reverse=True)
  return [sid for _, sid in scored[:limit]]

async def write_user_week_recs(
  prisma: Prisma,
  username: str,
  year: int,
  week: int,
  cat_songs: List[Tuple[Category, List[int]]],
  force: bool,
  dry_run: bool,
) -> None:
  existing = await prisma.recommendation.find_first(where={"year": year, "week": week, "username": username})
  if existing and not force:
    print(f"[{username}] recs for {year}-W{week} already exist (use --force).")
    return

  if not dry_run and force:
    await prisma.recommendation.delete_many(where={"year": year, "week": week, "username": username})

  if dry_run:
    print(f"[{username}] would write {sum(len(x[1]) for x in cat_songs)} rows for {year}-W{week}")
    return

  for cat, song_ids in cat_songs:
    for sid in song_ids:
      data = {"year": year, "week": week, "song": {"connect": {"id": int(sid)}}, "user": {"connect": {"username": username}}}
      if cat.kind == "tag":
        data["catTag"] = {"connect": {"tag": cat.name}}
      else:
        data["catDance"] = {"connect": {"name": cat.name}}
      await prisma.recommendation.create(data=data)

async def main() -> None:
  args = parse_args()
  rng = random.Random(args.seed or None)

  today = datetime.now(tz=BERLIN).date()
  target_date = parse_date(args.date) if args.date else today
  year, week = iso_year_week(target_date)

  prisma = Prisma()
  await prisma.connect()

  try:
    if args.all_users:
      users = await prisma.user.find_many(where={"recommend": True}, select={"username": True})
      usernames = [str(u.username) for u in users]
    else:
      if not args.username:
        raise SystemExit("Provide --username or --all-users")
      usernames = [args.username]

    seasonal_tags = await get_applicable_seasonal_tags(prisma, target_date)

    for username in usernames:
      profile = await get_user_profile(prisma, username)

      categories = build_categories(seasonal_tags)

      # add dances until >= 3 categories total
      need = max(0, 3 - len(categories))
      if need > 0:
        dances = await pick_personal_dance_categories(
          prisma, rng, profile["liked_dances"], profile["disliked_dances"], need
        )
        categories.extend([Category(kind="dance", name=d) for d in dances])

      exclude: Set[int] = set()
      out: List[Tuple[Category, List[int]]] = []

      for cat in categories:
        if cat.kind == "tag":
          ids = await fetch_songs_for_tag_personal(
            prisma, cat.name, args.songs_per_category, target_date, exclude, profile
          )
        else:
          ids = await fetch_songs_for_dance_personal(
            prisma, cat.name, args.songs_per_category, args.min_dance_rating, target_date, exclude, profile
          )
        exclude.update(ids)
        out.append((cat, ids))

      await write_user_week_recs(prisma, username, year, week, out, args.force, args.dry_run)

  finally:
    await prisma.disconnect()

if __name__ == "__main__":
  asyncio.run(main())
