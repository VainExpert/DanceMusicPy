from __future__ import annotations

import argparse
import asyncio
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

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
    kind: str  # 'dance' | 'tag'
    name: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Generate weekly recommendations from DB only (no website scraping). "
            "Default target week is the current ISO week." 
        )
    )
    p.add_argument(
        "--date",
        type=str,
        default="",
        help="Any date inside the target week (YYYY-MM-DD). Default: today (Europe/Berlin)",
    )
    p.add_argument(
        "--songs-per-category",
        type=int,
        default=2,
        help="How many songs to write per category",
    )
    p.add_argument(
        "--min-dance-rating",
        type=int,
        default=1,
        help="Only consider DanceSong rows with rating >= this",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing global recs for that week (delete+recreate)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB, only print a preview",
    )
    return p.parse_args()


async def get_applicable_seasonal_tags(prisma: Prisma, d: date) -> List[str]:
    """All Tag.tag values that are seasonal and match the current month."""
    tags = await prisma.tag.find_many(select={"tag": True, "season": True, "type": True})
    out: List[str] = []
    for t in tags:
        if (t.type or "Seasonal").lower() != "seasonal":
            continue
        if is_tag_applicable_for_date(t.season or "", d):
            out.append(str(t.tag))
    return out


async def pick_dance_categories(prisma: Prisma, rng: random.Random, k: int) -> List[str]:
    """Pick k dance names, trying to diversify by Dance.type."""
    dances = await prisma.dance.find_many(select={"name": True, "type": True})
    if not dances:
        return []

    # Group by type
    by_type: Dict[str, List[str]] = {}
    for d in dances:
        by_type.setdefault(str(d.type), []).append(str(d.name))

    types = list(by_type.keys())
    rng.shuffle(types)

    picked: List[str] = []
    for t in types:
        if len(picked) >= k:
            break
        names = by_type[t]
        picked.append(rng.choice(names))

    # Not enough distinct types -> fill from remaining dances
    if len(picked) < k:
        all_names = [str(d.name) for d in dances]
        rng.shuffle(all_names)
        for name in all_names:
            if len(picked) >= k:
                break
            if name not in picked:
                picked.append(name)

    return picked[:k]


def build_week_categories(seasonal_tags: Sequence[str]) -> List[Category]:
    """Policy:

    - There should be 3 recommendation categories.
    - One of those should be seasonal *if applicable*.
    - If multiple seasonal categories are applicable, all of them should be added.
    - If no seasonal tag applies, use only dance categories.

    We implement this as:
    - categories = all applicable seasonal tag categories (0..n)
    - then append dance categories until len(categories) >= 3
    """
    cats = [Category(kind="tag", name=t) for t in seasonal_tags]
    return cats


async def fetch_songs_for_tag(
    prisma: Prisma,
    tag: str,
    limit: int,
    release_lte: date,
    exclude_ids: Set[int],
) -> List[int]:
    rows = await prisma.songtag.find_many(
        where={"tagName": tag},
        include={
            "song": {
                "include": {"artist": True},
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
        if not bool(s.checked):
            continue
        # release filter (if release is NULL, allow)
        if s.release is not None and s.release.date() > release_lte:
            continue
        votes = int(s.votes or 0)
        avg = float(s.avgScore or 0)
        # simple score: prioritize community evidence
        score = votes * 10.0 + avg
        scored.append((score, sid))

    scored.sort(reverse=True)
    return [sid for _, sid in scored[:limit]]


async def fetch_songs_for_dance(
    prisma: Prisma,
    dance_name: str,
    limit: int,
    min_rating: int,
    release_lte: date,
    exclude_ids: Set[int],
) -> List[int]:
    rows = await prisma.dancesong.find_many(
        where={"danceName": dance_name, "rating": {"gte": min_rating}},
        include={
            "song": {
                "include": {"artist": True},
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
        if not bool(s.checked):
            continue
        if s.release is not None and s.release.date() > release_lte:
            continue

        votes = int(s.votes or 0)
        avg = float(s.avgScore or 0)
        rating = float(r.rating or 0)
        score = rating * 100.0 + votes * 10.0 + avg
        scored.append((score, sid))

    scored.sort(reverse=True)
    return [sid for _, sid in scored[:limit]]


async def preview_category_songs(prisma: Prisma, song_ids: List[int]) -> List[str]:
    if not song_ids:
        return []
    out: List[str] = []
    for chunk in chunked(song_ids, 200):
        rows = await prisma.song.find_many(where={"id": {"in": list(chunk)}}, include={"artist": True})
        # stable order by provided IDs
        m = {int(r.id): r for r in rows}
        for sid in chunk:
            r = m.get(int(sid))
            if r:
                out.append(f"{r.artist.name} – {r.title}")
    return out


async def upsert_week_recommendations(
    prisma: Prisma,
    year: int,
    week: int,
    categories: List[Tuple[Category, List[int]]],
    force: bool,
    dry_run: bool,
) -> None:
    # Only manage global recs (username IS NULL). Keep user-specific recs intact.
    existing = await prisma.recommendation.find_first(where={"year": year, "week": week, "username": None})
    if existing and not force:
        print(f"Global recommendations for ISO week {year}-W{week:02d} already exist. Use --force to overwrite.")
        return

    print(f"\n=== Weekly recommendations preview for ISO week {year}-W{week:02d} ===")
    for cat, song_ids in categories:
        songs = await preview_category_songs(prisma, song_ids)
        print(f"\n[{cat.kind}] {cat.name}")
        for s in songs:
            print(f"  - {s}")

    if dry_run:
        print("\n(dry-run) No DB changes made.")
        return

    # Delete only global recs for that week.
    # Some prisma-client-py versions are picky about NULL filters; raw SQL is reliable.
    await prisma.query_raw(
        f"DELETE FROM recommendations WHERE year = {int(year)} AND week = {int(week)} AND username IS NULL"
    )

    # Insert.
    for cat, song_ids in categories:
        for sid in song_ids:
            data = {
                "year": int(year),
                "week": int(week),
                "song": {"connect": {"id": int(sid)}},
                "username": None,
            }
            if cat.kind == "tag":
                data["catTag"] = {"connect": {"tag": cat.name}}
            else:
                data["catDance"] = {"connect": {"name": cat.name}}

            await prisma.recommendation.create(data=data)

    total = sum(len(ids) for _, ids in categories)
    print(f"\nWrote {total} recommendation rows for ISO week {year}-W{week:02d}.")


async def main() -> None:
    args = parse_args()

    if args.date:
        d = parse_date(args.date)
    else:
        d = datetime.now(tz=BERLIN).date()

    year, week = iso_year_week(d)

    prisma = Prisma()
    await prisma.connect()
    try:
        seasonal = await get_applicable_seasonal_tags(prisma, d)

        # Seeded randomness = deterministic per week
        rng = random.Random(f"{year}-W{week:02d}")

        categories: List[Category] = build_week_categories(seasonal)

        # Fill with dance categories until at least 3 total (seasonals can push above 3)
        need = max(0, 3 - len(categories))
        dance_names = await pick_dance_categories(prisma, rng, need)
        categories.extend([Category(kind="dance", name=n) for n in dance_names])

        # Fill songs per category
        used_song_ids: Set[int] = set()
        cat_songs: List[Tuple[Category, List[int]]] = []

        for cat in categories:
            if cat.kind == "tag":
                ids = await fetch_songs_for_tag(
                    prisma,
                    tag=cat.name,
                    limit=int(args.songs_per_category),
                    release_lte=d,
                    exclude_ids=used_song_ids,
                )
            else:
                ids = await fetch_songs_for_dance(
                    prisma,
                    dance_name=cat.name,
                    limit=int(args.songs_per_category),
                    min_rating=int(args.min_dance_rating),
                    release_lte=d,
                    exclude_ids=used_song_ids,
                )

            used_song_ids.update(ids)
            cat_songs.append((cat, ids))

        await upsert_week_recommendations(
            prisma,
            year=year,
            week=week,
            categories=cat_songs,
            force=bool(args.force),
            dry_run=bool(args.dry_run),
        )
    finally:
        await prisma.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
