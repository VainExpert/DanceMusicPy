from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Dict, List, Optional, Tuple

from prisma import Prisma

from .common import (
    YearMonth,
    chunked,
    last_complete_month,
    month_range,
    normalize_dance_rating_to_unit,
    normalize_score_to_unit,
    parse_year_month,
    prev_month,
)


@dataclass
class RankedSong:
    song_id: int
    score: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Generate a monthly Chart purely from the database (no website scraping). "
            "Default target is the last completed month."
        )
    )
    p.add_argument(
        "--month",
        type=str,
        default="",
        help="Target month as YYYY-MM (default: last completed month)",
    )
    p.add_argument("--top", type=int, default=30, help="How many chart entries to write")
    p.add_argument(
        "--min-votes",
        type=int,
        default=10,
        help="Bayesian prior votes for the Song.avgScore weighting",
    )
    p.add_argument(
        "--alpha",
        type=float,
        default=0.6,
        help="Weight for weighted user score",
    )
    p.add_argument(
        "--beta",
        type=float,
        default=0.4,
        help="Weight for average dance rating",
    )
    p.add_argument(
        "--use-plays",
        action="store_true",
        help="If logentries exist for that month, incorporate play-count into ranking",
    )
    p.add_argument(
        "--plays-weight",
        type=float,
        default=0.2,
        help="How much monthly plays should influence ranking (only if --use-plays)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing chart entries for that month (delete+recreate)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write to DB, only print a preview",
    )
    return p.parse_args()


async def _get_song_dance_avg(prisma: Prisma, song_ids: List[int]) -> Dict[int, float]:
    """Return songId -> avg dance rating (normalized 0..1)."""
    out: Dict[int, float] = {}

    # Pull DanceSong rows in chunks to avoid too-large queries.
    for chunk in chunked(song_ids, 500):
        ds_rows = await prisma.dancesong.find_many(
            where={"songId": {"in": list(chunk)}},
            select={"songId": True, "rating": True},
        )
        buckets: Dict[int, List[float]] = defaultdict(list)
        for r in ds_rows:
            buckets[int(r.songId)].append(normalize_dance_rating_to_unit(r.rating))

        for sid, vals in buckets.items():
            if vals:
                out[sid] = sum(vals) / len(vals)

    return out


async def _get_monthly_play_counts(prisma: Prisma, ym: YearMonth) -> Dict[int, int]:
    """songId -> play count for that month (from logentries)."""
    start_dt, end_dt = month_range(ym)

    # Using raw SQL because group_by support varies across prisma-client-py versions.
    sql = (
        "SELECT song_id AS songId, COUNT(*) AS plays "
        "FROM logentries "
        f"WHERE time >= '{start_dt.strftime('%Y-%m-%d %H:%M:%S')}' "
        f"AND time < '{end_dt.strftime('%Y-%m-%d %H:%M:%S')}' "
        "GROUP BY song_id"
    )
    rows = await prisma.query_raw(sql)

    counts: Dict[int, int] = {}
    for r in rows or []:
        try:
            counts[int(r["songId"])] = int(r["plays"])
        except Exception:
            continue
    return counts


async def rank_songs_for_month(
    prisma: Prisma,
    ym: YearMonth,
    min_votes: int,
    alpha: float,
    beta: float,
    use_plays: bool,
    plays_weight: float,
) -> List[RankedSong]:
    # Base pool: checked songs only
    songs = await prisma.song.find_many(
        where={"checked": True},
        select={"id": True, "avgScore": True, "votes": True},
    )

    if not songs:
        return []

    song_ids = [int(s.id) for s in songs]
    dance_avg = await _get_song_dance_avg(prisma, song_ids)

    play_counts: Dict[int, int] = {}
    if use_plays:
        play_counts = await _get_monthly_play_counts(prisma, ym)

    # normalize plays to 0..1
    max_plays = max(play_counts.values(), default=0)

    ranked: List[RankedSong] = []
    for s in songs:
        sid = int(s.id)
        votes = int(s.votes or 0)
        user_score_norm = normalize_score_to_unit(s.avgScore)

        # Bayesian style weighting: pull low-vote songs toward dataset average (0.5)
        prior = 0.5
        weighted_score = ((votes * user_score_norm) + (prior * min_votes)) / (votes + min_votes)

        dance_component = dance_avg.get(sid, 0.0)
        combined = alpha * weighted_score + beta * dance_component

        if use_plays and max_plays > 0:
            plays = play_counts.get(sid, 0)
            plays_norm = plays / max_plays
            # Re-normalize weights so alpha+beta+plays_weight == 1
            base_w = alpha + beta
            total_w = base_w + plays_weight
            if total_w > 0:
                combined = (alpha / total_w) * weighted_score + (beta / total_w) * dance_component + (
                    plays_weight / total_w
                ) * plays_norm

        ranked.append(RankedSong(song_id=sid, score=float(combined)))

    ranked.sort(key=lambda r: r.score, reverse=True)
    return ranked


async def upsert_month_chart(
    prisma: Prisma,
    ym: YearMonth,
    ranked: List[RankedSong],
    top_n: int,
    force: bool,
    dry_run: bool,
) -> None:
    # If chart exists and not forcing, skip.
    existing = await prisma.chart.find_first(where={"year": ym.year, "month": ym.month})
    if existing and not force:
        print(f"Chart {ym.year}-{ym.month:02d} already exists. Use --force to overwrite.")
        return

    # previous lookup
    prev = prev_month(ym)
    prev_rows = await prisma.chart.find_many(
        where={"year": prev.year, "month": prev.month},
        select={"songId": True, "placement": True},
    )
    prev_pos = {int(r.songId): int(r.placement) for r in prev_rows}

    top = ranked[:top_n]

    # Pretty preview
    song_map: Dict[int, Tuple[str, str]] = {}
    for chunk in chunked([r.song_id for r in top], 200):
        rows = await prisma.song.find_many(
            where={"id": {"in": list(chunk)}},
            include={"artist": True},
        )
        for row in rows:
            song_map[int(row.id)] = (row.title, row.artist.name)

    print(f"\n=== Monthly chart preview for {ym.year}-{ym.month:02d} (top {len(top)}) ===")
    for idx, r in enumerate(top, start=1):
        title, artist = song_map.get(r.song_id, (f"song_id={r.song_id}", ""))
        previous = prev_pos.get(r.song_id, -1)
        print(f"{idx:>2}. {artist} – {title} | score={r.score:.4f} | prev={previous}")

    if dry_run:
        print("\n(dry-run) No DB changes made.")
        return

    # Overwrite month entries for idempotency.
    await prisma.chart.delete_many(where={"year": ym.year, "month": ym.month})

    # Insert new chart entries.
    for idx, r in enumerate(top, start=1):
        await prisma.chart.create(
            data={
                "year": ym.year,
                "month": ym.month,
                "placement": idx,
                "previous": prev_pos.get(r.song_id, -1),
                "song": {"connect": {"id": r.song_id}},
            }
        )

    print(f"\nWrote {len(top)} chart entries to charts for {ym.year}-{ym.month:02d}.")


async def main() -> None:
    args = parse_args()
    target: YearMonth
    if args.month:
        target = parse_year_month(args.month)
    else:
        target = last_complete_month()

    prisma = Prisma()
    await prisma.connect()
    try:
        ranked = await rank_songs_for_month(
            prisma,
            target,
            min_votes=args.min_votes,
            alpha=args.alpha,
            beta=args.beta,
            use_plays=bool(args.use_plays),
            plays_weight=float(args.plays_weight),
        )
        if not ranked:
            print("No songs found to rank (are there checked songs in DB?).")
            return

        await upsert_month_chart(
            prisma,
            target,
            ranked,
            top_n=int(args.top),
            force=bool(args.force),
            dry_run=bool(args.dry_run),
        )
    finally:
        await prisma.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
