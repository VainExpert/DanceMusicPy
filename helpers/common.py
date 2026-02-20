from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, List, Optional, Sequence, Tuple

try:
    # Optional dependency – if present, we auto-load .env.
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

from zoneinfo import ZoneInfo


BERLIN = ZoneInfo(os.getenv("TZ", "Europe/Berlin"))


@dataclass(frozen=True)
class YearMonth:
    year: int
    month: int


def prev_month(ym: YearMonth) -> YearMonth:
    if ym.month == 1:
        return YearMonth(ym.year - 1, 12)
    return YearMonth(ym.year, ym.month - 1)


def last_complete_month(today: Optional[date] = None) -> YearMonth:
    """Returns the last fully completed calendar month."""
    if today is None:
        today = datetime.now(tz=BERLIN).date()
    first_of_this_month = date(today.year, today.month, 1)
    last_day_prev_month = first_of_this_month - timedelta(days=1)
    return YearMonth(last_day_prev_month.year, last_day_prev_month.month)


def parse_year_month(s: str) -> YearMonth:
    """Parse 'YYYY-MM'."""
    parts = s.strip().split("-")
    if len(parts) != 2:
        raise ValueError("Expected YYYY-MM")
    y, m = int(parts[0]), int(parts[1])
    if not (1 <= m <= 12):
        raise ValueError("month must be 1..12")
    return YearMonth(y, m)


def iso_year_week(d: date) -> Tuple[int, int]:
    iso = d.isocalendar()
    return int(iso.year), int(iso.week)


def parse_date(s: str) -> date:
    """Parse 'YYYY-MM-DD'."""
    return datetime.strptime(s.strip(), "%Y-%m-%d").date()


def month_range(ym: YearMonth) -> Tuple[datetime, datetime]:
    """[start, end) datetimes in Europe/Berlin."""
    start = datetime(ym.year, ym.month, 1, tzinfo=BERLIN)
    if ym.month == 12:
        end = datetime(ym.year + 1, 1, 1, tzinfo=BERLIN)
    else:
        end = datetime(ym.year, ym.month + 1, 1, tzinfo=BERLIN)
    return start, end


def normalize_score_to_unit(x: Optional[float]) -> float:
    """Best-effort normalization of an arbitrary score scale to [0,1]."""
    if x is None:
        return 0.0
    try:
        v = float(x)
    except Exception:
        return 0.0

    # Common ranges:
    # -1..1 (sentiment-like)
    if -1.0 <= v <= 1.0:
        return (v + 1.0) / 2.0

    # 0..5 stars
    if 0.0 <= v <= 5.0:
        return v / 5.0

    # 0..10
    if 0.0 <= v <= 10.0:
        return v / 10.0

    # 0..100
    if 0.0 <= v <= 100.0:
        return v / 100.0

    # Fallback: clamp
    return max(0.0, min(1.0, v))


def normalize_dance_rating_to_unit(r: Optional[float]) -> float:
    """Normalize dance rating to [0,1]. Supports 1..10, 0..10, 0..100."""
    if r is None:
        return 0.0
    try:
        v = float(r)
    except Exception:
        return 0.0

    # Assume 1..10
    if 1.0 <= v <= 10.0:
        return (v - 1.0) / 9.0

    # 0..10
    if 0.0 <= v <= 10.0:
        return v / 10.0

    # 0..100
    if 0.0 <= v <= 100.0:
        return v / 100.0

    return max(0.0, min(1.0, v))


def parse_months_spec(spec: str) -> List[int]:
    """Parse Tag.season values into a list of month numbers.

    Supported examples:
      - "11-12"   -> [11, 12]
      - "2"      -> [2]
      - "2-3"    -> [2, 3]
      - "9,10"   -> [9, 10]
      - "12-1"   -> [12, 1] (wrap-around)

    Anything unparseable returns [].
    """
    if not spec:
        return []
    s = spec.strip().lower().replace(" ", "")

    # allow comma-separated
    if "," in s:
        months: List[int] = []
        for part in s.split(","):
            months.extend(parse_months_spec(part))
        # de-dup while preserving order
        out: List[int] = []
        for m in months:
            if m not in out:
                out.append(m)
        return out

    # range a-b
    if "-" in s:
        a_str, b_str = s.split("-", 1)
        if not a_str.isdigit() or not b_str.isdigit():
            return []
        a, b = int(a_str), int(b_str)
        if not (1 <= a <= 12 and 1 <= b <= 12):
            return []
        if a <= b:
            return list(range(a, b + 1))
        # wrap-around (e.g. 12-1)
        return list(range(a, 13)) + list(range(1, b + 1))

    if s.isdigit():
        m = int(s)
        if 1 <= m <= 12:
            return [m]

    return []


def is_tag_applicable_for_date(tag_season_spec: str, d: date) -> bool:
    months = parse_months_spec(tag_season_spec)
    return d.month in months


def chunked(seq: Sequence[int], size: int) -> Iterable[Sequence[int]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]
