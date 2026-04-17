#!/usr/bin/env python3
"""Collect AIS ship positions around the Strait of Hormuz and build timelapses.

Usage examples:
  AISSTREAM_API_KEY=... python hormuz_track.py collect --hours 48
  python hormuz_track.py timelapse --window-hours 12 --output out/hormuz_12h.gif
  python hormuz_track.py timelapse --window-hours 24 --output out/hormuz_24h.gif
  python hormuz_track.py timelapse --window-hours 48 --output out/hormuz_48h.gif
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter

try:
    import websocket
except ImportError as exc:  # pragma: no cover - runtime dependency check
    raise SystemExit(
        "Missing dependency 'websocket-client'. Install with: pip install websocket-client"
    ) from exc


DEFAULT_BBOX = (24.0, 55.0, 28.5, 59.5)  # min_lat, min_lon, max_lat, max_lon
CSV_HEADERS = ["timestamp", "mmsi", "lat", "lon", "sog", "cog"]


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_iso8601(value: str) -> dt.datetime:
    value = value.replace("Z", "+00:00")
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def floor_hour(t: dt.datetime) -> dt.datetime:
    return t.replace(minute=0, second=0, microsecond=0)


def parse_position_message(payload: dict) -> dict | None:
    """Extract one normalized position row from an AISStream payload."""
    message = payload.get("Message") or {}
    report = message.get("PositionReport")
    metadata = payload.get("MetaData") or {}

    if not report:
        return None

    lat = report.get("Latitude")
    lon = report.get("Longitude")
    if lat is None or lon is None:
        return None

    timestamp = metadata.get("time_utc")
    if not timestamp:
        timestamp = utcnow().isoformat()

    mmsi = metadata.get("MMSI") or report.get("UserID")
    if mmsi is None:
        return None

    row = {
        "timestamp": parse_iso8601(timestamp).isoformat(),
        "mmsi": str(mmsi),
        "lat": float(lat),
        "lon": float(lon),
        "sog": report.get("Sog"),
        "cog": report.get("Cog"),
    }
    return row


def append_csv_row(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_HEADERS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def collect_positions(args: argparse.Namespace) -> None:
    api_key = args.api_key or os.getenv("AISSTREAM_API_KEY")
    if not api_key:
        raise SystemExit("Provide --api-key or set AISSTREAM_API_KEY.")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / "raw_stream.jsonl"
    merged_path = out_dir / "positions.csv"

    min_lat, min_lon, max_lat, max_lon = args.bbox

    subscription = {
        "APIKey": api_key,
        "BoundingBoxes": [[[min_lat, min_lon], [max_lat, max_lon]]],
        "FilterMessageTypes": ["PositionReport"],
    }

    ws = websocket.create_connection("wss://stream.aisstream.io/v0/stream")
    ws.send(json.dumps(subscription))

    started = utcnow()
    deadline = started + dt.timedelta(hours=args.hours)
    print(f"Collecting AIS positions until {deadline.isoformat()} ...")

    rows_seen = 0
    hourly_counts: dict[str, int] = defaultdict(int)

    try:
        while utcnow() < deadline:
            try:
                raw = ws.recv()
            except Exception:
                # reconnect automatically
                ws.close()
                ws = websocket.create_connection("wss://stream.aisstream.io/v0/stream")
                ws.send(json.dumps(subscription))
                continue

            with raw_path.open("a", encoding="utf-8") as handle:
                handle.write(raw + "\n")

            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            row = parse_position_message(payload)
            if not row:
                continue

            ts = parse_iso8601(row["timestamp"])
            hour_tag = floor_hour(ts).strftime("%Y%m%d_%H")
            hourly_path = out_dir / "hourly" / f"positions_{hour_tag}.csv"
            append_csv_row(hourly_path, row)
            append_csv_row(merged_path, row)

            rows_seen += 1
            hourly_counts[hour_tag] += 1
            if rows_seen % 100 == 0:
                print(f"Captured {rows_seen} messages ...")
    finally:
        ws.close()

    print(f"Done. Captured {rows_seen} messages.")
    print("Hourly counts:")
    for hour, count in sorted(hourly_counts.items()):
        print(f"  {hour}: {count}")


def load_rows(csv_file: Path) -> list[dict]:
    if not csv_file.exists():
        raise SystemExit(f"Missing data file: {csv_file}")
    rows: list[dict] = []
    with csv_file.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                rows.append(
                    {
                        "timestamp": parse_iso8601(row["timestamp"]),
                        "mmsi": row["mmsi"],
                        "lat": float(row["lat"]),
                        "lon": float(row["lon"]),
                    }
                )
            except Exception:
                continue
    if not rows:
        raise SystemExit("No valid rows found in CSV.")
    return rows


def hourly_buckets(rows: Iterable[dict]) -> dict[dt.datetime, list[dict]]:
    buckets: dict[dt.datetime, list[dict]] = defaultdict(list)
    for row in rows:
        buckets[floor_hour(row["timestamp"])].append(row)
    return dict(sorted(buckets.items()))


def make_timelapse(args: argparse.Namespace) -> None:
    rows = load_rows(Path(args.csv))

    latest = max(r["timestamp"] for r in rows)
    cutoff = latest - dt.timedelta(hours=args.window_hours)
    window_rows = [r for r in rows if r["timestamp"] >= cutoff]
    if not window_rows:
        raise SystemExit("No rows in requested time window.")

    buckets = hourly_buckets(window_rows)
    frame_hours = list(buckets.keys())
    min_lat, min_lon, max_lat, max_lon = args.bbox

    fig, ax = plt.subplots(figsize=(10, 8))

    def draw_frame(i: int) -> None:
        ax.clear()
        hour = frame_hours[i]

        # accumulate all points up to and including this hour for a trail effect
        active = [r for r in window_rows if floor_hour(r["timestamp"]) <= hour]
        lats = [r["lat"] for r in active]
        lons = [r["lon"] for r in active]

        ax.scatter(lons, lats, s=8, alpha=0.6, c="#0066cc")
        ax.set_title(
            f"Ship tracks near Strait of Hormuz\nWindow: last {args.window_hours}h | Frame: {hour:%Y-%m-%d %H:00 UTC}"
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.set_xlim(min_lon, max_lon)
        ax.set_ylim(min_lat, max_lat)
        ax.grid(True, linestyle="--", alpha=0.35)

    anim = FuncAnimation(fig, draw_frame, frames=len(frame_hours), interval=args.interval_ms)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    anim.save(output, writer=PillowWriter(fps=args.fps))
    plt.close(fig)

    print(f"Timelapse saved: {output}")
    print(f"Frames: {len(frame_hours)} (hourly)")
    print(f"Window start: {cutoff.isoformat()}")
    print(f"Window end:   {latest.isoformat()}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)

    collect = sub.add_parser("collect", help="Stream AIS positions and store hourly CSV files.")
    collect.add_argument("--hours", type=float, default=48.0, help="How many hours to collect (default: 48)")
    collect.add_argument("--api-key", default=None, help="AISStream API key (or set AISSTREAM_API_KEY)")
    collect.add_argument("--out-dir", default="data", help="Output directory")
    collect.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("MIN_LAT", "MIN_LON", "MAX_LAT", "MAX_LON"),
        default=DEFAULT_BBOX,
        help="Bounding box around Hormuz",
    )

    tl = sub.add_parser("timelapse", help="Create a GIF timelapse from collected CSV data.")
    tl.add_argument("--csv", default="data/positions.csv", help="Merged CSV from collect step")
    tl.add_argument("--window-hours", type=int, choices=[12, 24, 48], required=True)
    tl.add_argument("--output", required=True, help="Output GIF file")
    tl.add_argument("--fps", type=int, default=2, help="Animation FPS")
    tl.add_argument("--interval-ms", type=int, default=500, help="Frame interval for rendering")
    tl.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("MIN_LAT", "MIN_LON", "MAX_LAT", "MAX_LON"),
        default=DEFAULT_BBOX,
        help="Plot bounding box",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.cmd == "collect":
        collect_positions(args)
    elif args.cmd == "timelapse":
        make_timelapse(args)
    else:  # pragma: no cover
        raise SystemExit(f"Unsupported command: {args.cmd}")


if __name__ == "__main__":
    main()
