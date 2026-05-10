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
import math
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Iterable

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
    value = value.strip().replace("Z", "+00:00")

    # AISStream currently sends MetaData.time_utc as a Go timestamp like:
    # "2026-05-07 15:52:48.957625783 +0000 UTC".
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(?:\.(\d+))? ([+-]\d{4}) UTC",
        value,
    )
    if match:
        timestamp, fraction, offset = match.groups()
        if fraction:
            timestamp = f"{timestamp}.{fraction[:6].ljust(6, '0')}"
            parsed = dt.datetime.strptime(f"{timestamp} {offset}", "%Y-%m-%d %H:%M:%S.%f %z")
        else:
            parsed = dt.datetime.strptime(f"{timestamp} {offset}", "%Y-%m-%d %H:%M:%S %z")
        return parsed.astimezone(dt.timezone.utc)

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


def connect_ais_stream(timeout: float) -> websocket.WebSocket:
    ws = websocket.create_connection("wss://stream.aisstream.io/v0/stream", timeout=timeout)
    ws.settimeout(timeout)
    return ws


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

    ws = connect_ais_stream(args.recv_timeout)
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
            except websocket.WebSocketTimeoutException:
                continue
            except Exception:
                # reconnect automatically
                try:
                    ws.close()
                except Exception:
                    pass
                print("Connection interrupted; reconnecting ...")
                ws = connect_ais_stream(args.recv_timeout)
                ws.send(json.dumps(subscription))
                continue

            if isinstance(raw, bytes):
                raw = raw.decode("utf-8")

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
            if rows_seen == 1 or rows_seen % 100 == 0:
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


def rows_in_bbox(rows: Iterable[dict], bbox: tuple[float, float, float, float]) -> list[dict]:
    min_lat, min_lon, max_lat, max_lon = bbox
    return [
        row
        for row in rows
        if min_lat <= row["lat"] <= max_lat and min_lon <= row["lon"] <= max_lon
    ]


def describe_bounds(rows: list[dict]) -> str:
    lats = [r["lat"] for r in rows]
    lons = [r["lon"] for r in rows]
    return (
        f"lat {min(lats):.3f}..{max(lats):.3f}, "
        f"lon {min(lons):.3f}..{max(lons):.3f}"
    )


def draw_hormuz_base_map(ax, bbox: tuple[float, float, float, float]) -> None:
    from matplotlib.patches import Polygon

    min_lat, min_lon, max_lat, max_lon = bbox
    ax.set_facecolor("#b8d7e6")

    land_color = "#e6dcc5"
    edge_color = "#8b7f68"
    coastlines = [
        # Southern Iran / Qeshm side
        [
            (55.0, 26.1),
            (55.5, 26.4),
            (56.1, 26.8),
            (56.9, 27.1),
            (57.8, 27.2),
            (58.8, 27.4),
            (59.5, 27.7),
            (59.5, 28.5),
            (55.0, 28.5),
        ],
        # Oman / UAE side
        [
            (55.0, 24.0),
            (59.5, 24.0),
            (59.5, 25.9),
            (58.8, 25.7),
            (58.0, 25.5),
            (57.2, 25.4),
            (56.4, 25.2),
            (55.7, 24.8),
            (55.0, 24.6),
        ],
        # Qeshm Island, approximate
        [
            (55.4, 26.65),
            (55.9, 26.82),
            (56.35, 26.9),
            (56.55, 26.78),
            (56.0, 26.62),
            (55.45, 26.55),
        ],
        # Musandam Peninsula, approximate
        [
            (56.15, 25.55),
            (56.45, 25.75),
            (56.55, 26.15),
            (56.25, 26.25),
            (56.05, 25.95),
        ],
    ]

    for points in coastlines:
        ax.add_patch(
            Polygon(points, closed=True, facecolor=land_color, edgecolor=edge_color, linewidth=0.9, zorder=0)
        )

    ax.text(57.4, 27.85, "Iran", fontsize=10, color="#4f4638", ha="center", va="center")
    ax.text(58.25, 24.65, "Oman", fontsize=10, color="#4f4638", ha="center", va="center")
    ax.text(55.55, 24.95, "UAE", fontsize=10, color="#4f4638", ha="center", va="center")
    ax.text(56.55, 26.45, "Strait of Hormuz", fontsize=9, color="#24485a", ha="center", va="center")

    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_aspect("equal", adjustable="box")
    ax.grid(True, linestyle="--", alpha=0.28, color="#456")


def lon_to_tile_x(lon: float, zoom: int) -> float:
    return (lon + 180.0) / 360.0 * (2**zoom)


def lat_to_tile_y(lat: float, zoom: int) -> float:
    lat_rad = math.radians(lat)
    return (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * (2**zoom)


def fetch_osm_tile(cache_dir: Path, zoom: int, x: int, y: int):
    from PIL import Image
    from urllib.error import URLError
    from urllib.request import Request, urlopen

    tile_path = cache_dir / str(zoom) / str(x) / f"{y}.png"
    if tile_path.exists():
        return Image.open(tile_path).convert("RGB")

    tile_path.parent.mkdir(parents=True, exist_ok=True)
    url = f"https://tile.openstreetmap.org/{zoom}/{x}/{y}.png"
    request = Request(url, headers={"User-Agent": "Hormuz-track/1.0"})
    try:
        with urlopen(request, timeout=15) as response:
            tile_path.write_bytes(response.read())
    except URLError as exc:
        raise RuntimeError(f"Could not fetch map tile {zoom}/{x}/{y}") from exc

    return Image.open(tile_path).convert("RGB")


def build_osm_background(bbox: tuple[float, float, float, float], zoom: int, cache_dir: Path):
    from PIL import Image

    min_lat, min_lon, max_lat, max_lon = bbox
    west_x = lon_to_tile_x(min_lon, zoom)
    east_x = lon_to_tile_x(max_lon, zoom)
    north_y = lat_to_tile_y(max_lat, zoom)
    south_y = lat_to_tile_y(min_lat, zoom)

    min_tile_x = math.floor(west_x)
    max_tile_x = math.floor(east_x)
    min_tile_y = math.floor(north_y)
    max_tile_y = math.floor(south_y)

    tile_size = 256
    width = (max_tile_x - min_tile_x + 1) * tile_size
    height = (max_tile_y - min_tile_y + 1) * tile_size
    mosaic = Image.new("RGB", (width, height))

    for x in range(min_tile_x, max_tile_x + 1):
        for y in range(min_tile_y, max_tile_y + 1):
            tile = fetch_osm_tile(cache_dir, zoom, x, y)
            mosaic.paste(tile, ((x - min_tile_x) * tile_size, (y - min_tile_y) * tile_size))

    left = round((west_x - min_tile_x) * tile_size)
    right = round((east_x - min_tile_x) * tile_size)
    top = round((north_y - min_tile_y) * tile_size)
    bottom = round((south_y - min_tile_y) * tile_size)
    return mosaic.crop((left, top, right, bottom))


def draw_map_background(ax, args: argparse.Namespace, osm_background=None) -> None:
    min_lat, min_lon, max_lat, max_lon = args.bbox
    ax.set_xlim(min_lon, max_lon)
    ax.set_ylim(min_lat, max_lat)
    ax.set_aspect("equal", adjustable="box")

    if args.map_background == "none":
        ax.grid(True, linestyle="--", alpha=0.35)
        return

    if args.map_background == "simple":
        draw_hormuz_base_map(ax, args.bbox)
        return

    if osm_background is None:
        draw_hormuz_base_map(ax, args.bbox)
        return

    ax.imshow(osm_background, extent=[min_lon, max_lon, min_lat, max_lat], origin="upper", zorder=0)
    ax.text(
        max_lon,
        min_lat,
        "(C) OpenStreetMap contributors",
        ha="right",
        va="bottom",
        fontsize=7,
        color="#333333",
        bbox={"facecolor": "white", "alpha": 0.65, "edgecolor": "none", "pad": 1.5},
        zorder=4,
    )
    ax.grid(True, linestyle="--", alpha=0.18, color="#333333")


def make_timelapse(args: argparse.Namespace) -> None:
    matplotlib_config_dir = Path(os.getenv("TMPDIR", "/tmp")) / "hormuz_matplotlib"
    matplotlib_config_dir.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(matplotlib_config_dir))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.animation import FuncAnimation, PillowWriter

    rows = load_rows(Path(args.csv))
    min_lat, min_lon, max_lat, max_lon = args.bbox

    latest = max(r["timestamp"] for r in rows)
    cutoff = latest - dt.timedelta(hours=args.window_hours)
    window_rows = [r for r in rows if r["timestamp"] >= cutoff]
    if not window_rows:
        raise SystemExit("No rows in requested time window.")

    window_rows = rows_in_bbox(window_rows, args.bbox)
    if not window_rows:
        all_window_rows = [r for r in rows if r["timestamp"] >= cutoff]
        raise SystemExit(
            "No rows in the requested map bounding box. "
            f"Window data bounds are {describe_bounds(all_window_rows)}; "
            f"map bbox is lat {min_lat:.3f}..{max_lat:.3f}, lon {min_lon:.3f}..{max_lon:.3f}. "
            "If these bounds look swapped or far away, recollect data with the current fixed collector."
        )

    buckets = hourly_buckets(window_rows)
    frame_hours = list(buckets.keys())

    fig, ax = plt.subplots(figsize=(10, 8))
    osm_background = None
    if args.map_background == "osm":
        try:
            osm_background = build_osm_background(args.bbox, args.map_zoom, Path(args.map_cache))
        except Exception as exc:
            print(f"Map tile background unavailable ({exc}); using simple map.")

    def draw_frame(i: int) -> None:
        ax.clear()
        hour = frame_hours[i]
        draw_map_background(ax, args, osm_background)

        # accumulate all points up to and including this hour for a trail effect
        active = [r for r in window_rows if floor_hour(r["timestamp"]) <= hour]
        lats = [r["lat"] for r in active]
        lons = [r["lon"] for r in active]

        ax.scatter(lons, lats, s=18, alpha=0.85, c="#ff3b30", edgecolors="white", linewidths=0.35, zorder=5)
        ax.set_title(
            f"Ship tracks near Strait of Hormuz\nWindow: last {args.window_hours}h | Frame: {hour:%Y-%m-%d %H:00 UTC}"
        )
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")

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
        "--recv-timeout",
        type=float,
        default=30.0,
        help="Seconds to wait for a stream message before checking the deadline (default: 30)",
    )
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
        "--map-background",
        choices=["osm", "simple", "none"],
        default="osm",
        help="Background map style for timelapse frames (default: osm)",
    )
    tl.add_argument("--map-zoom", type=int, default=7, help="OpenStreetMap tile zoom when using --map-background osm")
    tl.add_argument("--map-cache", default="data/map_tiles", help="Directory for cached OpenStreetMap tiles")
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
