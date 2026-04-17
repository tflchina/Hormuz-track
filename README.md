# Hormuz-track

Collect AIS ship tracks around the Strait of Hormuz and create hourly timelapses for the latest **12h**, **24h**, or **48h**.

## What this does

- Streams live AIS position reports inside a Hormuz bounding box.
- Stores one merged CSV (`data/positions.csv`) and hourly snapshots (`data/hourly/positions_YYYYMMDD_HH.csv`).
- Builds a GIF timelapse using hourly frames for the last 12/24/48 hours.
- Includes a GitHub Pages website (`docs/`) that displays the latest 12/24/48h timelapses.

> Data source in this project is [`AISStream`](https://aisstream.io/) WebSocket API and requires an API key.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set your key:

```bash
export AISSTREAM_API_KEY="your_key_here"
```

## 1) Collect ship positions around Hormuz

Run a collector for 48h (hour-by-hour data is written automatically):

```bash
python hormuz_track.py collect --hours 48 --out-dir data
```

You can tune the bounding box (`min_lat min_lon max_lat max_lon`) if needed:

```bash
python hormuz_track.py collect --hours 24 --bbox 24.0 55.0 28.5 59.5
```

## 2) Build timelapses for latest 12/24/48h

After (or during) collection:

```bash
python hormuz_track.py timelapse --window-hours 12 --output docs/timelapses/hormuz_12h.gif
python hormuz_track.py timelapse --window-hours 24 --output docs/timelapses/hormuz_24h.gif
python hormuz_track.py timelapse --window-hours 48 --output docs/timelapses/hormuz_48h.gif
```

Each frame represents one hour, using all points up to that hour (trail effect).

## 3) Update website manifest

Update `docs/timelapses/manifest.json` with the latest UTC timestamp each time new GIFs are generated:

```json
{
  "updated_at_utc": "2026-04-17T12:00:00Z",
  "timelapses": {
    "12": "timelapses/hormuz_12h.gif",
    "24": "timelapses/hormuz_24h.gif",
    "48": "timelapses/hormuz_48h.gif"
  }
}
```

## 4) Publish website on GitHub Pages

1. Push this repo to GitHub.
2. In **Settings → Pages**, set source to **Deploy from a branch**.
3. Select your branch (for example `main`) and folder **`/docs`**.
4. Save; your site will be published at your GitHub Pages URL.

## Output layout

- `data/raw_stream.jsonl`: raw stream payloads (one JSON per line)
- `data/positions.csv`: merged position rows
- `data/hourly/positions_YYYYMMDD_HH.csv`: per-hour snapshots
- `docs/timelapses/*.gif`: timelapses used by website
- `docs/index.html`: site entry point

## Notes

- For exact 48h timelapse, keep collector running at least 48 hours.
- If less data exists, timelapse will use whatever is available in that window.
- You can schedule `collect` with systemd/cron for continuous operation.
