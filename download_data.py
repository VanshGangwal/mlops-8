"""
Download NYC TLC yellow taxi Parquet files and the zone lookup CSV.

Usage:
    python download_data.py                         # 2023, all 12 months
    python download_data.py --year 2023 --months 1 2 3
    python download_data.py --dest ./data/raw

Data sharing across 2 nodes:
  Option A (NFS): mount the same NFS share at identical paths on both nodes.
  Option B (scp): scp -r ./data/ user@worker-node:/path/to/assignment/data/
"""

import argparse
import os
import sys

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download_file(url: str, dest_path: str) -> None:
    if os.path.exists(dest_path):
        print(f"  [skip] {os.path.basename(dest_path)} already exists")
        return
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    print(f"  [dl]   {url}")
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    print(f"\r  {pct:3d}% {downloaded>>20}MB/{total>>20}MB", end="", flush=True)
    print(f"\r  [ok]   {os.path.basename(dest_path)} ({downloaded>>20} MB)")


def main():
    parser = argparse.ArgumentParser(description="Download NYC TLC taxi data")
    parser.add_argument("--year", type=int, default=2023)
    parser.add_argument("--months", type=int, nargs="+", default=list(range(1, 13)))
    parser.add_argument("--dest", default="./data/raw")
    args = parser.parse_args()

    print(f"Destination: {args.dest}")
    print(f"Downloading {len(args.months)} month(s) of {args.year} yellow taxi data ...")

    for month in args.months:
        fname = f"yellow_tripdata_{args.year}-{month:02d}.parquet"
        url = f"{BASE_URL}/{fname}"
        dest = os.path.join(args.dest, fname)
        try:
            download_file(url, dest)
        except requests.HTTPError as e:
            print(f"  [warn] {fname}: {e}", file=sys.stderr)

    zone_dest = os.path.join(args.dest, "taxi_zone_lookup.csv")
    print("Downloading zone lookup ...")
    download_file(ZONE_URL, zone_dest)

    print("\nDone.")
    total_size = sum(
        os.path.getsize(os.path.join(args.dest, f))
        for f in os.listdir(args.dest)
        if os.path.isfile(os.path.join(args.dest, f))
    )
    print(f"Total data size: {total_size >> 20} MB in {args.dest}")


if __name__ == "__main__":
    main()
