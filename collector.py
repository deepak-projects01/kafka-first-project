"""
collector.py — Kafka Stream → pandas DataFrame → File Export
Collects N messages from Kafka, then saves to CSV, Parquet, JSON, and Excel.

Run this WHILE your producer.py is running.
"""

import json
import pandas as pd
from kafka import KafkaConsumer
from datetime import datetime
from pathlib import Path

TOPIC = "weather-sensors"
BOOTSTRAP_SERVERS = ["localhost:9092"]
COLLECT_N = 300          # Collect 300 messages (~100 per sensor over ~100s), then save
OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)


def collect_from_kafka(n: int) -> pd.DataFrame:
    """Read exactly n messages from Kafka and return as a DataFrame."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="data-collector-group",
        auto_offset_reset="earliest",  # Read from beginning for a full dataset
        consumer_timeout_ms=10_000,    # Stop if no message for 10s
    )

    records = []
    print(f"Collecting {n} messages from Kafka topic '{TOPIC}'...")

    for i, message in enumerate(consumer):
        records.append(message.value)
        if (i + 1) % 50 == 0:
            print(f"  ...collected {i + 1}/{n}")
        if len(records) >= n:
            break

    consumer.close()
    print(f"Done. Collected {len(records)} records.\n")

    df = pd.DataFrame(records)

    # Parse timestamp string → proper datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # Sort by time
    df = df.sort_values("timestamp").reset_index(drop=True)

    return df


def export_all_formats(df: pd.DataFrame):
    """Save the DataFrame to every common format a data scientist uses."""

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    # ── CSV ────────────────────────────────────────────────────────────────
    # Best for: sharing, Excel users, simple logging
    # Drawback: no type info, large files, slow for big data
    csv_path = OUTPUT_DIR / f"weather_{timestamp_str}.csv"
    df.to_csv(csv_path, index=False)
    print(f"CSV     saved → {csv_path}  ({csv_path.stat().st_size / 1024:.1f} KB)")

    # ── Parquet ────────────────────────────────────────────────────────────
    # Best for: production pipelines, big data, fast column reads
    # Requires: pyarrow  →  pip install pyarrow
    # Advantage: 3-10× smaller than CSV, preserves dtypes (datetime, float64)
    parquet_path = OUTPUT_DIR / f"weather_{timestamp_str}.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow", compression="snappy")
    print(f"Parquet saved → {parquet_path}  ({parquet_path.stat().st_size / 1024:.1f} KB)")

    # ── JSON Lines (.jsonl) ────────────────────────────────────────────────
    # Best for: REST APIs, NoSQL ingestion, one JSON object per line
    # Advantage: human-readable, append-friendly
    jsonl_path = OUTPUT_DIR / f"weather_{timestamp_str}.jsonl"
    df.to_json(jsonl_path, orient="records", lines=True, date_format="iso")
    print(f"JSONL   saved → {jsonl_path}  ({jsonl_path.stat().st_size / 1024:.1f} KB)")

    # ── JSON (nested array) ────────────────────────────────────────────────
    json_path = OUTPUT_DIR / f"weather_{timestamp_str}.json"
    df.to_json(json_path, orient="records", indent=2, date_format="iso")
    print(f"JSON    saved → {json_path}  ({json_path.stat().st_size / 1024:.1f} KB)")

    # ── Excel ──────────────────────────────────────────────────────────────
    # Best for: business reports, non-technical stakeholders
    # Requires: openpyxl  →  pip install openpyxl
    xlsx_path = OUTPUT_DIR / f"weather_{timestamp_str}.xlsx"
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="raw_data", index=False)
        # Also write a per-sensor summary sheet
        summary = df.groupby("sensor_id").agg(
            count=("temperature", "count"),
            avg_temp=("temperature", "mean"),
            max_temp=("temperature", "max"),
            min_temp=("temperature", "min"),
            avg_humidity=("humidity", "mean"),
        ).round(2)
        summary.to_excel(writer, sheet_name="summary")
    print(f"Excel   saved → {xlsx_path}  ({xlsx_path.stat().st_size / 1024:.1f} KB)")

    # ── HDF5 (optional — great for very large numeric arrays) ─────────────
    # pip install tables
    # hdf5_path = OUTPUT_DIR / f"weather_{timestamp_str}.h5"
    # df.to_hdf(hdf5_path, key="sensors", mode="w", complevel=9)

    print(f"\nAll files saved to ./{OUTPUT_DIR}/")
    return df


if __name__ == "__main__":
    df = collect_from_kafka(COLLECT_N)
    export_all_formats(df)
    print("\nDataFrame preview:")
    print(df.head(10).to_string())
    print(f"\nShape: {df.shape}  |  dtypes:\n{df.dtypes}")