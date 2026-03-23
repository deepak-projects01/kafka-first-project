"""
stream_analyzer.py — Windowed Statistics Directly on the Live Kafka Stream
No file saving needed. Prints a stats summary every 30 seconds.

This is closer to what a real-time data scientist does with streaming data:
windowed aggregations instead of batch analysis.
"""

import json
import time
import numpy as np
import pandas as pd
from collections import deque
from datetime import datetime
from kafka import KafkaConsumer

TOPIC = "weather-sensors"
BOOTSTRAP_SERVERS = ["localhost:9092"]
WINDOW_SECONDS = 30       # Analyse the last 30 seconds of data
REPORT_EVERY_N = 30       # Print a report every N messages


class WindowedStreamAnalyzer:
    """
    Maintains a sliding time window of readings per sensor.
    Every REPORT_EVERY_N messages, prints a statistical summary.
    """

    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds
        # Store (timestamp, value) tuples per sensor
        self.windows: dict[str, deque] = {}
        self.total = 0
        self.start_time = time.time()

    def _get_window(self, sensor_id: str) -> deque:
        if sensor_id not in self.windows:
            self.windows[sensor_id] = deque()
        return self.windows[sensor_id]

    def add(self, sensor_id: str, temperature: float, humidity: float, ts: datetime):
        self.total += 1
        window = self._get_window(sensor_id)
        window.append((ts, temperature, humidity))

        # Evict readings older than window_seconds
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=self.window_seconds)
        while window and window[0][0] < cutoff.replace(tzinfo=None):
            window.popleft()

    def report(self):
        elapsed = time.time() - self.start_time
        rate = self.total / elapsed if elapsed > 0 else 0

        print(f"\n{'─' * 64}")
        print(f"  WINDOWED REPORT  |  last {self.window_seconds}s  "
              f"|  total={self.total}  |  rate={rate:.1f} msg/s")
        print(f"{'─' * 64}")

        for sensor_id, window in sorted(self.windows.items()):
            if not window:
                continue
            temps = np.array([r[1] for r in window])
            hums  = np.array([r[2] for r in window])
            n = len(temps)

            # Core stats
            t_mean, t_std = temps.mean(), temps.std()
            t_min, t_max  = temps.min(), temps.max()

            # Anomaly: readings more than 2 std devs from window mean
            anomalies = np.sum(np.abs(temps - t_mean) > 2 * t_std)

            # Trend: simple linear slope (positive = warming)
            if n > 3:
                slope = np.polyfit(range(n), temps, 1)[0]
                trend = f"+{slope:.3f}°/s" if slope >= 0 else f"{slope:.3f}°/s"
            else:
                trend = "n/a"

            alert_flag = " *** ALERT ***" if t_max > 35 else ""

            print(
                f"  {sensor_id:10s}  n={n:3d}  "
                f"temp: {t_mean:.2f}±{t_std:.2f}°C  "
                f"[{t_min:.1f}–{t_max:.1f}]  "
                f"trend={trend}  "
                f"anomalies={anomalies}  "
                f"hum={hums.mean():.1f}%"
                f"{alert_flag}"
            )

        # Cross-sensor: which sensor is hottest right now?
        if len(self.windows) > 1:
            means = {
                sid: np.mean([r[1] for r in w])
                for sid, w in self.windows.items() if w
            }
            hottest = max(means, key=means.get)
            print(f"\n  Hottest sensor right now: {hottest} ({means[hottest]:.2f}°C)")

        print(f"{'─' * 64}")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="stream-analyzer-group",
        auto_offset_reset="latest",
    )

    analyzer = WindowedStreamAnalyzer(window_seconds=WINDOW_SECONDS)
    print(f"Stream analyzer running. Reporting every {REPORT_EVERY_N} messages...\n")

    try:
        for message in consumer:
            data = message.value
            ts = datetime.fromisoformat(data["timestamp"])

            analyzer.add(
                sensor_id=data["sensor_id"],
                temperature=data["temperature"],
                humidity=data["humidity"],
                ts=ts,
            )

            if analyzer.total % REPORT_EVERY_N == 0:
                analyzer.report()

    except KeyboardInterrupt:
        print(f"\nStopped. Final report:")
        analyzer.report()
    finally:
        consumer.close()


if __name__ == "__main__":
    main()