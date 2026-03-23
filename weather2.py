"""
IoT Weather Simulator — Kafka Consumer
Reads sensor data, maintains a rolling average, and alerts on high temperature.

Key concepts demonstrated:
  - Consumer groups (group_id)
  - auto_offset_reset: where to start reading ("earliest" vs "latest")
  - Manual partition assignment vs subscribe()
  - Rolling average using a deque (fixed-size window)
"""

import json
from collections import deque, defaultdict
from kafka import KafkaConsumer

TOPIC = "weather-sensors"
BOOTSTRAP_SERVERS = ["localhost:9092"]
ALERT_THRESHOLD_CELSIUS = 35.0
ROLLING_WINDOW = 10  # Number of readings to average


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Deserialize JSON bytes back to a Python dict
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        # Consumer group ID — Kafka tracks offsets per group.
        # Multiple consumers in the same group share partitions (load balance).
        # Different group IDs each get ALL messages independently.
        group_id="weather-monitor-group",
        # Where to start if no committed offset exists for this group:
        # "earliest" = from the very beginning of the topic
        # "latest"   = only new messages from now on
        auto_offset_reset="latest",
        # Automatically commit offsets every 5 seconds
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
    )


class WeatherMonitor:
    """Tracks rolling averages and emits alerts."""

    def __init__(self, window_size: int, alert_threshold: float):
        self.window_size = window_size
        self.alert_threshold = alert_threshold
        # defaultdict creates a new deque automatically for any new sensor
        self.temp_windows: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=window_size)
        )
        self.humidity_windows: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=window_size)
        )
        self.message_count = 0

    def process(self, sensor_id: str, temperature: float, humidity: float, timestamp: str):
        self.message_count += 1

        # Add to rolling windows
        self.temp_windows[sensor_id].append(temperature)
        self.humidity_windows[sensor_id].append(humidity)

        # Calculate averages (only meaningful once window fills up)
        avg_temp = sum(self.temp_windows[sensor_id]) / len(self.temp_windows[sensor_id])
        avg_hum = sum(self.humidity_windows[sensor_id]) / len(self.humidity_windows[sensor_id])
        window_filled = len(self.temp_windows[sensor_id]) == self.window_size

        # Pretty print the reading
        status = "●" if window_filled else "○"  # ○ = warming up, ● = full window
        print(
            f"  [{timestamp[11:19]}] {sensor_id:10s} "
            f"temp={temperature:6.2f}°C  "
            f"hum={humidity:5.1f}%  "
            f"{status} avg_temp={avg_temp:.2f}°C  avg_hum={avg_hum:.1f}%"
        )

        # ALERT: current reading exceeds threshold
        if temperature > self.alert_threshold:
            print(
                f"\n  ⚠️  ALERT [{sensor_id}] Temperature {temperature}°C "
                f"exceeds threshold of {self.alert_threshold}°C!\n"
            )

        # ALERT: rolling average exceeds threshold (more reliable signal)
        if window_filled and avg_temp > self.alert_threshold:
            print(
                f"\n  🔴 SUSTAINED ALERT [{sensor_id}] "
                f"Rolling avg {avg_temp:.2f}°C over last {self.window_size} readings!\n"
            )


def main():
    consumer = create_consumer()
    monitor = WeatherMonitor(
        window_size=ROLLING_WINDOW,
        alert_threshold=ALERT_THRESHOLD_CELSIUS,
    )

    print(f"Consumer started. Listening on topic '{TOPIC}'...")
    print(f"Alert threshold: {ALERT_THRESHOLD_CELSIUS}°C | Rolling window: {ROLLING_WINDOW} readings")
    print(f"Consumer group: 'weather-monitor-group'\n")
    print(f"  Legend: ○ = window filling up  ● = full {ROLLING_WINDOW}-reading window\n")

    try:
        # poll() blocks until messages arrive or timeout_ms elapses
        for message in consumer:
            sensor_id = message.key
            data = message.value

            # Log Kafka metadata (useful while learning)
            if monitor.message_count % 10 == 0:
                print(
                    f"  [kafka] partition={message.partition}  "
                    f"offset={message.offset}  "
                    f"total_read={monitor.message_count}"
                )

            monitor.process(
                sensor_id=data["sensor_id"],
                temperature=data["temperature"],
                humidity=data["humidity"],
                timestamp=data["timestamp"],
            )

    except KeyboardInterrupt:
        print(f"\nConsumer stopped. Total messages processed: {monitor.message_count}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()