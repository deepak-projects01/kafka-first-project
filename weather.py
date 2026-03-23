"""
IoT Weather Simulator — Kafka Producer
Simulates 3 sensors publishing JSON weather data every second.
Key concept: message key = sensor ID → routes to a consistent partition.
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

TOPIC = "weather-sensors"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# Sensor profiles: each has a base temp and humidity range
SENSORS = {
    "sensor-A": {"base_temp": 22.0, "base_humidity": 55.0},
    "sensor-B": {"base_temp": 30.0, "base_humidity": 70.0},
    "sensor-C": {"base_temp": 18.0, "base_humidity": 45.0},
}


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Serialize the message value as JSON bytes
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # Serialize the key as UTF-8 bytes (used for partitioning)
        key_serializer=lambda k: k.encode("utf-8"),
    )


def generate_reading(sensor_id: str) -> dict:
    """Generate a realistic sensor reading with some random drift."""
    profile = SENSORS[sensor_id]
    # Add small random drift (+/- 3 degrees / 5% humidity)
    temperature = round(profile["base_temp"] + random.uniform(-3, 3), 2)
    humidity = round(profile["base_humidity"] + random.uniform(-5, 5), 2)

    return {
        "sensor_id": sensor_id,
        "temperature": temperature,   # Celsius
        "humidity": humidity,         # Percentage
        "timestamp": datetime.utcnow().isoformat(),
        "unit": "celsius",
    }


def on_send_success(record_metadata):
    print(
        f"  ✓ Sent to topic='{record_metadata.topic}' "
        f"partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(excp):
    print(f"  ✗ Failed to send: {excp}")


def main():
    producer = create_producer()
    print(f"Producer started. Publishing to topic '{TOPIC}' every 1s...\n")

    try:
        while True:
            for sensor_id in SENSORS:
                reading = generate_reading(sensor_id)

                print(f"[{reading['timestamp']}] {sensor_id} → "
                      f"temp={reading['temperature']}°C  "
                      f"humidity={reading['humidity']}%")

                # KEY CONCEPT: The message key routes this sensor to
                # a consistent partition. Same sensor always goes to
                # the same partition — ordering is preserved per sensor.
                producer.send(
                    topic=TOPIC,
                    key=sensor_id,
                    value=reading,
                ).add_callback(on_send_success).add_errback(on_send_error)

            # Flush ensures messages are actually sent before sleeping
            producer.flush()
            print()
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
