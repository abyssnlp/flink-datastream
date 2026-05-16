"""
Get data from OpenF1 API and produce to Kafka
Get drivers
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import List, Dict
from itertools import chain
import json
import requests
import requests.adapters
from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic


class F1Producer:
    def __init__(
        self,
        broker: str,
        session_key: int,
        meeting_key: int,
        base_url: str,
        kafka_topics: Dict[str, str],
        replay_speed: float = 60.0,
    ):
        if replay_speed <= 0:
            raise ValueError("replay_speed must be greater than 0")

        self.producer = Producer(
            {
                "bootstrap.servers": broker,
                "linger.ms": 50,
                "batch.num.messages": 1000,
                "compression.type": "lz4",
            }
        )
        self.broker = broker
        self._session = self._get_session()
        self.session_key = session_key
        self.meeting_key = meeting_key
        self.base_url = base_url
        self.kafka_topics = kafka_topics
        self.replay_speed = replay_speed
        self.drivers = self.get_drivers()

    def _ensure_topics(self) -> None:
        admin_client = AdminClient(
            {
                "bootstrap.servers": self.broker,
            }
        )

        new_topics = [
            NewTopic(topic, num_partitions=3, replication_factor=1)
            for topic in self.kafka_topics.values()
        ]

        futures = admin_client.create_topics(new_topics, operation_timeout=30)

        for topic, future in futures.items():
            try:
                future.result()
                print(f"Topic '{topic}' created successfully.")
            except Exception as e:
                error = e.args[0]
                if error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"Topic '{topic}' already exists.")
                else:
                    print(f"Failed to create topic '{topic}': {e}")
                    raise

    def _get_session(self) -> requests.Session:
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10, pool_maxsize=10, max_retries=3
        )
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"Accept": "application/json"})
        return session

    @staticmethod
    def delivery_callback(err, msg):
        if err:
            print(f"Message failed delivery: {err}")
        else:
            print(
                f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
            )

    def _produce_one(
        self,
        topic: str,
        record: dict,
        event_time: datetime | None = None,
    ) -> None:
        payload = json.dumps(record).encode("utf-8")
        timestamp_ms = (
            int(event_time.timestamp() * 1000) if event_time is not None else None
        )

        while True:
            try:
                if timestamp_ms is None:
                    self.producer.produce(
                        topic,
                        value=payload,
                        callback=self.delivery_callback,
                    )
                else:
                    self.producer.produce(
                        topic,
                        value=payload,
                        timestamp=timestamp_ms,
                        callback=self.delivery_callback,
                    )
                break
            except BufferError:
                self.producer.poll(1.0)

        self.producer.poll(0)

    def _produce_many(self, topic: str, records: List[dict]) -> None:
        for record in records:
            self._produce_one(topic, record)

        self.producer.flush()

    @staticmethod
    def _parse_event_time(record: dict) -> datetime:
        value = record.get("date")
        if not value:
            raise ValueError(f"Record is missing required date field: {record}")

        event_time = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        return event_time.astimezone(timezone.utc)

    def _produce_telemetry_in_event_time(
        self,
        topic_records: Dict[str, List[dict]],
    ) -> None:
        events = [
            (self._parse_event_time(record), topic, record)
            for topic, records in topic_records.items()
            for record in records
        ]
        events.sort(key=lambda event: event[0])

        if not events:
            return

        first_event_time = events[0][0]
        playback_started_at = time.monotonic()

        for event_time, topic, record in events:
            event_offset_seconds = (event_time - first_event_time).total_seconds()
            target_elapsed_seconds = event_offset_seconds / self.replay_speed
            sleep_seconds = (
                playback_started_at + target_elapsed_seconds - time.monotonic()
            )
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

            self._produce_one(topic, record, event_time)

        self.producer.flush()

    def produce_messages(self) -> None:
        self._ensure_topics()
        self._produce_many(self.kafka_topics["drivers"], self.drivers)

        self._produce_telemetry_in_event_time(
            {
                self.kafka_topics["position"]: self.get_positions(),
                self.kafka_topics["location"]: self.get_location(),
                self.kafka_topics["car_data"]: self.get_car_data(),
            }
        )

    def _get_api_data(self, endpoint: str, params: Dict) -> List[Dict]:
        url = f"{self.base_url}/{endpoint}"
        response = self._session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_drivers(self) -> List[Dict]:
        return self._get_api_data(
            "drivers",
            {
                "session_key": self.session_key,
                "meeting_key": self.meeting_key,
            },
        )

    def get_positions(self) -> List[Dict]:
        return self._get_api_data(
            "position",
            {
                "session_key": self.session_key,
                "meeting_key": self.meeting_key,
            },
        )

    def get_location(self) -> List[Dict]:
        locations = []
        for driver in self.drivers:
            time.sleep(1)
            locations.append(
                self._get_api_data(
                    "location",
                    {
                        "session_key": self.session_key,
                        "driver_number": driver["driver_number"],
                    },
                )
            )
        return list(chain.from_iterable(locations))

    def get_car_data(self) -> List[Dict]:
        car_data = []
        for driver in self.drivers:
            time.sleep(1)
            try:
                car_data.append(
                    self._get_api_data(
                        "car_data",
                        {
                            "session_key": self.session_key,
                            "driver_number": driver["driver_number"],
                        },
                    )
                )
            except requests.exceptions.HTTPError as e:
                code = e.response.status_code
                if code == 404:
                    print(
                        f"Car data not found for driver {driver['driver_number']} (HTTP 404)"
                    )
                else:
                    raise
        return list(chain.from_iterable(car_data))


def main() -> None:
    F1_SESSION_KEY = 11280  # Miami Grand Prix 2026
    F1_MEETING_KEY = 1284
    KAFKA_BROKER = "localhost:19092"
    KAFKA_TOPICS: Dict[str, str] = {
        "position": "f1_position",
        "location": "f1_location",
        "car_data": "f1_car_data",
        "drivers": "f1_drivers",
    }
    BASE_URL = "https://api.openf1.org/v1"
    REPLAY_SPEED = float(os.getenv("F1_REPLAY_SPEED", "60"))
    producer = F1Producer(
        KAFKA_BROKER,
        F1_SESSION_KEY,
        F1_MEETING_KEY,
        BASE_URL,
        KAFKA_TOPICS,
        replay_speed=REPLAY_SPEED,
    )
    producer.produce_messages()


if __name__ == "__main__":
    main()
