import json
import time
import random
import argparse
from typing import Dict, Any
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker


class KafkaJsonProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=5,
            max_request_size=5242880,  # 1 MB
            buffer_memory=5242880,  # 1 MB
            request_timeout_ms=15000,
            api_version_auto_timeout_ms=5000
        )
        self.fake = Faker()

        # id
        self._customer_id_start = 0

    def _generate_customer_data(self) -> Dict[str, Any]:
        _customer_id = self._customer_id_start + 1
        self._customer_id_start += 1
        return {
            "customer_id": _customer_id,
            "order_id": random.randint(1000, 9999),
            "product_id": random.randint(1000, 9999),
            "created_at": datetime.now().isoformat(),
            "status": random.choice(["pending", "completed", "failed"]),
            "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),
            "amount": round(random.uniform(10.0, 500.0), 2),
        }

    def produce_messages(self, num_messages: int, wait_time_ms: int = 0):
        sleep_time = wait_time_ms / 1000.0

        print(
            f"Producing {num_messages} messages to topic '{self.topic}' with a wait time of {wait_time_ms} ms between messages."
        )

        messages_sent = 0
        start_time = time.time()

        try:
            for _ in range(num_messages):
                message = self._generate_customer_data()
                self.producer.send(self.topic, value=message)
                messages_sent += 1

                if messages_sent % 100 == 0:
                    print(f"Produced {messages_sent}/{num_messages} messages...")

                if sleep_time > 0:
                    time.sleep(sleep_time)
            self.producer.flush()
            elapsed_time = time.time() - start_time
            print(f"Produced {messages_sent} messages in {elapsed_time:.2f} seconds.")
        except Exception as e:
            print(f"Error producing messages: {e}")
        finally:
            self.producer.close()
            print("Producer closed.")


def main():
    parser = argparse.ArgumentParser(description="Kafka JSON Producer")
    parser.add_argument(
        "--bootstrap_servers", type=str, required=True, help="Kafka bootstrap servers"
    )
    parser.add_argument(
        "--topic", type=str, required=True, help="Kafka topic to produce messages to"
    )
    parser.add_argument(
        "--num_messages", type=int, required=True, help="Number of messages to produce"
    )
    parser.add_argument(
        "--wait_time_ms",
        type=int,
        default=0,
        help="wait time in milliseconds between messages",
    )

    args = parser.parse_args()
    producer = KafkaJsonProducer(
        bootstrap_servers=args.bootstrap_servers, topic=args.topic
    )

    producer.produce_messages(
        num_messages=args.num_messages, wait_time_ms=args.wait_time_ms
    )


if __name__ == "__main__":
    main()
