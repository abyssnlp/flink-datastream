"""
Reset committed offsets for a Kafka consumer group.

Examples:
    uv run reseeker --group grafana-f1 --topic f1_car_data --to beginning
    uv run reseeker --group grafana-f1 --topic f1_car_data --to beginning --execute
    uv run reseeker --group grafana-f1 --topic f1_car_data --offset 42 --execute
"""

import argparse
from typing import Dict, List, Optional

from confluent_kafka import (
    OFFSET_INVALID,
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient


def parse_partitions(value: Optional[str]) -> Optional[List[int]]:
    if value is None:
        return None

    partitions = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        partitions.append(int(item))
    return partitions


def get_topic_partitions(
    admin_client: AdminClient,
    topic: str,
    timeout: float,
) -> List[int]:
    metadata = admin_client.list_topics(topic=topic, timeout=timeout)
    topic_metadata = metadata.topics.get(topic)

    if topic_metadata is None:
        raise ValueError(f"Topic {topic!r} does not exist.")
    if topic_metadata.error is not None:
        raise KafkaException(topic_metadata.error)

    return sorted(topic_metadata.partitions.keys())


def get_current_offsets(
    admin_client: AdminClient,
    group_id: str,
    topic: str,
    partitions: List[int],
    request_timeout: float,
) -> Dict[int, int]:
    topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
    request = ConsumerGroupTopicPartitions(group_id, topic_partitions)
    futures = admin_client.list_consumer_group_offsets(
        [request],
        request_timeout=request_timeout,
    )
    result = futures[group_id].result()

    return {
        topic_partition.partition: topic_partition.offset
        for topic_partition in result.topic_partitions
        if topic_partition.topic == topic
    }


def get_watermark_targets(
    broker: str,
    topic: str,
    partitions: List[int],
    target: str,
    timeout: float,
) -> Dict[int, int]:
    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "__offset_reseeker",
            "enable.auto.commit": False,
        }
    )

    try:
        offsets = {}
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=timeout,
            )
            offsets[partition] = low if target == "beginning" else high
        return offsets
    finally:
        consumer.close()


def alter_offsets(
    admin_client: AdminClient,
    group_id: str,
    topic: str,
    target_offsets: Dict[int, int],
    request_timeout: float,
) -> None:
    topic_partitions = [
        TopicPartition(topic, partition, offset)
        for partition, offset in sorted(target_offsets.items())
    ]
    request = ConsumerGroupTopicPartitions(group_id, topic_partitions)
    futures = admin_client.alter_consumer_group_offsets(
        [request],
        request_timeout=request_timeout,
    )
    result = futures[group_id].result()

    partition_errors = [
        topic_partition
        for topic_partition in result.topic_partitions
        if topic_partition.error is not None
    ]
    if partition_errors:
        error_messages = ", ".join(
            f"{topic_partition.topic}[{topic_partition.partition}]: {topic_partition.error}"
            for topic_partition in partition_errors
        )
        raise RuntimeError(f"Failed to alter offsets: {error_messages}")


def format_offset(offset: Optional[int]) -> str:
    if offset is None or offset == OFFSET_INVALID:
        return "unset"
    return str(offset)


def print_plan(
    topic: str,
    group_id: str,
    current_offsets: Dict[int, int],
    target_offsets: Dict[int, int],
    execute: bool,
) -> None:
    status = "updated" if execute else "planned"
    print(f"Consumer group: {group_id}")
    print(f"Topic: {topic}")
    print(f"Action: {status}")
    print()
    print(f"{'partition':>9}  {'current':>10}  {'target':>10}")
    for partition, target_offset in sorted(target_offsets.items()):
        current_offset = current_offsets.get(partition)
        print(
            f"{partition:>9}  {format_offset(current_offset):>10}  {target_offset:>10}"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Reset committed offsets for one Kafka consumer group and topic. "
            "Stop consumers in the group before executing the reset."
        )
    )
    parser.add_argument(
        "--broker",
        default="localhost:19092",
        help="Kafka bootstrap server. Default: %(default)s",
    )
    parser.add_argument("--group", required=True, help="Consumer group id to update.")
    parser.add_argument("--topic", required=True, help="Topic whose offsets to update.")
    parser.add_argument(
        "--partitions",
        help="Comma-separated partitions to update. Default: all topic partitions.",
    )

    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument(
        "--to",
        choices=("beginning", "end"),
        help="Reset to the topic's low watermark or high watermark.",
    )
    target.add_argument(
        "--offset", type=int, help="Reset every selected partition to this offset."
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Commit the offset reset. Without this flag, only prints the plan.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Metadata and watermark timeout in seconds. Default: %(default)s",
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=30.0,
        help="Admin request timeout in seconds. Default: %(default)s",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    admin_client = AdminClient({"bootstrap.servers": args.broker})

    partitions = parse_partitions(args.partitions)
    if partitions is None:
        partitions = get_topic_partitions(admin_client, args.topic, args.timeout)

    if args.offset is not None:
        target_offsets = {partition: args.offset for partition in partitions}
    else:
        target_offsets = get_watermark_targets(
            args.broker,
            args.topic,
            partitions,
            args.to,
            args.timeout,
        )

    current_offsets = get_current_offsets(
        admin_client,
        args.group,
        args.topic,
        partitions,
        args.request_timeout,
    )

    if args.execute:
        alter_offsets(
            admin_client,
            args.group,
            args.topic,
            target_offsets,
            args.request_timeout,
        )

    print_plan(args.topic, args.group, current_offsets, target_offsets, args.execute)

    if not args.execute:
        print()
        print("Dry run only. Add --execute to commit these offsets.")


if __name__ == "__main__":
    main()
