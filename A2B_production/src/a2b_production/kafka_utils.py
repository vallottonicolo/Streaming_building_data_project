from __future__ import annotations

import json
import time
from collections.abc import Iterable
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic
from kafka.errors import InvalidPartitionsError, TopicAlreadyExistsError

from .config import AppConfig


def ensure_topics(cfg: AppConfig) -> None:
    """Create or expand configured Kafka topics to the desired partition count."""
    admin = KafkaAdminClient(
        bootstrap_servers=cfg.kafka_bootstrap,
        client_id="a2b-production-topic-admin",
    )
    try:
        existing = set(admin.list_topics())
        new_topics = []

        #Check for missing topics and create them if they don't exist
        for topic in cfg.topics.values():
            if topic not in existing:
                new_topics.append(
                    NewTopic(
                        name=topic,
                        num_partitions=int(cfg.raw["kafka"]["partitions"]),
                        replication_factor=int(cfg.raw["kafka"]["replication_factor"]),
                    )
                )
        if new_topics:
            try:
                admin.create_topics(new_topics)
            except TopicAlreadyExistsError:
                pass

    finally:
        admin.close()


def make_producer(cfg: AppConfig) -> KafkaProducer:
    """Create a JSON-serializing Kafka producer for the configured cluster."""
    return KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap,
        key_serializer=lambda v: str(v).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":"), default=str).encode("utf-8"),
        linger_ms=20,
        retries=5,
    )


def make_consumer(cfg: AppConfig, topics: Iterable[str], group_id: str | None) -> KafkaConsumer:
    """Create a JSON-deserializing Kafka consumer for the requested topics."""
    return KafkaConsumer(
        *topics,
        bootstrap_servers=cfg.kafka_bootstrap,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def topic_offsets(cfg: AppConfig) -> dict[str, dict[int, int]]:
    """Return Kafka end offsets for every configured topic and partition."""
    consumer = KafkaConsumer(bootstrap_servers=cfg.kafka_bootstrap)
    try:
        result: dict[str, dict[int, int]] = {}
        for topic in cfg.topics.values():
            partitions = consumer.partitions_for_topic(topic) or set()
            offsets: dict[int, int] = {}
            tps = []
            for partition in partitions:
                tps.append(TopicPartition(topic, partition))
            if tps:
                end = consumer.end_offsets(tps)
                offsets = {tp.partition: off for tp, off in end.items()}
            result[topic] = offsets
        return result
    finally:
        consumer.close()


def wait_for_kafka(cfg: AppConfig, timeout_s: int = 60) -> None:
    """Block until Kafka is reachable or raise after the timeout."""
    deadline = time.time() + timeout_s
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=cfg.kafka_bootstrap, client_id="a2b-production-wait")
            admin.list_topics()
            admin.close()
            return
        except Exception as exc:  # pragma: no cover - depends on Docker timing
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Kafka not reachable at {cfg.kafka_bootstrap}: {last_error}")
