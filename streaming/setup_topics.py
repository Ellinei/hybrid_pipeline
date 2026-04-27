"""Create required Kafka topics in Redpanda if they don't already exist.

Idempotent — safe to run multiple times.

Usage:
    python -m streaming.setup_topics
"""
from __future__ import annotations

import os
import sys

import structlog
from dotenv import load_dotenv
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

log = structlog.get_logger()

_TOPICS = [
    NewTopic("price-ticks",      num_partitions=3, replication_factor=1),
    NewTopic("trading-signals",  num_partitions=1, replication_factor=1),
]


def setup_topics(bootstrap_servers: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    try:
        existing = set(admin.list_topics())
        to_create = [t for t in _TOPICS if t.name not in existing]

        if not to_create:
            log.info("all_topics_already_exist", topics=[t.name for t in _TOPICS])
            return

        admin.create_topics(to_create, validate_only=False)
        for t in to_create:
            log.info("topic_created", topic=t.name, partitions=t.num_partitions)
    except TopicAlreadyExistsError:
        log.info("topics_already_exist")
    finally:
        admin.close()


if __name__ == "__main__":
    load_dotenv()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    log.info("setup_topics_starting", bootstrap=bootstrap)
    setup_topics(bootstrap)
    print("Done — Kafka topics ready.")
    sys.exit(0)
