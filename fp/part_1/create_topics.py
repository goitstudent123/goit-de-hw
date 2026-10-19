import time

from kafka.admin import KafkaAdminClient, NewTopic

from config import OUTPUT_TOPIC, RESULTS_TOPIC, TOPIC_PREFIX, kafka_config

TOPICS = [RESULTS_TOPIC, OUTPUT_TOPIC]


def recreate_topics(admin, names, partitions=2):
    # start from empty topics, so a rerun doesn't read messages of the previous one
    existing = [name for name in names if name in admin.list_topics()]
    if existing:
        admin.delete_topics(existing)
        while any(name in admin.list_topics() for name in existing):
            time.sleep(1)
        print(f"[topics] deleted {', '.join(existing)}")

    admin.create_topics(
        [
            NewTopic(name, num_partitions=partitions, replication_factor=1)
            for name in names
        ]
    )
    print(f"[topics] created {', '.join(names)}")


def main():
    admin = KafkaAdminClient(**kafka_config())
    try:
        recreate_topics(admin, TOPICS)
        print("[topics] on broker:")
        for topic in sorted(admin.list_topics()):
            if topic.startswith(TOPIC_PREFIX):
                print(topic)
    finally:
        admin.close()


if __name__ == "__main__":
    main()
