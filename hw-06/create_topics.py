from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from config import ALERTS, BUILDING_SENSORS, TOPIC_PREFIX, kafka_config

TOPICS = [BUILDING_SENSORS, ALERTS]


def create_topics(admin, names, partitions=2):
    for name in names:
        topic = NewTopic(name, num_partitions=partitions, replication_factor=1)
        try:
            admin.create_topics([topic])
            print(f"[topics] created {name}")
        except TopicAlreadyExistsError:
            print(f"[topics] {name} already exists")


def main():
    admin = KafkaAdminClient(**kafka_config())
    try:
        create_topics(admin, TOPICS)
        print("[topics] on broker:")
        for topic in admin.list_topics():
            if TOPIC_PREFIX in topic:
                print(topic)
    finally:
        admin.close()


if __name__ == "__main__":
    main()
