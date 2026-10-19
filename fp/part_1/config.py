import os

import pyspark
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST") or "localhost"
MYSQL_PORT = os.getenv("MYSQL_PORT") or ("3306" if os.getenv("MYSQL_HOST") else "3308")
MYSQL_USER = os.getenv("MYSQL_USER") or "root"
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD") or "root"
MYSQL_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/olympic_dataset"

BIO_TABLE = "olympic_dataset.athlete_bio"
RESULTS_TABLE = "olympic_dataset.athlete_event_results"
OUTPUT_TABLE = f"{os.getenv('MYSQL_OUTPUT_SCHEMA') or 'anton'}.athlete_enriched_agg"

TOPIC_PREFIX = os.getenv("TOPIC_PREFIX") or "anton"
RESULTS_TOPIC = f"{TOPIC_PREFIX}_athlete_event_results"
OUTPUT_TOPIC = f"{TOPIC_PREFIX}_athlete_enriched_agg"

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "localhost:29095"
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
SASL_MECHANISM = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")

PACKAGES = [
    "com.mysql:mysql-connector-j:9.5.0",
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}",
]


def create_spark(app_name):
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", ",".join(PACKAGES))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def jdbc_options(table):
    return {
        "url": MYSQL_URL,
        "driver": "com.mysql.cj.jdbc.Driver",
        "dbtable": table,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
    }


def kafka_config():
    config = {"bootstrap_servers": BOOTSTRAP_SERVERS}
    if KAFKA_USERNAME:
        config.update(
            security_protocol=SECURITY_PROTOCOL,
            sasl_mechanism=SASL_MECHANISM,
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
        )
    return config


def spark_kafka_options():
    options = {"kafka.bootstrap.servers": BOOTSTRAP_SERVERS}
    if KAFKA_USERNAME:
        options.update(
            {
                "kafka.security.protocol": SECURITY_PROTOCOL,
                "kafka.sasl.mechanism": SASL_MECHANISM,
                "kafka.sasl.jaas.config": (
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
                ),
            }
        )
    return options
