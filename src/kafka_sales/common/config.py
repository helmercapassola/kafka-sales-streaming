
import os

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC_SALES = os.getenv(
    "KAFKA_TOPIC_SALES",
    "sales"
)

# Application configuration
APP_ENV = os.getenv(
    "APP_ENV",
    "development"
)
