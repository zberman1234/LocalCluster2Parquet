# Makefile for Kafka setup and Java project

# Kafka variables
KAFKA_PATH = /opt/kafka-3.4.0-src
TOPIC_NAME = my_topic

# Java project variables
MAIN_CLASS = KafkaConsumerApp
OUTPUT_PATH = /parquet

.PHONY: start-kafka stop-kafka

start-zookeeper:
	$(KAFKA_PATH)/bin/zookeeper-server-start.sh $(KAFKA_PATH)/config/zookeeper.properties

start-kafka:
	$(KAFKA_PATH)/bin/kafka-server-start.sh $(KAFKA_PATH)/config/server.properties

stop-kafka:
	$(KAFKA_PATH)/bin/kafka-server-stop.sh

start-broker:
	$(KAFKA_PATH)/bin/kafka-server-start.sh -daemon $(KAFKA_PATH)/config/server.properties

stop-zookeeper:
	$(KAFKA_PATH)/bin/zookeeper-server-stop.sh

produce:
	$(KAFKA_PATH)/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic $(TOPIC_NAME)


