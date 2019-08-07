#!/bin/bash
export PATH=/opt/apache-maven-3.5.0/bin:$PATH

KAFKA_HOME="/usr/hdp/2.6.1.0-129/kafka/bin"
TOPIC="testTopic"

echo "Demo project kafka"
echo "Please, select action:"
echo "1. build jar"
echo "2. start kafka servers"
echo "3. create topic"
echo "4. describe topic"
echo "5. show available topics"
echo "6. run producer"
echo "7. run consumer"

read item
case "$item" in
    1) echo "Selected build jar action:"
       mvn clean package
       ;;
    2) echo "Selected start kafka servers action:"
       $KAFKA_HOME/kafka-server-start.sh resources/kafka/server-1.properties &
       $KAFKA_HOME/kafka-server-start.sh resources/kafka/server-2.properties &
       ;;
    3) echo "Selected create topic action:"
       $KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 1 --topic $TOPIC
       ;;
    4) echo "Selected describe topic action:"
       $KAFKA_HOME/kafka-topics.sh --describe --zookeeper localhost:2181 --topic $TOPIC
       ;;
    5) echo "Selected show available topics action:"
       $KAFKA_HOME/kafka-topics,sh --list --zookeeper localhost:2181
     ;;
    6) echo "Selected run producer action:"
       echo "Enter message count"
       read number
       mvn exec:java -Dexec.mainClass="com.epam.courses.kafka.producer.KafkaFibNumProducer"  -Dexec.args="$TOPIC $number"
       ;;
    7) echo "Selected run consumer action:"
       echo "Enter number for sum output"
       read number
       mvn exec:java -Dexec.mainClass="com.epam.courses.kafka.consumer.KafkaFibNumConsumer"  -Dexec.args="$TOPIC $number"
       ;;
    *) echo "Nothing selected..."
       ;;
esac