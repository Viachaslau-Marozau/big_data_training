#!/bin/bash
export PATH=/opt/apache-maven-3.5.0/bin:$PATH

KAFKA_HOME='/usr/hdp/2.6.1.0-129/kafka/bin'
REPL_FACTOR='1'
JAR='spark_hw3-1.0-jar-with-dependencies.jar'
CLASS='com.epam.courses.spark.streaming.kafka.TopicGenerator'

echo "Kafka menu"
echo "Please, select action:"
echo "1. run kafka broker"
echo "2. run producer"
echo "3. create topic"
echo "4. show available topics"


read item
case "$item" in
    1) echo "Selected create kafka broker action:"
       $KAFKA_HOME/kafka-server-start.sh src/main/resources/conf/server.properties
       ;;
    2) echo "Selected run producer action:"
       java -cp target/$JAR $CLASS
	   ;;
    3) echo "Selected create topic action:"
       echo "Please enter topic name"
       read topic
       $KAFKA_HOME/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor $REPL_FACTOR --partitions 1 --topic $topic
       ;;
    4) echo "Selected show available topics action:"
       $KAFKA_HOME/kafka-topics.sh --list --zookeeper localhost:2181
       ;;
esac