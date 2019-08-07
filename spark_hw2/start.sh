#!/bin/bash
export PATH=/opt/apache-maven-3.5.2/bin:$PATH

INPUT='/input_data/viachaslau_marozau'
OUTPUT='/output_data/viachaslau_marozau'

JAR='spark_hw2-jar-with-dependencies.jar'
BIDS_PATH="/input_data/viachaslau_marozau/bids.gz.parquet"
MOTELS_PATH="/input_data/viachaslau_marozau/motels.gz.parquet"
RATES_PATH="/input_data/viachaslau_marozau/exchange_rate.txt"

SPARK_HOME="/usr/hdp/current/spark-client/bin/spark-submit"

echo "Demo project spark hw2"
echo "Please, select action:"
echo "1. build jar"
echo "2. copy data to HDFS"
echo "3. execute spark task"

read item
case "$item" in
    1) echo "Selected build jar action:"
       mvn clean package
       ;;
    2) echo "Selected copy data to HDFS action:"
       hdfs dfs -mkdir -p $INPUT
       hdfs dfs -copyFromLocal -f input_data/viachaslau_marozau/* $INPUT
       ;;
    3) echo "Selected execute spark task action:"
       hdfs dfs -rm -r -f $OUTPUT
       # hdfs dfs -mkdir -p $OUTPUT
       $SPARK_HOME --master yarn --conf spark.ui.port=4041 --driver-memory 1g --executor-memory 1g target/$JAR $BIDS_PATH $MOTELS_PATH $RATES_PATH $OUTPUT
       ;;
    *) echo "Nothing selected..."
       ;;
esac