#!/bin/bash
# export PATH=/opt/apache-maven-3.5.0/bin:$PATH
mvn package
JOB_JAR="target/hadoop_hw1-1.0-RELEASE.jar"
JOB_CLASS="LongestWordJobRunner"
INPUT="/input_data/viachaslau_marozau/*"
OUTPUT="/output_data/viachaslau_marozau/output"
# create dir if not exists
hadoop fs -test -d /input_data/viachaslau_marozau/
if [ $? -eq 0 ];
    then
        echo "HDFS remove dir: /input_data/viachaslau_marozau/*"
        hadoop fs -rm -r -f /input_data/viachaslau_marozau/
fi
echo "HDFS create dir: /input_data/viachaslau_marozau/*"
hdfs dfs -mkdir /input_data/viachaslau_marozau/
# remove old result if exists
hdfs dfs -test -d /output_data/viachaslau_marozau/
if [ $? -eq 0 ];
    then
        echo "HDFS remove dir: /output_data/viachaslau_marozau/*"
        hadoop fs -rm -r -f /output_data/viachaslau_marozau/
fi
hdfs dfs -copyFromLocal -f input_data/viachaslau_marozau /input_data/viachaslau_marozau
# run mapreduce job
echo "Run mapreduce job:"
yarn jar "$JOB_JAR" "$JOB_CLASS" "$INPUT" "$OUTPUT"
# display result if exists
hdfs dfs -test -f /output_data/viachaslau_marozau/output/*
if [ $? -eq 0 ];
    then
        echo "Mapreduce result:"
        hdfs dfs -cat /output_data/viachaslau_marozau/output/*
    else
        echo "Result file not found"
fi