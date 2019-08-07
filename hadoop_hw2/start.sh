#!/bin/bash
# export PATH=/opt/apache-maven-3.5.0/bin:$PATH
mvn package
# Set parameters
JOB_JAR="target/hadoop_hw2-1.0-RELEASE-jar-with-dependencies.jar"
JOB_CLASS="BytesCalculationJobRunner"
INPUT="/input_data/viachaslau_marozau/*"
OUTPUT="/output_data/viachaslau_marozau/output"
OUTPUT_FORMAT="TEXT"
if [ ! -z "$1" ];
    then
        OUTPUT_FORMAT=$1
fi
# create dir if not exists
hdfs dfs -test -d /input_data/viachaslau_marozau/
if [ $? -eq 0 ];
    then
        echo "HDFS remove dir: /input_data/viachaslau_marozau/*"
        hdfs dfs -rm -r -f /input_data/viachaslau_marozau/
fi
echo "HDFS create dir: /input_data/viachaslau_marozau/*"
hdfs dfs -mkdir /input_data/viachaslau_marozau/
# remove old result if exists
hdfs dfs -test -d /output_data/viachaslau_marozau/
if [ $? -eq 0 ];
    then
        echo "HDFS remove dir: /output_data/viachaslau_marozau/*"
        hdfs dfs -rm -r -f /output_data/viachaslau_marozau/
fi
hdfs dfs -copyFromLocal -f input_data/viachaslau_marozau /input_data/viachaslau_marozau
# run mapreduce job
echo "Run mapreduce job:"
yarn jar $JOB_JAR $JOB_CLASS $INPUT $OUTPUT $OUTPUT_FORMAT
# display result if exists
hdfs dfs -test -f /output_data/viachaslau_marozau/output/*
if [ $? -eq 0 ];
    then
        echo "Mapreduce result:"
        # hadoop fs -cat /output_data/viachaslau_marozau/output/*
        hdfs dfs -libjars $JOB_JAR -text $OUTPUT/*
    else
        echo "Result file not found"
fi