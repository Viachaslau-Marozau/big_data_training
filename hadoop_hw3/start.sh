#!/bin/bash
# export PATH=/opt/apache-maven-3.5.0/bin:$PATH
mvn package
# Set parameters
JOB_JAR="target/hadoop_hw3-1.0-RELEASE-jar-with-dependencies.jar"
JOB_CLASS="EventNumberCalculationJobRunner"
INPUT="/input_data/viachaslau_marozau/imp*.txt"
OUTPUT="/output_data/viachaslau_marozau/output"

# create dir if not exists
hdfs dfs -test -d /input_data/viachaslau_marozau/
if [ $? -ne 0 ];
    then
        echo "HDFS check ERROR: /input_data/viachaslau_marozau/* not exists"
fi

hdfs dfs -test -d /cache/viachaslau_marozau/
if [ $? -ne 0 ];
    then
        echo "HDFS check ERROR: /cache/viachaslau_marozau/* not exists"
fi
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
yarn jar $JOB_JAR $JOB_CLASS $INPUT $OUTPUT
# display result if exists
hdfs dfs -test -f /output_data/viachaslau_marozau/output/*
if [ $? -eq 0 ];
    then
        echo "Mapreduce result:"
        # hdfs dfs -cat /output_data/viachaslau_marozau/output/*
        # hdfs dfs -libjars $JOB_JAR -text $OUTPUT/*
        hdfs dfs -ls $OUTPUT
    else
        echo "Result file not found"
fi