#!/bin/bash

export PATH=/opt/apache-maven-3.5.0/bin:$PATH

echo "Demo project hive_hw3"
echo "Please, select engine for connecting to hive:"
echo "1. mr"
echo "2. tez"

ENGINE="tez"

read item
case "$item" in
    1) echo "Selected mr engine"
       ENGINE="mr"
       ;;
    2) echo "Selected tez engine"
       ;;
    *) echo "Nothing selected... Will be used tez engine by default"
       ;;
esac

CONNECTION_hive_hw3_DB="-u jdbc:hive2://localhost:10000/hive_hw3 -n root -p root -hiveconf hive.execution.engine="$ENGINE

echo "Please, select action:"
echo "1. run task 1 (note: you must load data and build jar before)"
echo "2. build jar"
echo "3. create database"
echo "4. create tables (note: you must create database before)"
echo "5. load data into tables (note: you must create tables before)"
echo "6. drop tables"
echo "7. run all"

read item
case "$item" in
    1) echo "Selected run task 1 explanation action"
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_task1.hql";
       ;;
    2) echo "Selected build jar action"
       mvn package
       hadoop fs -test -d /input_data
       if [ $? != 0 ]; then
           hadoop fs -mkdir -p /input_data/viachaslau_marozau/
       fi
       hadoop fs -copyFromLocal -f target/hive_hw3-jar-with-dependencies.jar /input_data/viachaslau_marozau/
       ;;
    3) echo "Selected create database action"
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_create_database.hql";
       ;;
    4) echo "Selected create tables action"
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_create_tables.hql";
       ;;
    5) echo "Selected load data into tables action"
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_load_data_into_tables.hql";
       ;;
    6) echo "Selected drop tables action"
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_drop_tables.hql";
       ;;
    7) echo "Selected run all action"
       mvn package
       hadoop fs -test -d /input_data
       if [ $? != 0 ]; then
           hadoop fs -mkdir -p /input_data/viachaslau_marozau/
       fi
       hadoop fs -copyFromLocal -f target/hive_hw3-jar-with-dependencies.jar /input_data/viachaslau_marozau/
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_create_database.hql";
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_create_tables.hql";
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_load_data_into_tables.hql";
       beeline $CONNECTION_hive_hw3_DB -f "resources/hive/hive_hw3_task1.hql";
      ;;
    *) echo "Nothing selected..."
       ;;
esac