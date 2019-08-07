#!/bin/bash

echo "Demo project hive_hw2"
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

CONNECTION_hive_hw2_DB="-u jdbc:hive2://localhost:10000/hive_hw2 -n root -p root -hiveconf hive.execution.engine="$ENGINE

echo "Please, select action:"
echo "1. run task 1 (note: you must load data before)"
echo "2. run task 1 explanation (note: you must load data before)"
echo "3. create database"
echo "4. create tables (note: you must create database before)"
echo "5. load data into tables (note: you must create tables before)"
echo "6. drop tables"
echo "7. run all"

read item
case "$item" in
    1) echo "Selected run task 1 action"
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_task1.hql";
       ;;
    2) echo "Selected run task 1 explanation action"
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_task1_explanation.hql";
       ;;
    3) echo "Selected create database action"
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_create_database.hql";
       ;;
    4) echo "Selected create tables action"
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_create_tables.hql";
       ;;
    5) echo "Selected load data into tables action"
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_load_data_into_tables.hql";
       ;;
    6) echo "Selected drop tables action"
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_drop_tables.hql";
       ;;
    7) echo "Selected run all action"
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_create_database.hql";
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_create_tables.hql";
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_load_data_into_tables.hql";
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_task1_explanation.hql";
       beeline $CONNECTION_hive_hw2_DB -f "src/main/resources/hive/hive_hw2_task1.hql";
      ;;
    *) echo "Nothing selected..."
       ;;
esac