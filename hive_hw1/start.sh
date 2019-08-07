#!/bin/bash

echo "Demo project hive_hw1"
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

CONNECTION_HIVE_HW1_DB="-u jdbc:hive2://localhost:10000/hive_hw1 -n root -p root -hiveconf hive.execution.engine="$ENGINE

echo "Please, select action:"
echo "1. run task 1 (note: you must load data before)"
echo "2. run task 2 (note: you must load data before)"
echo "3. run task 3 (note: you must load data before)"
echo "4. run task 4 (note: you must load data before)"
echo "5. create database"
echo "6. create tables (note: you must create database before)"
echo "7. load data into tables (note: you must create tables before)"
echo "8. drop tables"
echo "9. run all"

read item
case "$item" in
    1) echo "Selected run task 1 action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task1.hql";
       ;;
    2) echo "Selected run task 2 action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task2.hql";
       ;;
    3) echo "Selected run task 3 action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task3.hql";
       ;;
    4) echo "Selected run task 4 action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task4.hql";
       ;;

    5) echo "Selected create database action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_create_database.hql";
       ;;
    6) echo "Selected create tables action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_create_tables.hql";
       ;;
    7) echo "Selected load data into tables action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_load_data_into_tables.hql";
       ;;
    8) echo "Selected drop tables action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_drop_tables.hql";
       ;;
    9) echo "Selected run all action"
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_create_database.hql";
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_create_tables.hql";
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_load_data_into_tables.hql";
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task1.hql";
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task2.hql";
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task3.hql";
       beeline $CONNECTION_HIVE_HW1_DB -f "src/main/resources/hive/hive_hw1_task4.hql";
       ;;
    *) echo "Nothing selected..."
       ;;
esac