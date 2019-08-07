#!/bin/bash

CONNECTION="jdbc:mysql://localhost/weatherdb"
USERNAME="root"
PASSWORD="hadoop"
TABLE="weather"
NUM_OF_MAPPERS=4
EXPORT_DIR="/input_data/viachaslau_marozau/weather/"
STAGING_TABLE="weather_tmp"

echo "Demo project sqoop"
echo "Please, select action:"
echo "1. run task1"
echo "2. run task2"
echo "3. create mysql database (with tables)"
echo "4. export data to MySQL"
echo "5. drop mysql database (with tables)"


read item
case "$item" in
    1) echo "Selected run task1 action:"
       printf "[client]\nuser = root\npassword = hadoop">~/.my.cnf
       mysql weatherdb < resources/mysql/aux_hw1_task1.sql
       ;;
    2) echo "Selected run task2 action:"
       printf "[client]\nuser = root\npassword = hadoop">~/.my.cnf
       mysql weatherdb < resources/mysql/aux_hw1_task2.sql
       ;;
    3) echo "Selected create mysql database action:"
       printf "[client]\nuser = root\npassword = hadoop">~/.my.cnf
       mysql < resources/mysql/aux_hw1_create_all.sql
       ;;
    4) echo "Selected export data to MySQL action:"
       echo "Enter mappers count (recomended 4)"
       read number

       sqoop export --connect $CONNECTION --username $USERNAME --password $PASSWORD --table $TABLE -m $number --export-dir $EXPORT_DIR --staging-table weather_tmp
       ;;
    5) echo "Selected drop mysql database action:"
       printf "[client]\nuser = root\npassword = hadoop">~/.my.cnf
       mysql weatherdb < resources/mysql/aux_hw1_drop_all.sql
       ;;
    *) echo "Nothing selected..."
       ;;
esac