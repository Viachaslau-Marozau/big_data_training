#!/bin/bash

echo "Demo project aux_hw1"
echo "Please, select task:"
echo "1. flume"
echo "2. kafka"
echo "3. sqoop"

read item
case "$item" in
    1) echo "Selected flume task"
       chmod +x flume.sh
       ./flume.sh
       ;;
    2) echo "Selected kafka task"
       chmod +x kafka.sh
       ./kafka.sh
       ;;
    3) echo "Selected sqoop task"
       chmod +x sqoop.sh
       ./sqoop.sh
       ;;
    *) echo "Nothing selected..."
       ;;
esac