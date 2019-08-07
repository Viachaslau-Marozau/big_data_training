#!/bin/bash
OUTPUT="/output_data/viachaslau_marozau"

hdfs dfs -test -d $OUTPUT
if [ $? -ne 0 ]; then
    hdfs dfs -rm -r -f $OUTPUT
fi
hdfs dfs -mkdir -p $OUTPUT

echo "Demo project flume"
echo "Please, select action:"
echo "1. run flume agent to tail output.txt"
echo "2. run read file per line"
echo "3. check result"


read item
case "$item" in
    1) echo "Selected run flume agent to tail output.txt action:"
       rm -f output.txt
       touch output.txt
       flume-ng agent --conf-file resources/flume/flume.properties --name a1 -Dflume.root.logger=INFO,console
       ;;
    2) echo "Selected run read file per line action:"
       bash -c '/bin/cat input_data/viachaslau_marozau/flume/linux_messages_3000lines.txt | while read line ; do echo "$line" ; sleep 0.02 ; done > output.txt'
       ;;
    3) echo "Selected check result action:"
       hdfs dfs -cat /output_data/viachaslau_marozau/*messages-output*
       ;;
    *) echo "Nothing selected..."
       ;;
esac