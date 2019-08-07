Expected, that already done:
    1. Installed and configured Java version: 1.8.0_141, vendor: Oracle Corporation
    2. Installed and configured Apache Maven 3.5.0
    3. All data files for mapreduce job already uploaded to dir /input_data/viachaslau_marozau/
    4. Distributed cache files uploaded to dir /cache/viachaslau_marozau/

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/hadoop-basic/homework4):
    1. Unzip data files and put them in HDFS system and make screenshot #1 of HDFS files:
    2. Calculate amount of high-bid-priced  (more than 250) impression events by city ()
    3. Print result where each city presented with its name rather than id
    4. Make screenshot of execution #2 and results #3 from HDFS
    5. Add Custom Partitioner by OperationSystemType
    6. Run with several reducers and make screenshot of successful job #4 and results from HDFS #5

Task complete criteria:
    1. IDE agnostic build (Maven, Ant, Gradle, sbt, etc)
    Done
    2. Unit tests are provided:
        - unit test for mapper
        - unit test for reducer
        - unit test for MapReduce
    Done
    3. Code is well-documented
    Done, see readme.txt file
    4. Working MapReduce application
    Done
    5. Disributed cache is used
    Done
    6. Combiner was used to reduce network congestion
    Done
    7. CustomType used thought whole phases
    Done
    8. Custom partitioner is used
    No needs, already realized method compareTo (implements WritableComparable interface)
    9. Writable entities are reused
	Done

Preparation steps:
    1. Upload files for processing to HDFS /input_data/viachaslau_marozau/
    2. Create new folder in order to download home task (mkdir demo)
    3. Go to folder (cd demo/)
    4. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    5. Open hadoop_hw3 folder (cd big_data_training/hadoop_hw2/)
    6. Apply run sh script (chmod +x start.sh)

Run steps:
    1. Run start.sh script (./start.sh)

Expected result:
    At console will be displayed list of result files (one file for each OS):

    Mapreduce result:
    Found 26 items
    -rw-r--r--   1 root hdfs          0 2017-09-17 20:08 /output_data/viachaslau_marozau/output/_SUCCESS
    -rw-r--r--   1 root hdfs       5013 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00000
    -rw-r--r--   1 root hdfs        101 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00001
    -rw-r--r--   1 root hdfs       4763 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00002
    -rw-r--r--   1 root hdfs       1210 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00003
    -rw-r--r--   1 root hdfs       2119 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00004
    -rw-r--r--   1 root hdfs        127 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00005
    -rw-r--r--   1 root hdfs       2858 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00006
    -rw-r--r--   1 root hdfs       3943 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00007
    -rw-r--r--   1 root hdfs       4617 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00008
    -rw-r--r--   1 root hdfs       3045 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00009
    -rw-r--r--   1 root hdfs         61 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00010
    -rw-r--r--   1 root hdfs         11 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00011
    -rw-r--r--   1 root hdfs        184 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00012
    -rw-r--r--   1 root hdfs         54 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00013
    -rw-r--r--   1 root hdfs         79 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00014
    -rw-r--r--   1 root hdfs         12 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00015
    -rw-r--r--   1 root hdfs       1975 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00016
    -rw-r--r--   1 root hdfs       1520 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00017
    -rw-r--r--   1 root hdfs       5202 2017-09-17 20:07 /output_data/viachaslau_marozau/output/part-r-00018
    -rw-r--r--   1 root hdfs        183 2017-09-17 20:08 /output_data/viachaslau_marozau/output/part-r-00019
    -rw-r--r--   1 root hdfs        822 2017-09-17 20:08 /output_data/viachaslau_marozau/output/part-r-00020
    -rw-r--r--   1 root hdfs       4712 2017-09-17 20:08 /output_data/viachaslau_marozau/output/part-r-00021
    -rw-r--r--   1 root hdfs       5410 2017-09-17 20:08 /output_data/viachaslau_marozau/output/part-r-00022
    -rw-r--r--   1 root hdfs        740 2017-09-17 20:08 /output_data/viachaslau_marozau/output/part-r-00023
    -rw-r--r--   1 root hdfs       3260 2017-09-17 20:08 /output_data/viachaslau_marozau/output/part-r-00024


Attached:
    1. Screenshots of data files in HDFS system (/hadoop_hw3/img/)
	2. Screenshot of successfully executed tests (/hadoop_hw3/img/)
	3. Screenshots of successfully executed job and results without partitioner (/hadoop_hw3/img/)
	4. Screenshots of successfully executed job and results with partitioner (/hadoop_hw3/img/)