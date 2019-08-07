Expected, that already done:
    1. All data files for mapreduce job already uploaded to dirs
       airports.cvs to /input_data/viachaslau_marozau/airports/
       carriers.cvs to /input_data/viachaslau_marozau/carriers/
       flights.cvs to /input_data/viachaslau_marozau/flights/

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/hive-basic/homework1):
    1. Put dataset on HDFS and explore it through Hive.
    2. Count total number of flights per carrier in 2007 (Screanshot#1)
    3. The total number of flights served in Jun 2007 by NYC (all airports, use join with Airports)(Screanshot#2)
    4. Find five most busy airports in US during Jun 01 - Aug 31. (Screanshot#3)
    5. Find the carrier who served the biggest number of flights (Screanshot#4)

Task complete criteria:
   1. Automated system agnostic parameterized preparation and execution  scripts
   Done
   2. Provided implementation meets all technical requirements
   Done

Preparation steps:
    1. Upload files for processing to HDFS /input_data/viachaslau_marozau/
    2. Create new folder in order to download home task (mkdir demo)
    3. Go to folder (cd demo/)
    4. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    5. Open hive_hw3 folder (cd big_data_training/hive_hw3/)
    6. Apply run sh script (chmod +x start.sh)

Run steps:
    1. Run start.sh script (./start.sh)

Expected result:
    At console will be displayed list of engines and possible actions, you can check and execute it.

Attached:
    1. Screenshots of successfully executed 1 task at ambari and console (mr and tez engines) (/hive_hw3/img/)
	2. Screenshots of successfully executed 2 task at ambari and console (mr and tez engines) (/hive_hw3/img/)
	3. Screenshots of successfully executed 3 task at ambari and console (mr and tez engines) (/hive_hw3/img/)
	4. Screenshots of successfully executed 4 task at ambari and console (mr and tez engines) (/hive_hw3/img/)