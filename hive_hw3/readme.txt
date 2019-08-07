Expected, that already done:
    1. Installed and configured Java version: 1.8.0_141, vendor: Oracle Corporation
    2. Installed and configured Apache Maven 3.5.0
    3. All data files for mapreduce job already uploaded to dirs
       city.en.txt to /input_data/viachaslau_marozau/cities/
       imp.*.txt to /input_data/viachaslau_marozau/data/


Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/hive-basic/homework2):
    1. Write custom UDF which can parse any user agent (UA) string into separate fields
    2. Use data from you UDF and find most popular device, browser, OS for each city.

Task complete criteria:
    1. Automated system agnostic parameterized preparation and execution scripts
    2. Provided implementation meets all technical requirements (task specific)
    3. Unit tests are provided

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