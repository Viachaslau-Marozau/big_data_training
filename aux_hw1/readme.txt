Expected, that already done:
    For Kafka task:
        1. Installed and configured Java version: 1.8.0_141, vendor: Oracle Corporation
        2. Installed and configured Apache Maven 3.5.0
    For sqoop task:
        1. All data files for processing already uploaded to dir /input_data/viachaslau_marozau/weather/
        2. MySQL server installed and ran

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/aux_part_1/homework1):
    1. Kafka:
        1.1 Create a topic in Kafka from command line.
        1.2 Write a producer which sends the first n Fibonacci numbers through Kafka (the n number can be given as a command line argument).
        1.3 Write a consumer which reads the numbers from Kafka, calculates the sum of them and writes it to the standard output for every m-th input (the m number can be given as a command line argument). 
    2. Sqoop:
        2.1 Upload the weather data into your HDP sandbox's HDFS
        2.2 Use sqoop to export all the data to MySQL (the username/password for MySQL on the VM is root/hadoop).
        2.3 Include a screenshot in your report with the result of the following queries run in MySQL:
            SELECT count(*) FROM weather;
            SELECT * FROM weather ORDER BY stationid, date LIMIT 10;
    3. Flume
        3.1 Upload linux_messages_3000lines.txt from here
        3.2 Use the following command to create and gradually grow the input:
            shell cat linux_messages_3000lines.txt | while read line ; do echo "$line" ; sleep 0.2 ; done > output.txt
        3.3 Write a Flume configuration to upload the ever growing output.txt file to HDFS.
        3.4 Include a screenshot in your report of the output of following command of your output in HDFS:
            shell hdfs dfs cat ...

Task complete criteria:
    1. System/IDE agnostic build scripts should are used to compile and run project
    Done
    2. Unit tests are provided
    Done
    3. Provided implementation meets all technical requirements (task specific)
    Done

Preparation steps:
    1. Upload files for processing to HDFS /input_data/viachaslau_marozau/
    2. Create new folder in order to download home task (mkdir demo)
    3. Go to folder (cd demo/)
    4. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    5. Open hadoop_hw3 folder (cd big_data_training/hadoop_hw2/)
    6. Apply run start sh script (chmod +x start.sh)
    or
    7. Apply run flume sh script (chmod +x flume.sh)
    8. Apply run kafka sh script (chmod +x kafka.sh)
    9. Apply run sqoop sh script (chmod +x sqoop.sh)

Run steps:
    1. Run start.sh script and enjoy (./start.sh)
    Flume:
        1. Select run flume agent to tail output.txt action
        2. Open second terminal
        3. Select run read file per line action at second terminal
        4. Select check result action
    Kafka:
        1. Select build jar action
        2. Select start kafka servers action
        3. Select create topic action
        4. Select run producer action
        5. Select run consumer action
    Sqoop:
        1. Select drop mysql database action
        2. Select create mysql database action
        3. Select export data to MySQL action
        4. Select run task1 action
        5. Select run task2 action

Attached:
    1. Screenshots of successfully executed Flume task (/aux_hw1/img/flume)
	2. Screenshots of successfully executed Kafka task (/aux_hw1/img/kafka)
	3. Screenshots of successfully executed Sqoop task (/aux_hw1/img/sqoop)
    4. flume log file (output_data/viachaslau_marozau/)

Question:
    Add commet why do we chose default number of mapper or set only one
Answer:
    Best practice are set number of mappers equals number of processors (in our case 4)
    If you set more mappers it won't change time for execution.
