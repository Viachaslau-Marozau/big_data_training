Expected, that already done:
    1. Installed and configured Java version: 1.8.0_141, vendor: Oracle Corporation
    2. Installed and configured Apache Maven 3.5.2

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/spark-sql/homework1):

    Some time has passed since we have implemented the application for Motels.home. Meanwhile the guys at Databricks
    were busy and they came out with SparkSQL. The management of Motels.home are aware of new technologies, and they
    realized that it is mandatory to update the application because of the benefits that SparkSQL provides.

    This time the tasks is to update the application and use SparkSQL instead of SparkCore API while leaving the business
    logic intact. Again you have to replace the ??? with your implementation.

    Requirements
        1. work with DataFrames - don't use the old RDD API
        2. using DataFrame DSL is strongly recommended, although you can use SQL statements if you get stuck somewhere

Task complete criteria:
    1. System/IDE agnostic build scripts should are used to compile and run project
        Done
    2. Unit tests are provided
        Done
    3. Provided implementation meets all technical requirements (task specific)
        Done

Preparation steps:
    1. Create new folder in order to download home task (mkdir demo)
    2. Go to folder (cd demo/)
    3. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    4. Open spark_hw1 folder (cd big_data_training/spark_hw2/)
    5. Apply run sh script (chmod +x start.sh)

Run steps:
    1. Run start.sh script (./start.sh)
    2. Select copy data to HDFS action
    3. Select build jar action
    4. Select execute spark task action

Expected result:
    Results of execution will be saved to /output_data/viachaslau_marozau/aggregated and /output_data/viachaslau_marozau/erroneous

Attached:
        1. Screenshot of successfully executed tests (/spark_hw2/img/)
        2. Screenshots of successfully executed local job (result files and configuration) (/spark_hw2/img/)
        3. Screenshots of successfully executed spark job (result files, task and jobs information) (/spark_hw2/img/)
        4. Result files (aggregated, erroneous and log file) of local execution