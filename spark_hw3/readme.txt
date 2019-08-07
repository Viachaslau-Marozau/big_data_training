Expected, that already done:
    1. Installed and configured Java version: 1.8.0_141, vendor: Oracle Corporation
    2. Installed and configured Apache Maven 3.5.2

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/spark-streaming/homework2):

    Realtime Anomaly Detection

        This project is a prototype for real time anomaly detection.
        It uses Numenta's Hierarchical Temporal Memory -  a technology which emulates the work of the cortex.


    What is done
        1. Everything concerning the intellectual part - the HTM network.
        2. Spark steaming application which takes the input stream from Kafka,
        detects anomalies using online learning and outputs enriched records back into Kafka.
        3. Prototype of visualization based on zeppelin notebook.


    What is not done yet

        1. Optimise parameters and select best properties for real life run
        2.Fix maven to not include the properties into the final jar. Add exclude spark libraries from uber jar.


        Run sequence

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
    4. Open spark_hw1 folder (cd big_data_training/spark_hw3/)
    5. Apply run sh script (chmod +x start.sh)
    6. open Zeppelin
    7. import project "anomaly-detector-kafka"
    8. configure interceptor (new or recofigure spark2)
    9. make sure that you have correct memory configuration to run a project

Run steps:
    1. Run start.sh script (./start.sh)
    2. Select build jar action
    3  Select kafka -> run kafka broker build jar action
    4. Select kafka -> create topic wirh name monitoring20
    5. Select kafka -> create topic wirh name monitoringEnriched2
    6. Run Zeppelin spark2 project part
    7. Select execute spark task action
    8. Select kafka -> run producer action
    9. run Zeppelin angular task


Expected result:
    You will see processes visualization

Attached:
        1. Screenshot of successfully executed tests (/spark_hw3/img/)
        2. Screenshots of successfully executed Zeppelin task (/spark_hw3/img/)
        3. Screenshots of successfully executed spark console task (/spark_hw3/img/)
        4. spark console application log file (/spark_hw3/output_data/viachaslau_marozau/log/)