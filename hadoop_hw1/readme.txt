Expected, that already installed and configured next software:
    1. Java version: 1.8.0_141, vendor: Oracle Corporation
    2. Apache Maven 3.5.0

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/hadoop-basic/homework1):
    1. Write MapReduce program which takes text file as an input, searches the longest word and writes
    this word to the output.
    2. Think and discuss with mentor how to improve performance of this program and apply it for
    multiple files.

Task complete criteria:
    1. IDE agnostic build (Maven, Ant, Gradle, sbt, etc)
    Done
    2. Unit tests are provided:
        - unit test for mapper
        - unit test for reducer
        - unit test for MapReduce
    Done
    3. Code is well-documented
    4. Working MapReduce application to find the longest words in the document set
    Done
    5. Combiner was used to reduce network congestion
    Done
    6. Preliminary filtering was applied on the map stage
    Done
    7. Proposed solution allows detecting few 'longest' words. Corresponding Unit test is attached
    Done
    8. Writable entities are reused
    Done

Preparation steps:
    1. Create new folder in order to download home task (mkdir demo)
    2. Go to folder (cd demo/)
    2. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    3. Open hadoop_hw1 folder (cd big_data_training/hadoop_hw1/)
    4. Apply run sh script (chmod +x start.sh)

Run steps:
    1. Run start.sh script (./start.sh)

Expected result:
    At console will be displayed:

    17      floor-to-ceilling
    17      forty-five-degree

Attached:
    1. Screenshot of successfully executed tests (/hadoop_hw1/img/)
    2. Input test text files (/hadoop_hw1/input_data/)
    3. Output file (/hadoop_hw1/output_data/)
    4. Saved console logs (/hadoop_hw1/output_data/log/)

 