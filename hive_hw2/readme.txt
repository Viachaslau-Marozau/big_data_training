Expected, that already done:
    1. All data files for mapreduce job already uploaded to dirs
       airports.cvs to /input_data/viachaslau_marozau/airports/
       carriers.cvs to /input_data/viachaslau_marozau/carriers/
       flights.cvs to /input_data/viachaslau_marozau/flights/

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/hive-basic/homework2):
    1. Find all carriers who cancelled more than 1 flights during 2007, order them from biggest to lowest by number
       of cancelled flights and list in each record all departure cities where cancellation happened. (Screenshot #1)
    2. How many MR jobs where instanced for this query?

    As I can see at attached log file was run 2 MR job instances:
    one for temporary table and one for select results.

Task complete criteria:
    1. Automated system agnostic parameterized preparation and execution  scripts
    Done
    2. Provided implementation meets all technical requirements (task specific)
    Done

Preparation steps:
    1. Upload files for processing to HDFS /input_data/viachaslau_marozau/
    2. Create new folder in order to download home task (mkdir demo)
    3. Go to folder (cd demo/)
    4. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    5. Open hive_hw2 folder (cd big_data_training/hive_hw2/)
    6. Apply run sh script (chmod +x start.sh)

Run steps:
    1. Run start.sh script (./start.sh)

Expected result:
    At console will be displayed list of engines and possible actions, you can check and execute it.

Attached:
    1. Screenshots of successfully executed 1 task at ambari and console (mr and tez engines) (/hive_hw2/img/)
    2. mr job log file(/hive_hw2/output_data/viachaslau_marozau/log/)