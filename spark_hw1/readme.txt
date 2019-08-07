Expected, that already done:
    1. Installed and configured Java version: 1.8.0_141, vendor: Oracle Corporation
    2. Installed and configured Apache Maven 3.5.0

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/spark-core/homework1):

    Please find the prepared skeleton in the source directory. You only have to substitute the ???-s with proper logic.
    You can also find some example input/output files in the test resource directory, you can use those to validate your logic.

    1. Erroneous records
        Nothing is perfect neither is the bidding system. From time to time something goes wrong in the bidding file
        generator in the partner's side, so it includes corrupted records in the stream.
        A record is corrupted if the value in the third column is not a number but a text with this format: ERROR_(.*)
        The task here is to filter out the corrupted records from the input and count how many occurred from the given type
        in the given hour.
        You may want to use BidError domain class. The output should be formatted as comma separated lines containing
        the date (with hour precision), the error message and the count of such messages in the given hour.
        See "expected_core" under expected_out for example.

    2. Exchange rates
        As Motels.home is Europe based it is convenient to convert all types of currencies to EUR for the business analysts.
        In our example we have only USD so we will do only USD to EUR conversion. Here you have to read the currencies into
        a map where the dates are the keys and the related conversion rates are the values. Use this data as mapping source
        to be able to exchange the provided USD value to EUR on any given date/time.

    3. Dealing with bids
        Now we can focus on the most important parts, the bids. In Task 1 you have read the original bids data.
        The first part is to get rid of the erroneous records and keep only the conforming ones which are not prefixed
        with ERROR_ strings.
        In this campaign Motel.home is focusing only on three countries: US,CA,MX so you'll have to only work with those three
        and also the records have to be transposed so one record will only contain price for one Losa.
        Somewhere in this task you have to do the following conversions/filters:
        Convert USD to EUR. The result should be rounded to 3 decimal precision.
        Convert dates to proper format - use formats in Constants util class
        Get rid of records where there is no price for a Losa or the price is not a proper decimal number


    4. Load motels
        Load motels data and prepare it for joining with bids.
        Hint: we want to enrich the bids data with motel names, so you'll probably need the motel id and motel name as well.

    5. Finally enrich the data and find the maximum
        Motels.home wants to identify rich markets so it is interested where the advertisement is the most expensive
        so we are looking for maximum values.
        Join the bids with motel names. You can use EnrichedItem domain class to represent the join record.
        As a final output we want to find and only keep the records which have the maximum prices for a given motelId/bidDate.
        When determining the maximum if the same price appears twice then keep the first object you found with the given price.

Task complete criteria:
    1. System/IDE agnostic build scripts should are used to compile and run project
        Done
    2. Additinal unit tests are provided
        Done
    3. Additinal domain classes are provided to simplify reading and support
        Done, added GroupedBidError
    4. Provided implementation meets all technical requirements and passes predefined and provided tests
        Done

Preparation steps:
    1. Create new folder in order to download home task (mkdir demo)
    2. Go to folder (cd demo/)
    3. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    4. Open spark_hw1 folder (cd big_data_training/spark_hw1/)
    5. Apply run sh script (chmod +x start.sh)

Run steps:
    1. Run start.sh script (./start.sh)
    2. Select copy data to HDFS action
    3. Select build jar action
    4. Select execute spark task action

Expected result:
    Results of execution will be saved to /output_data/viachaslau_marozau/aggregated and /output_data/viachaslau_marozau/erroneous

Attached:
        1. Screenshot of successfully executed tests (/spark_hw1/img/)
        2. Screenshots of successfully executed local job (result files and configuration) (/spark_hw1/img/)
        3. Screenshots of successfully executed spark job (result files, task and jobs information) (/spark_hw1/img/)
        4. Result files (aggregated, erroneous and log file) of local execution