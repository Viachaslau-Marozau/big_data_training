Expected, that already installed and configured next software:
    1. Java version: 1.8.0_141, vendor: Oracle Corporation
    2. Apache Maven 3.5.0

Task (https://git.epam.com/epmc-bdcc/bigdata-training/tree/master/dev/homeworks/hadoop-basic/homework3):
	1. Write MR job:
		- to count average bytes per request by IP and total bytes by IP
		- try to use combiner
		- output is CSV file with rows as next:
          IP,175.5,109854
	2. Add MR Unit tests for your Mapper/Reducer
	3. Modify previous MR job to use custom Writable data type
	4. Save output as Sequence file compressed with Snappy (key is IP, and value is custom object for avg and total size)
	5. Use counters to get stats how many users of IE, Mozzila or other were detected and print them in STDOUT of Driver and make screenshot #2
	6. Read content of compressed file from console using command line (screenshot #3):

Task complete criteria:
	1. IDE agnostic build (Maven, Ant, Gradle, sbt, etc)
	Done
	2. Unit tests are provided: 	 
		- unit test for mapper
		- unit test for reducer
		- unit test for MapReduce
	Done
	3. Code is well-documented
	Done (see readme.txt)
	4. Working MapReduce application
	Done
	5. Counters is used
	Done
	6. Combiner was used to reduce network congestion
	Done
	7. Output is a SequenceFile
	Done (configured by script parameters)
	8. Provided solutions for type/output format/compression:
		- TEXT/CSV/none
		- CustomType/SequenceFile/Snappy
	Done
	9. Writable entities are reused
	Done
	
Preparation steps:
    1. Create new folder in order to download home task (mkdir demo)
    2. Go to folder (cd demo/)
    2. Clone  repository (git clone https://Viachaslau_Marozau@bitbucket.org/Viachaslau_Marozau/big_data_training.git)
       (or copy and unpack provided archive file)
    3. Open hadoop_hw2 folder (cd big_data_training/hadoop_hw2/)
    4. Apply run sh script (chmod +x start.sh)

Run steps:
    1. Run start.sh script (./start.sh)
	Note: You can provide output format as script parameter (./start.sh snappy)

Expected result:
    At console will be displayed:
	
		Found browsers:

	Apple Mail: 263
	CFNetwork: 1
	Chrome: 177
	Chrome 10: 1323
	Chrome 8: 10
	Chrome 9: 3
	Downloading Tool: 5
	Firefox 1.5: 1
	Firefox 2: 29
	Firefox 3: 2841
	Firefox 4: 2618
	Internet Explorer: 12
	Internet Explorer 5: 1
	Internet Explorer 5.5: 10
	Internet Explorer 6: 195
	Internet Explorer 7: 374
	Internet Explorer 8: 1277
	Internet Explorer 9: 198
	Konqueror: 11
	Mobile Safari: 228
	Mozilla: 77
	Opera: 15
	Opera 10: 217
	Opera 9: 7
	Opera Mini: 24
	Outlook 2007: 2
	Robot/Spider: 2504
	Safari: 84
	Safari 4: 67
	Safari 5: 618
	SeaMonkey: 63
	Unknown: 246

	
	Mapreduce result:
	ip1,25819.018691588786,2762635
	ip2,30824.0,339064
	ip3,72209.0,72209
	...
	ip1614,72090.0,72090
	ip1615,24425.125,390802

Attached:
	1. Screenshot of successfully executed tests (/hadoop_hw2/img/)
	2. Screenshot #1 of successfully executed job from point 1 (/hadoop_hw2/img/)
	3. Screenshot #2 of usage counters  (/hadoop_hw2/img/)
	4. Screenshot #3 of reading compressed content (/hadoop_hw2/img/) 