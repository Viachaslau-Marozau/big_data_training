package com.epam.courses.hadoop;

import com.epam.courses.hadoop.combiner.EventNumberCalculationCombiner;
import com.epam.courses.hadoop.mapper.EventNumberCalculationMapper;
import com.epam.courses.hadoop.reducer.EventNumberCalculationReducer;
import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class BytesCalculationMapReduceTest
{
    MapReduceDriver<LongWritable, Text, ComplexKeyWritable, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp()
    {
        EventNumberCalculationMapper mapper = new EventNumberCalculationMapper();
        EventNumberCalculationReducer reducer = new EventNumberCalculationReducer();
        EventNumberCalculationCombiner combiner = new EventNumberCalculationCombiner();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
        URI cacheFileURI = new File("input_data/viachaslau_marozau/cache/city.en.txt").toURI();
        mapReduceDriver.setCacheFiles(new URI[] {cacheFileURI});
    }

    @Test
    public void testEmptyMapReduce() throws IOException
    {
        final String testString = "";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString));
        mapReduceDriver.runTest();
    }

    @Test(expected = NullPointerException.class)
    public void testNullMapReduce() throws IOException
    {
        mapReduceDriver.withInput(new LongWritable(), null);
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceOneOS() throws IOException
    {
        final String testString1 = "b0d4a724b340b9c846bbcc63447b83c5	20131020165400567	1	DAGM51Emv2i	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	123.88.179.*	216	233	2	13625cb070ffb306b425cd803c4b7ab4	5d3329560b19d27582dbe370bffa8e63	null	1105259604	160	600	OtherView	Na	148	7317	277	148	null	2259	10083";
        final String testString2 = "c73ffdd1ffd688e259f6a1aa37cb291b	20131020113731332	1	DAGMarCWsir	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.116.37.*	216	233	1	5777beaf611cab0f34afde6b9c3668c1	c360b8d268030c88322939ad45aeacde	null	mm_17936709_2306571_9246546	300	250	ThirdView	Na	0	7323	294	13	null	2259	10114,10063";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString1));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));

        mapReduceDriver.withOutput(new Text("dongguan"), new IntWritable(2));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceDifferentOS() throws IOException
    {
        final String testString1 = "c73ffdd1ffd688e259f6a1aa37cb291b	20131020113731332	1	DAGMarCWsir	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.116.37.*	216	219	1	5777beaf611cab0f34afde6b9c3668c1	c360b8d268030c88322939ad45aeacde	null	mm_17936709_2306571_9246546	300	250	ThirdView	Na	0	7323	294	13	null	2259	10114,10063";
        final String testString2 = "a8ad3416f3ca7089c4163536e528dbd7	20131020161805462	1	DAGMiP9asOX	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.102.173.*	216	219	3	7902f785316cb076166c688b538d62fa	2f6b8f067c966d489b364b230c822578	null	PD_PL_F_Rectangle2	300	250	Na	Na	50	7323	294	260	null	2259	10063";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString1));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));

        mapReduceDriver.withOutput(new Text("shenzhen"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("shenzhen"), new IntWritable(1));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceSomeOS() throws IOException
    {
        final String testString1 = "c73ffdd1ffd688e259f6a1aa37cb291b	20131020113731332	1	DAGMarCWsir	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.116.37.*	216	219	1	5777beaf611cab0f34afde6b9c3668c1	c360b8d268030c88322939ad45aeacde	null	mm_17936709_2306571_9246546	300	250	ThirdView	Na	0	7323	294	13	null	2259	10114,10063";
        final String testString2 = "a8ad3416f3ca7089c4163536e528dbd7	20131020161805462	1	DAGMiP9asOX	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.102.173.*	216	219	3	7902f785316cb076166c688b538d62fa	2f6b8f067c966d489b364b230c822578	null	PD_PL_F_Rectangle2	300	250	Na	Na	50	7323	294	260	null	2259	10063";
        final String testString3 = "c73ffdd1ffd688e259f6a1aa37cb291b	20131020113731332	1	DAGMarCWsir	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.116.37.*	216	219	1	5777beaf611cab0f34afde6b9c3668c1	c360b8d268030c88322939ad45aeacde	null	mm_17936709_2306571_9246546	300	250	ThirdView	Na	0	7323	244	13	null	2259	10114,10063";
        final String testString4 = "a8ad3416f3ca7089c4163536e528dbd7	20131020161805462	1	DAGMiP9asOX	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.102.173.*	216	219	3	7902f785316cb076166c688b538d62fa	2f6b8f067c966d489b364b230c822578	null	PD_PL_F_Rectangle2	300	250	Na	Na	50	7323	294	260	null	2259	10063";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString1));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString3));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString4));

        mapReduceDriver.withOutput(new Text("shenzhen"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("shenzhen"), new IntWritable(1));

        mapReduceDriver.runTest();
    }
}
