package com.epam.courses.hadoop.mapper;

import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class EventNumberCalculationMapperTest
{
    MapDriver<LongWritable, Text, ComplexKeyWritable, IntWritable> mapDriver;

    @Before
    public void setUp()
    {
        EventNumberCalculationMapper mapper = new EventNumberCalculationMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testCorrectDataMapped() throws IOException
    {
        final String testString = "aefce280367ac3603bd0d33c10a88d\t20131027120600798\t1\tDAEHW679yP_\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; 8UEFlag)\t10.237.92.*\t0\t23\t1\t16076f675c38d669d66869a5dd41aba8\t40b3780ac1b3ae695c84f02668e8b794\tnull\tmm_10989163_978818_8968536\t620\t60\tFirstView\tNa\t0\t12625\t294\t13\tnull\t2261\tnull";
        mapDriver.withInput(new LongWritable(0), new Text(testString));
        mapDriver.withOutput(new ComplexKeyWritable(23, "Windows XP"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testIncorrectString() throws IOException
    {
        final String testString = "fb3652d800d44c24f0148e895273752b	20130311175505312	1	2ef61dd4551958fd715d0e6ef326c514	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; æ°ççäº§å»ºè®¾å";

        mapDriver.withInput(new LongWritable(0), new Text(testString));
        mapDriver.runTest();
    }

    @Test
    public void testEmptyString() throws IOException
    {
        mapDriver.withInput(new LongWritable(), new Text());
        mapDriver.runTest();
    }

    @Test
    public void testBiddingAmountLessThan250() throws IOException
    {
        final String testString = "a8ad3416f3ca7089c4163536e528dbd7	20131020161805462	1	DAGMiP9asOX	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.102.173.*	216	219	3	7902f785316cb076166c688b538d62fa	2f6b8f067c966d489b364b230c822578	null	PD_PL_F_Rectangle2	300	250	Na	Na	50	7323	249	260	null	2259	10063";

        mapDriver.withInput(new LongWritable(0), new Text(testString));
        mapDriver.runTest();
    }

    @Test
    public void testSomeLinesMapped() throws IOException
    {
        final String testString1 = "b0d4a724b340b9c846bbcc63447b83c5	20131020165400567	1	DAGM51Emv2i	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	123.88.179.*	216	233	2	13625cb070ffb306b425cd803c4b7ab4	5d3329560b19d27582dbe370bffa8e63	null	1105259604	160	600	OtherView	Na	148	7317	277	148	null	2259	10083";
        final String testString2 = "c73ffdd1ffd688e259f6a1aa37cb291b	20131020113731332	1	DAGMarCWsir	Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.116.37.*	216	219	1	5777beaf611cab0f34afde6b9c3668c1	c360b8d268030c88322939ad45aeacde	null	mm_17936709_2306571_9246546	300	250	ThirdView	Na	0	7323	294	13	null	2259	10114,10063";
        final String testString3 = "a8ad3416f3ca7089c4163536e528dbd7	20131020161805462	1	DAGMiP9asOX	Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1	113.102.173.*	216	219	3	7902f785316cb076166c688b538d62fa	2f6b8f067c966d489b364b230c822578	null	PD_PL_F_Rectangle2	300	250	Na	Na	50	7323	294	260	null	2259	10063";

        mapDriver.withInput(new LongWritable(0), new Text(testString1));
        mapDriver.withInput(new LongWritable(0), new Text(testString2));
        mapDriver.withInput(new LongWritable(0), new Text(testString3));

        mapDriver.withOutput(new ComplexKeyWritable(233, "Windows XP"), new IntWritable(1));
        mapDriver.withOutput(new ComplexKeyWritable(219, "Windows XP"), new IntWritable(1));
        mapDriver.withOutput(new ComplexKeyWritable(219, "Windows 7"), new IntWritable(1));
        mapDriver.runTest();
    }
}
