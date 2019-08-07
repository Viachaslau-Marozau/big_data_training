package com.epam.courses.hadoop;

import com.epam.courses.hadoop.combiner.BytesCalculationCombiner;
import com.epam.courses.hadoop.mapper.BytesCalculationMapper;
import com.epam.courses.hadoop.reducer.BytesCalculationReducer;
import com.epam.courses.hadoop.type.ByteCountWritable;
import com.epam.courses.hadoop.type.ByteStatisticWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class BytesCalculationMapReduceTest
{
    MapReduceDriver<LongWritable, Text, Text, ByteCountWritable, Text, ByteStatisticWritable> mapReduceDriver;

    @Before
    public void setUp()
    {
        BytesCalculationMapper mapper = new BytesCalculationMapper();
        BytesCalculationReducer reducer = new BytesCalculationReducer();
        BytesCalculationCombiner combiner = new BytesCalculationCombiner();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
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
    public void testMapReduceOneIP() throws IOException
    {
        final String testString1 = "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 512000 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString2 = "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 256000 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString1));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));

        mapReduceDriver.withOutput(new Text("ip13"), new ByteStatisticWritable(384000, 768000));
        mapReduceDriver.runTest();
    }

    @Test
    public void testMapReduceSomeIP() throws IOException
    {
        final String testString1 = "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 512000 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString2 = "ip11 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 128000 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString3 = "ip11 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 128000 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString4 = "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 384000 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString5 = "ip11 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 512000 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString6 = "ip103 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 10002 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString1));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString3));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString4));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString5));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString6));

        mapReduceDriver.withOutput(new Text("ip103"), new ByteStatisticWritable(10002, 10002));
        mapReduceDriver.withOutput(new Text("ip11"), new ByteStatisticWritable(256000, 768000));
        mapReduceDriver.withOutput(new Text("ip13"), new ByteStatisticWritable(448000, 896000));
        mapReduceDriver.runTest();
    }
}
