package com.epam.courses.hadoop.mapper;

import com.epam.courses.hadoop.type.ByteCountWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class BytesCalculationMapperTest
{
    MapDriver<LongWritable, Text, Text, ByteCountWritable> mapDriver;

    @Before
    public void setUp()
    {
        BytesCalculationMapper mapper = new BytesCalculationMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testCorrectDataMapped() throws IOException
    {
        final String testString = "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 4846545 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";

        mapDriver.withInput(new LongWritable(0), new Text(testString));
        mapDriver.withOutput(new Text("ip13"), new ByteCountWritable(4846545, 1));
        mapDriver.runTest();
    }

    @Test
    public void testEmptyString() throws IOException
    {
        mapDriver.withInput(new LongWritable(), new Text());
        mapDriver.runTest();
    }

    @Test
    public void testBytesNotMapped() throws IOException
    {
        final String testString = "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";

        mapDriver.withInput(new LongWritable(0), new Text(testString));
        mapDriver.withOutput(new Text("ip13"), new ByteCountWritable(0, 1));
        mapDriver.runTest();
    }

    @Test
    public void testSomeLinesMapped() throws IOException
    {
        final String testString1 = "ip13 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 4846545 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString2 = "ip12 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 - \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";
        final String testString3 = "ip11 - - [24/Apr/2011:04:41:53 -0400] \"GET /logs/access_log.3 HTTP/1.1\" 200 128 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";

        mapDriver.withInput(new LongWritable(0), new Text(testString1));
        mapDriver.withInput(new LongWritable(0), new Text(testString2));
        mapDriver.withInput(new LongWritable(0), new Text(testString3));

        mapDriver.withOutput(new Text("ip13"), new ByteCountWritable(4846545, 1));
        mapDriver.withOutput(new Text("ip12"), new ByteCountWritable(0, 1));
        mapDriver.withOutput(new Text("ip11"), new ByteCountWritable(128, 1));
        mapDriver.runTest();
    }
}
