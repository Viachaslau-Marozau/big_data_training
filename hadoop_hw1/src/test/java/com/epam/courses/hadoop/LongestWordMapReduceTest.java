package com.epam.courses.hadoop;

import com.epam.courses.hadoop.mapper.LongestWordMapper;
import com.epam.courses.hadoop.reducer.LongestWordReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LongestWordMapReduceTest
{
    MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;

    @Before
    public void setUp()
    {
        LongestWordMapper mapper = new LongestWordMapper();
        LongestWordReducer reducer = new LongestWordReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException
    {
        final String testString = "Light of the Sun hasn't touched these waters for a million years";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString));
        mapReduceDriver.withOutput(new IntWritable(7), new Text("million"));
        mapReduceDriver.withOutput(new IntWritable(7), new Text("touched"));
        mapReduceDriver.runTest();
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
    public void testMapReduceWithTwoInputs() throws IOException
    {
        final String testString1 = "Light of the Sun hasn't touched these waters for a million years. (decompression)";
        final String testString2 = "He runs his eyes across the control board. Even that seems excessive; climate control and indive entertainment take up a good half of the panel. Bored, he picks one of the headset feeds at random and taps in, sending the signal to a window on his main display.";

        mapReduceDriver.withInput(new LongWritable(), new Text(testString1));
        mapReduceDriver.withInput(new LongWritable(), new Text(testString2));

        mapReduceDriver.withOutput(new IntWritable(13), new Text("decompression"));
        mapReduceDriver.withOutput(new IntWritable(13), new Text("entertainment"));
        mapReduceDriver.runTest();
    }
}
