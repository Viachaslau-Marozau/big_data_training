package com.epam.courses.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LongestWordMapperTest
{
    MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;

    @Before
    public void setUp()
    {
        LongestWordMapper mapper = new LongestWordMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testFindWongestLords() throws IOException
    {
        final String testString = "Light of the Sun hasn't touched these waters for a million years";

        mapDriver.withInput(new LongWritable(), new Text(testString));
        mapDriver.withOutput(new IntWritable(7), new Text("million"));
        mapDriver.withOutput(new IntWritable(7), new Text("touched"));


        mapDriver.runTest();
    }

    @Test
    public void testEmptyString() throws IOException
    {
        mapDriver.withInput(new LongWritable(), new Text());
        mapDriver.runTest();
    }

    @Test
    public void testFindWongestLordsWithNumbers() throws IOException
    {
        final String testString = "Sunlight hasn't touched these waters for a 100000000 years";

        mapDriver.withInput(new LongWritable(), new Text(testString));
        mapDriver.withOutput(new IntWritable(8), new Text("sunlight"));
        mapDriver.runTest();
    }

    @Test
    public void testFindWongestLordsWithPunctuation() throws IOException
    {
        final String testString = "Light of the Sun hasn't touched these waters for a 1000000 (million) years.";

        mapDriver.withInput(new LongWritable(), new Text(testString));
        mapDriver.withOutput(new IntWritable(7), new Text("million"));
        mapDriver.withOutput(new IntWritable(7), new Text("touched"));

        mapDriver.runTest();
    }
}
