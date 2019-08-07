package com.epam.courses.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LongestWordReducerTest
{
    ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;

    @Before
    public void setUp()
    {
        LongestWordReducer reducer = new LongestWordReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduceIncorrectKey() throws IOException
    {
        reduceDriver.withInput(new IntWritable(), new ArrayList<Text>());
        reduceDriver.runTest();
    }

    @Test
    public void testReduceEmptyValues() throws IOException
    {
        reduceDriver.withInput(new IntWritable(8), new ArrayList<Text>());
        reduceDriver.runTest();
    }

    @Test
    public void testReduceLongestWordsFromTwoMappersWithTheSameLength() throws IOException
    {
        List<Text> values1 = new ArrayList<Text>();
        values1.add(new Text("million"));
        values1.add(new Text("touched"));
        reduceDriver.withInput(new IntWritable(7), values1);
        List<Text> values2 = new ArrayList<Text>();
        values2.add(new Text("billion"));
        values2.add(new Text("touched"));
        reduceDriver.withInput(new IntWritable(7), values2);

        reduceDriver.withOutput(new IntWritable(7), new Text("billion"));
        reduceDriver.withOutput(new IntWritable(7), new Text("million"));
        reduceDriver.withOutput(new IntWritable(7), new Text("touched"));
        reduceDriver.runTest();
    }

    @Test
    public void testReduceLongestWordsFromTwoMappersWithDifferentLength() throws IOException
    {
        List<Text> values1 = new ArrayList<Text>();
        values1.add(new Text("million"));
        values1.add(new Text("touched"));
        reduceDriver.withInput(new IntWritable(7), values1);
        List<Text> values2 = new ArrayList<Text>();
        values2.add(new Text("sunlight"));
        values2.add(new Text("arsonist"));
        reduceDriver.withInput(new IntWritable(8), values2);

        reduceDriver.withOutput(new IntWritable(8), new Text("arsonist"));
        reduceDriver.withOutput(new IntWritable(8), new Text("sunlight"));
        reduceDriver.runTest();
    }
}
