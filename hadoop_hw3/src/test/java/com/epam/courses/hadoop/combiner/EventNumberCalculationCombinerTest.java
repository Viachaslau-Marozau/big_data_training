package com.epam.courses.hadoop.combiner;

import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EventNumberCalculationCombinerTest
{
    ReduceDriver<ComplexKeyWritable, IntWritable, ComplexKeyWritable, IntWritable> combineDriver;

    @Before
    public void setUp()
    {
        EventNumberCalculationCombiner combiner = new EventNumberCalculationCombiner();
        combineDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testCombineEmptyValue() throws IOException
    {
        combineDriver.withInput(new ComplexKeyWritable(237, "Mac OS X (iPad)"), new ArrayList<IntWritable>());
        combineDriver.runTest();
    }

    @Test
    public void testCombineOneKey() throws IOException
    {
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        combineDriver.withInput(new ComplexKeyWritable(13, "Windows 7"), values1);
        combineDriver.withOutput(new ComplexKeyWritable(13, "Windows 7"), new IntWritable(5));
        combineDriver.runTest();
    }

    @Test
    public void testCombineSomeKeys() throws IOException
    {
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(1));
        combineDriver.withInput(new ComplexKeyWritable(13, "Windows 7"), values1);

        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));
        values2.add(new IntWritable(1));
        combineDriver.withInput(new ComplexKeyWritable(29, "Windows XP"), values2);

        combineDriver.withOutput(new ComplexKeyWritable(13, "Windows 7"), new IntWritable(2));
        combineDriver.withOutput(new ComplexKeyWritable(29, "Windows XP"), new IntWritable(3));
        combineDriver.runTest();
    }
}
