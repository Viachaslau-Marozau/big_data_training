package com.epam.courses.hadoop.combiner;

import com.epam.courses.hadoop.type.ByteCountWritable;
import com.epam.courses.hadoop.type.ByteStatisticWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BytesCalculationCombinerTest
{
    ReduceDriver<Text, ByteCountWritable, Text, ByteCountWritable> combineDriver;

    @Before
    public void setUp()
    {
        BytesCalculationCombiner combiner = new BytesCalculationCombiner();
        combineDriver = ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void testCombineIncorrectKey() throws IOException
    {
        combineDriver.withInput(new Text(), new ArrayList<ByteCountWritable>());
        combineDriver.runTest();
    }

    @Test
    public void testCombineEmptyValue() throws IOException
    {
        combineDriver.withInput(new Text("ip13"), new ArrayList<ByteCountWritable>());
        combineDriver.withOutput(new Text("ip13"), new ByteCountWritable(0, 0));
        combineDriver.runTest();
    }

    @Test
    public void testCombineOneKey() throws IOException
    {
        List<ByteCountWritable> values1 = new ArrayList<ByteCountWritable>();
        values1.add(new ByteCountWritable(512, 1));
        values1.add(new ByteCountWritable(128, 1));
        values1.add(new ByteCountWritable(128, 1));
        values1.add(new ByteCountWritable(512, 1));
        values1.add(new ByteCountWritable(256, 1));
        combineDriver.withInput(new Text("ip3"), values1);
        combineDriver.withOutput(new Text("ip3"), new ByteCountWritable(1536, 5));
        combineDriver.runTest();
    }

    @Test
    public void testCombineSomeKeys() throws IOException
    {
        List<ByteCountWritable> values1 = new ArrayList<ByteCountWritable>();
        values1.add(new ByteCountWritable(512, 1));
        values1.add(new ByteCountWritable(128, 1));
        combineDriver.withInput(new Text("ip1"), values1);

        List<ByteCountWritable> values2 = new ArrayList<ByteCountWritable>();
        values2.add(new ByteCountWritable(128, 1));
        values2.add(new ByteCountWritable(512, 1));
        values2.add(new ByteCountWritable(256, 1));
        combineDriver.withInput(new Text("ip7"), values2);

        combineDriver.withOutput(new Text("ip1"), new ByteCountWritable(640, 2));
        combineDriver.withOutput(new Text("ip7"), new ByteCountWritable(896, 3));
        combineDriver.runTest();
    }
}
