package com.epam.courses.hadoop.reducer;

import com.epam.courses.hadoop.type.ByteCountWritable;
import com.epam.courses.hadoop.type.ByteStatisticWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BytesCalculationReducerTest
{
    ReduceDriver<Text, ByteCountWritable, Text, ByteStatisticWritable> reduceDriver;

    @Before
    public void setUp()
    {
        BytesCalculationReducer reducer = new BytesCalculationReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReduceIncorrectKey() throws IOException
    {
        reduceDriver.withInput(new Text(), new ArrayList<ByteCountWritable>());
        reduceDriver.runTest();
    }

    @Test
    public void testReduceEmptyValues() throws IOException
    {
        reduceDriver.withInput(new Text("ip1"), new ArrayList<ByteCountWritable>());
        reduceDriver.withOutput(new Text("ip1"), new ByteStatisticWritable(Double.NaN, 0));
        reduceDriver.runTest();
    }

    @Test
    public void testReduceOneKey() throws IOException
    {
        List<ByteCountWritable> values1 = new ArrayList<ByteCountWritable>();
        values1.add(new ByteCountWritable(512, 4));
        values1.add(new ByteCountWritable(128, 1));
        values1.add(new ByteCountWritable(128, 1));
        values1.add(new ByteCountWritable(512, 4));
        values1.add(new ByteCountWritable(256, 2));
        reduceDriver.withInput(new Text("ip3"), values1);
        reduceDriver.withOutput(new Text("ip3"), new ByteStatisticWritable(128, 1536));
        reduceDriver.runTest();
    }

    @Test
    public void testReduceSomeKeys() throws IOException
    {
        List<ByteCountWritable> values1 = new ArrayList<ByteCountWritable>();
        values1.add(new ByteCountWritable(512, 4));
        values1.add(new ByteCountWritable(128, 1));
        reduceDriver.withInput(new Text("ip1"), values1);

        List<ByteCountWritable> values2 = new ArrayList<ByteCountWritable>();
        values2.add(new ByteCountWritable(128, 1));
        values2.add(new ByteCountWritable(512, 4));
        values2.add(new ByteCountWritable(256, 2));
        reduceDriver.withInput(new Text("ip7"), values2);

        reduceDriver.withOutput(new Text("ip1"), new ByteStatisticWritable(128, 640));
        reduceDriver.withOutput(new Text("ip7"), new ByteStatisticWritable(128, 896));
        reduceDriver.runTest();
    }
}
