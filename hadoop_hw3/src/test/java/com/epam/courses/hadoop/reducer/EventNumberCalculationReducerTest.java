package com.epam.courses.hadoop.reducer;

import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class EventNumberCalculationReducerTest
{
    ReduceDriver<ComplexKeyWritable, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp()
    {
        EventNumberCalculationReducer reducer = new EventNumberCalculationReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        URI cacheFileURI = new File("input_data/viachaslau_marozau/cache/city.en.txt").toURI();
        reduceDriver.setCacheFiles(new URI[] {cacheFileURI});
    }

    @Test
    public void testReduceEmptyValues() throws IOException
    {
        reduceDriver.withInput(new ComplexKeyWritable(237, "Mac OS X (iPad)"), new ArrayList<IntWritable>());
        reduceDriver.runTest();
    }

    @Test
    public void testReduceOneKey() throws IOException
    {
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(10));
        values1.add(new IntWritable(7));
        values1.add(new IntWritable(1));
        values1.add(new IntWritable(8));
        values1.add(new IntWritable(3));
        reduceDriver.withInput(new ComplexKeyWritable(13, "Windows 7"), values1);
        reduceDriver.withOutput(new Text("langfang"), new IntWritable(29));
        reduceDriver.runTest();
    }

    @Test
    public void testReduceSomeKeys() throws IOException
    {
        List<IntWritable> values1 = new ArrayList<IntWritable>();
        values1.add(new IntWritable(11));
        values1.add(new IntWritable(17));
        reduceDriver.withInput(new ComplexKeyWritable(13, "Windows 7"), values1);

        List<IntWritable> values2 = new ArrayList<IntWritable>();
        values2.add(new IntWritable(8));
        values2.add(new IntWritable(3));
        values2.add(new IntWritable(2));
        reduceDriver.withInput(new ComplexKeyWritable(29, "Windows XP"), values2);

        reduceDriver.withOutput(new Text("langfang"), new IntWritable(28));
        reduceDriver.withOutput(new Text("baotou"), new IntWritable(13));
        reduceDriver.runTest();
    }
}
