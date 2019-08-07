package com.epam.courses.hadoop.partitioner;

import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Map;
import java.util.TreeMap;

public class OperationSystemTypePartitioner extends Partitioner<ComplexKeyWritable, IntWritable>
{
    private static final String SYPPORTED_OPERATION_SYSTEMS = "Android,Android 1.x,Android 2.x,Android 2.x Tablet,Android 3.x Tablet,BlackBerryOS,Linux,Mac OS X,Mac OS X (iPad),Mac OS X (iPhone),Maemo,Nintendo Wii,Sony Playstation,Symbian OS,Symbian OS 9.x,WebOS,Windows,Windows 2000,Windows 7,Windows 98,Windows Mobile 7,Windows Vista,Windows XP,iOS 4 (iPhone),Unknown";
    private static final Map<String, Integer> osPartitioner = new TreeMap<String, Integer>();
    private static int osMapSize;

    static
    {
        String[] osArr = SYPPORTED_OPERATION_SYSTEMS.split(",");
        osMapSize = osArr.length;
        for (int i = 0; i < osMapSize; i++)
        {
            osPartitioner.put(osArr[i], i);
        }
    }

    @Override
    public int getPartition(ComplexKeyWritable complexKeyWritable, IntWritable intWritable, int reduceTasksNum)
    {
        if (reduceTasksNum < osMapSize)
        {
            throw new IllegalArgumentException("Number of supported OS more than number of redcers");
        }
        Integer reducerNumber = osPartitioner.get(complexKeyWritable.getOperationSystem());
        if (reducerNumber != null)
        {
            return reducerNumber.intValue();
        }
        return osMapSize - 1;
    }
}
