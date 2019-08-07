package com.epam.courses.hadoop.combiner;

import com.epam.courses.hadoop.type.ByteCountWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BytesCalculationCombiner extends Reducer<Text, ByteCountWritable, Text, ByteCountWritable>
{
    @Override
    protected void reduce(Text key, Iterable<ByteCountWritable> values, Context context)
        throws IOException, InterruptedException
    {
        if (isValidInput(key, values))
        {
            int totalBytes = 0;
            int requestCount = 0;
            for (ByteCountWritable value : values)
            {
                totalBytes += value.getTotalBytes();
                requestCount += value.getRequestCount();
            }
            ByteCountWritable combinedValue = new ByteCountWritable(totalBytes, requestCount);
            context.write(key, combinedValue);
        }
    }

    private boolean isValidInput(Text key, Iterable<ByteCountWritable> values)
    {
        if (key == null || key.getLength() <= 0 || values == null)
        {
            return false;
        }
        return true;
    }
}
