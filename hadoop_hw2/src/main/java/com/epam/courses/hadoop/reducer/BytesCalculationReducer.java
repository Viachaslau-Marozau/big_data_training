package com.epam.courses.hadoop.reducer;

import com.epam.courses.hadoop.type.ByteCountWritable;
import com.epam.courses.hadoop.type.ByteStatisticWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BytesCalculationReducer extends Reducer<Text, ByteCountWritable, Text, ByteStatisticWritable>
{
    private final ByteStatisticWritable statisticValue = new ByteStatisticWritable();

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
            double averageValue = ((double) totalBytes) / requestCount;
            statisticValue.setTotalBytes(totalBytes);
            statisticValue.setAverageValue(averageValue);
            context.write(key, statisticValue);
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