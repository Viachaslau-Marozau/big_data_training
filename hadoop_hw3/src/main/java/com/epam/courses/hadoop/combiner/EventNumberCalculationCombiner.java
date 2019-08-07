package com.epam.courses.hadoop.combiner;

import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EventNumberCalculationCombiner extends Reducer<ComplexKeyWritable, IntWritable, ComplexKeyWritable, IntWritable>
{
    private static final int ZERO = 0;

    private final IntWritable combinedValue = new IntWritable();

    @Override
    protected void reduce(ComplexKeyWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException
    {
        if (isValidInput(key, values))
        {
            int totalBiddingAmountNumber = ZERO;
            for (IntWritable value : values)
            {
                totalBiddingAmountNumber += value.get();
            }
            if (totalBiddingAmountNumber > ZERO)
            {
                combinedValue.set(totalBiddingAmountNumber);
                context.write(key, combinedValue);
            }
        }
    }

    private boolean isValidInput(ComplexKeyWritable key, Iterable<IntWritable> values)
    {
        if (key == null || values == null)
        {
            return false;
        }
        return true;
    }
}
