package com.epam.courses.hadoop.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class LongestWordReducer extends Reducer<IntWritable, Text, IntWritable, Text>
{
    private final IntWritable maxLenght = new IntWritable();
    private final Set<String> longestWords = new TreeSet<String>();
    private final Text value = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
    {
        if (!isValidInput(key, values) && key.get() < maxLenght.get())
        {
            return;
        }
        else if (key.get() > maxLenght.get())
        {
            longestWords.clear();
            maxLenght.set(key.get());
        }
        for (Text value : values)
        {
            longestWords.add(value.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
        if (!longestWords.isEmpty())
        {
            for (String word : longestWords)
            {
                value.set(word);
                context.write(maxLenght, value);
            }
        }
    }

    private boolean isValidInput(IntWritable key, Iterable<Text> values)
    {
        if (key == null || key.get() <= 0 || values == null)
        {
            return false;
        }
        return true;
    }
}