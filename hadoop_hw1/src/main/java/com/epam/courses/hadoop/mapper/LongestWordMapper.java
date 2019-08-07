package com.epam.courses.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class LongestWordMapper extends Mapper<LongWritable, Text, IntWritable, Text>
{
    private static final String WORD_PATTERN = "[a-z'-]+";
    private static final String PUNCT_PATTERN = "[!\\\"#$%&()*+,./:;<=>?@\\[\\]^_`{|}~]";

    private final IntWritable outputKey = new IntWritable();
    private final Text outputValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        if (isValidInput(value))
        {
            StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            Set<String> longestWords = new TreeSet<String>();
            int currentMaxLength = 0;
            while (stringTokenizer.hasMoreTokens())
            {
                String element = stringTokenizer.nextToken().toLowerCase();
                element = element.replaceAll(PUNCT_PATTERN, "");
                int wordLenght = element.length();
                if (!element.matches(WORD_PATTERN))
                {
                    continue;
                }
                if (currentMaxLength == wordLenght)
                {
                    longestWords.add(element);
                }
                if (currentMaxLength < wordLenght)
                {
                    longestWords.clear();
                    currentMaxLength = wordLenght;
                    longestWords.add(element);
                }
            }
            for (String word : longestWords)
            {
                outputKey.set(currentMaxLength);
                outputValue.set(word);
                context.write(outputKey, outputValue);
            }
        }
    }

    private boolean isValidInput(Text value)
    {
        if (value == null || value.getLength() <= 0)
        {
            return false;
        }
        return true;
    }
}
