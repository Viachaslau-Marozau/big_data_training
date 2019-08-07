package com.epam.courses.hadoop.mapper;

import com.epam.courses.hadoop.type.ByteCountWritable;
import nl.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BytesCalculationMapper extends Mapper<LongWritable, Text, Text, ByteCountWritable>
{
    private static final int ZERO = 0;
    private static final int ONE = 1;
    private static final int IP_INDEX = 1;
    private static final int BYTES_INDEX = 5;
    private static final int BROWSER_INFO_INDEX = 7;
    private static final String BROWSER_COUNTER_GROUP = "browsers";

    private static final String LINE_PATTERN_AS_STRING = "^(ip\\d+)\\s+-\\s+-\\s+\\[(.+)\\]\\s+\\\"([^\\\"]*)\\\"\\s+(\\d{3})\\s+(\\d+|-)\\s+\\\"([^\\\"]*)\\\"\\s+\\\"([^\\\"]*)\\\"";
    private static final Pattern LINE_PATTERN = Pattern.compile(LINE_PATTERN_AS_STRING);

    private final Text key = new Text();
    private final ByteCountWritable val = new ByteCountWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        if (isValidInput(value))
        {
            Matcher matcher = LINE_PATTERN.matcher(value.toString());
            if (matcher.find())
            {
                String ip = matcher.group(IP_INDEX);
                String bytesAsString = matcher.group(BYTES_INDEX);
                String browserInfo = matcher.group(BROWSER_INFO_INDEX);
                int bytes = ZERO;
                if (!"-".equals(bytesAsString))
                {
                    bytes = Integer.parseInt(bytesAsString);
                }
                writeData(context, ip, bytes);
                writeBrowser(context, browserInfo);
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

    private void writeData(Context context, String ip, int bytes) throws IOException, InterruptedException
    {
        key.set(ip);
        val.setTotalBytes(bytes);
        val.setRequestCount(ONE);
        context.write(key, val);
    }

    private void writeBrowser(Context context, String browserInfo)
    {
        UserAgent userAgent = UserAgent.parseUserAgentString(browserInfo);
        String browserName = userAgent.getBrowser().getName();
        if (browserName != null)
        {
            context.getCounter(BROWSER_COUNTER_GROUP, browserName).increment(ONE);
        }
    }
}
