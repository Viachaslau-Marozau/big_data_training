package com.epam.courses.hadoop.mapper;

import com.epam.courses.hadoop.type.ComplexKeyWritable;
import nl.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EventNumberCalculationMapper extends Mapper<LongWritable, Text, ComplexKeyWritable, IntWritable>
{
    private static final int MIN_FILTERING_AMOUNT = 250;
    private static final int ONE = 1;

    private static final int CITY_CODE_INDEX = 8;
    private static final int USER_AGENT_INDEX = 5;
    private static final int BIDDING_AMOUNT_INDEX = 20;

    private static final String OPERATION_SYSTEM_GROUP = "Operation systems";
    private static final String ERRORS_GROUP = "Errors";

    private static final String NOT_MATCHED_LINES = "Not matched lines";
    private static final String NUMBER_FORMAT_ERROR_LINES = "NumberFormatException lines";

    private static final String LINE_PATTERN_AS_STRING = "^([0-9a-f]+)\\t(\\d+)\\t(\\d+)\\t([^\\t]+)\\t" +
                                                         "([^\\t]*)\\t([^\\t]+)\\t(\\d+)\\t(\\d+)\\t" +
                                                         "([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)\\t([^\\t]+)\\t" +
                                                         "([^\\t]+)\\t(\\d+)\\t(\\d+)\\t([^\\t]+)\\t" +
                                                         "([^\\t]+)\\t(\\d+)\\t(\\d+)\\t(\\d+)\\t" +
                                                         "(\\d+)\\t([^\\t]+)\\t(\\d+)\\t([^\\t]+).*";
    private final Pattern LINE_PATTERN = Pattern.compile(LINE_PATTERN_AS_STRING);

    private final ComplexKeyWritable key = new ComplexKeyWritable();
    private final IntWritable val = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        if (isValidInput(value))
        {
            Matcher matcher = LINE_PATTERN.matcher(value.toString());
            if (matcher.find())
            {
                String cityCode = matcher.group(CITY_CODE_INDEX);
                String userAgentInfo = matcher.group(USER_AGENT_INDEX);
                String biddingAmount = matcher.group(BIDDING_AMOUNT_INDEX);
                writeData(context, cityCode, userAgentInfo, biddingAmount);
            }
            else
            {
                context.getCounter(ERRORS_GROUP, NOT_MATCHED_LINES).increment(ONE);
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

    private void writeData(Context context, String cityCode, String userAgentInfo, String biddingAmountAsString)
        throws IOException, InterruptedException
    {
        try
        {
            int biddingAmount = Integer.parseInt(biddingAmountAsString);
            if (biddingAmount > MIN_FILTERING_AMOUNT)
            {
                UserAgent userAgent = UserAgent.parseUserAgentString(userAgentInfo);
                String operationSystem = userAgent.getOperatingSystem().getName();
                context.getCounter(OPERATION_SYSTEM_GROUP, operationSystem).increment(ONE);
                key.setSityCode(Integer.parseInt(cityCode));
                key.setOperationSystem(operationSystem);
                val.set(ONE);

                context.write(key, val);
            }
        }
        catch (NumberFormatException e)
        {
            context.getCounter(ERRORS_GROUP, NUMBER_FORMAT_ERROR_LINES).increment(ONE);
        }
    }
}
