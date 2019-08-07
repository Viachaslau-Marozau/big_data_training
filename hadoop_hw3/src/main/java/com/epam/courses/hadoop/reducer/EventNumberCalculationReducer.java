package com.epam.courses.hadoop.reducer;

import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.TreeMap;

public class EventNumberCalculationReducer extends Reducer<ComplexKeyWritable, IntWritable, Text, IntWritable>
{

    private static final int CITY_CODE_INDEX = 0;
    private static final int CITY_NAME_INDEX = 1;

    private static final int ZERO = 0;

    private static final Map<Integer, String> cityNameDistributedCache = new TreeMap<Integer, String>();

    private final Text outputKey = new Text();
    private final IntWritable reducedValue = new IntWritable();

    @Override
    public void setup(Context context)
    {
        try
        {
            FileSystem hdfs = FileSystem.get(context.getConfiguration());
            URI[] uriArr = context.getCacheFiles();
            for (URI uri : uriArr)
            {
                if (uri.toString().endsWith("city.en.txt"))
                {
                    Path path = new Path(uri);
                    readCityNamesFromDistributedCacheFile(hdfs, path);
                }
            }
        }
        catch (IOException e)
        {
        }
    }

    @Override
    protected void reduce(ComplexKeyWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException
    {
        if (isValidInput(key, values))
        {
            String cityName = cityNameDistributedCache.get(key.getSityCode());
            if (cityName != null && !cityName.isEmpty())
            {
                outputKey.set(cityName);
            }
            else
            {
                outputKey.set("unknown (city code: " + key.getSityCode() + ")");
            }
            int totalBiddingAmountNumber = ZERO;
            for (IntWritable value : values)
            {
                totalBiddingAmountNumber += value.get();
            }
            if (totalBiddingAmountNumber > ZERO)
            {
                reducedValue.set(totalBiddingAmountNumber);
                context.write(outputKey, reducedValue);
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

    private void readCityNamesFromDistributedCacheFile(FileSystem hdfs, Path path) throws IOException
    {
        InputStreamReader inputStreamReader = new InputStreamReader(hdfs.open(path));
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String line;
        try
        {
            while ((line = bufferedReader.readLine()) != null)
            {
                String[] cityMapParams = line.split("[\\W]");
                cityNameDistributedCache
                    .put(Integer.parseInt(cityMapParams[CITY_CODE_INDEX].trim()), cityMapParams[CITY_NAME_INDEX].trim());
            }
        }
        finally
        {
            bufferedReader.close();
            inputStreamReader.close();
        }
    }
}