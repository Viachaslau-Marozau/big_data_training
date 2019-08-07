import com.epam.courses.hadoop.combiner.BytesCalculationCombiner;
import com.epam.courses.hadoop.mapper.BytesCalculationMapper;
import com.epam.courses.hadoop.reducer.BytesCalculationReducer;
import com.epam.courses.hadoop.type.ByteCountWritable;
import com.epam.courses.hadoop.type.ByteStatisticWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class BytesCalculationJobRunner
{
    private static final String BROWSER_COUNTER_GROUP = "browsers";
    public final static String SNAPPY_MODE = "snappy";

    private static class TextComparator extends WritableComparator
    {
        TextComparator()
        {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b)
        {
            Text t1 = (Text) a;
            Text t2 = (Text) b;
            int result = t1.getLength() - t2.getLength();
            if (result == 0)
            {
                result = t1.toString().compareTo(t2.toString());
            }
            return result;
        }
    }

    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
    {
        System.out.println("JAR parameters:");
        for (String arg : args)
        {
            System.out.println(arg);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf);

        job.setJobName("Hadoop homework 2");

        job.setJarByClass(BytesCalculationJobRunner.class);

        job.setMapperClass(BytesCalculationMapper.class);
        job.setCombinerClass(BytesCalculationCombiner.class);
        job.setReducerClass(BytesCalculationReducer.class);
        job.setSortComparatorClass(TextComparator.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (args.length > 2 && args[2] != null)
        {
            String mode = args[2];
            if (SNAPPY_MODE.equalsIgnoreCase(mode))
            {
                job.setOutputFormatClass(SequenceFileOutputFormat.class);
                SequenceFileOutputFormat.setCompressOutput(job, true);
                FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(ByteStatisticWritable.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(ByteCountWritable.class);
            }
            else
            {
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(ByteCountWritable.class);
            }
        }

        boolean exitMode = job.waitForCompletion(true);

        System.out.println("\n\nFound browsers:\n");
        CounterGroup counters = job.getCounters().getGroup(BROWSER_COUNTER_GROUP);
        for (Counter counter : counters)
        {
            System.out.println(counter.getDisplayName() + ": " + counter.getValue());
        }
        System.exit(exitMode ? 0 : -1);
    }
}
