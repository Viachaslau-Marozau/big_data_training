import com.epam.courses.hadoop.combiner.EventNumberCalculationCombiner;
import com.epam.courses.hadoop.mapper.EventNumberCalculationMapper;
import com.epam.courses.hadoop.partitioner.OperationSystemTypePartitioner;
import com.epam.courses.hadoop.reducer.EventNumberCalculationReducer;
import com.epam.courses.hadoop.type.ComplexKeyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class EventNumberCalculationJobRunner
{
    public final static String CACHE_PATH = "/cache/viachaslau_marozau/city.en.txt";

    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
    {
        System.out.println("JAR parameters:");
        for (String arg : args)
        {
            System.out.println(arg);
        }

        Configuration conf = new Configuration();

        conf.set("mapreduce.output.textoutputformat.separator", ":");

        Job job = Job.getInstance(conf);

        job.setJobName("Hadoop homework 3");

        job.setJarByClass(EventNumberCalculationJobRunner.class);

        job.setMapperClass(EventNumberCalculationMapper.class);
        job.setCombinerClass(EventNumberCalculationCombiner.class);
        job.setReducerClass(EventNumberCalculationReducer.class);
        job.setPartitionerClass(OperationSystemTypePartitioner.class);

        job.setOutputKeyClass(ComplexKeyWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(new Path(CACHE_PATH).toUri());
        job.setNumReduceTasks(25);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        boolean exitMode = job.waitForCompletion(true);
        System.exit(exitMode ? 0 : -1);
    }
}
