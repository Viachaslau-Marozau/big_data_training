import com.epam.courses.hadoop.mapper.LongestWordMapper;
import com.epam.courses.hadoop.reducer.LongestWordReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class LongestWordJobRunner
{
    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Search the longest words at input file");

        job.setJarByClass(LongestWordJobRunner.class);

        job.setMapperClass(LongestWordMapper.class);
        job.setReducerClass(LongestWordReducer.class);
        job.setCombinerClass(LongestWordReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
