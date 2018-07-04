package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PeopleInfoMain {

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PeopleInfoMain.class);

        job.setMapperClass(PeopleInfoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PeopleInfo.class);

        job.setReducerClass(PeopleInfoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
