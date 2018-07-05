package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/5
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobDiagnosticsUpdateEvent;

public class JobSubmitter {

    public static void main(String[] args) throws Exception {

    	/*
    	 * 通过加载classpath下的*-site.xml文件解析参数
    	 */
        Configuration conf = new Configuration();
//        conf.addResource("core-site.xml");

    	/*
    	 * 通过代码设置参数
    	 */
        //conf.setInt("top.n",3);
        //conf.setInt("top.n",Integer.parseInt(args[0]));

    	/*
    	 * 通过属性配置文件获取参数
    	 */
        //Properties props = new Properties();
        //props.load(JobSubmitter.class.getClassLoader().getResourceAsStream("topn.properties"));
        //conf.setInt("top.n", Integer.parseInt(pros.getProperty("top.n")))


        Job job = Job.getInstance(conf);

        job.setJarByClass(JobDiagnosticsUpdateEvent.class);

        job.setMapperClass(PageTopnMapper.class);
        job.setReducerClass(PageTopnReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\input"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output1"));

        job.waitForCompletion(true);
    }


}
