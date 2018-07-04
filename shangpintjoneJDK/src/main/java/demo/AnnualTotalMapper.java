package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AnnualTotalMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        // 数据：13,524,1998-01-20,2,999,1,1205.99
        String data = value1.toString();

        //分词
        String[] words = data.split(",");

        //输出：年份   金额
        context.write(new IntWritable(Integer.parseInt(words[2].substring(0, 4))), new DoubleWritable(Double.parseDouble(words[6])));
    }

}
