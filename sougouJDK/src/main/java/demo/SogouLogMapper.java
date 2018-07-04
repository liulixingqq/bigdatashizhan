package demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */
public class SogouLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        String data = value1.toString();

        String[] words = data.split("\t");

        //过滤不满足条件的数据
        if(words.length != 6) return;

        try{
            //判断第三个元素和第四个元素
            if(Integer.parseInt(words[3]) == 1 && Integer.parseInt(words[4]) == 2){
                context.write(value1, NullWritable.get());
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
    }
}