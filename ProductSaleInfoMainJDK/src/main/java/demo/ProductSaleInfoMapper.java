package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ProductSaleInfoMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {
        //得到输入文件的路径
        String path = ((FileSplit)context.getInputSplit()).getPath().getName();
        //得到文件名称
        String fileName = path.substring(path.lastIndexOf("/") + 1);

        // 读入的数据可能是：product商品信息 也可能是sales订单信息
        String data = value1.toString();
        String[] words = data.split(",");

        //输出
        if(fileName.equals("products")) {
            //                              商品ID                             商品名称
            context.write(new IntWritable(Integer.parseInt(words[0])), new Text("name:" + words[1]));
        }else {
            //否则输出订单表信息                                        商品ID                                   time_id       amount
            context.write(new IntWritable(Integer.parseInt(words[0])),new Text(words[2]+":"+words[6]));
        }

    }
}
