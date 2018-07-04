package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ProductSaleInfoReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key3, Iterable<Text> value3, Context context)
            throws IOException, InterruptedException {
        String productName = "";
        String info = "";

        //定义HashMap报出每年的销售的总额
        //  年份                     总额
        Map<Integer, Double> result = new HashMap<Integer, Double>();

        for(Text v:value3) {
            String str = v.toString();

            //判断: 是否存在  name:
            int index = str.indexOf("name:");
            if(index >= 0) {
                productName = str.substring(5);
            }else {
                //商品信息  time_id       amount
                //      words[2]+":"+words[6]
                //结果：       1999-02-11:9.31
                //取出年份
                int year = Integer.parseInt(str.substring(0, 4));

                //销售金额
                double amount = Double.parseDouble(str.substring(str.lastIndexOf(":")+1));

                if(result.containsKey(year)) {
                    result.put(year, result.get(year) + amount);
                }else {
                    result.put(year, amount);
                }

            }

        }
        context.write(new Text(productName), new Text(result.toString()));
    }
}




