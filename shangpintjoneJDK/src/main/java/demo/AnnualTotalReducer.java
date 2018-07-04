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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AnnualTotalReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable k3, Iterable<DoubleWritable> v3,Context context)
            throws IOException, InterruptedException {
        // 将同一年的金额和个数求和
        double totalCount = 0;
        double totalMoney = 0;

        for(DoubleWritable v:v3) {
            totalCount ++;
            totalMoney += v.get();
        }

        context.write(k3, new Text(totalCount+"\t"+totalMoney));

    }

}