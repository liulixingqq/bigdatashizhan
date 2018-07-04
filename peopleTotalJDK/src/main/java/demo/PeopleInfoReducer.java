package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PeopleInfoReducer extends Reducer<Text, PeopleInfo, Text, IntWritable> {

    @Override
    protected void reduce(Text key3, Iterable<PeopleInfo> value3,Context context) throws IOException, InterruptedException {
        //key3就是性别
        //定义几个变量来保存结果

        //总人数
        int totalNumber = 0;
        //最高身高
        int highest = 0;

        //最低身高：注意初始值
        int lowest = 10000;

        for(PeopleInfo info:value3){
            //总人数加一
            totalNumber ++;

            //得到最高身高
            if(info.getHeight() > highest){
                highest = info.getHeight();
            }

            //得到最低身高
            if(info.getHeight() < lowest){
                lowest = info.getHeight();
            }
        }

        //输出
        context.write(new Text("Total of " + key3.toString()), new IntWritable(totalNumber));
        context.write(new Text("Highest of " + key3.toString()), new IntWritable(highest));
        context.write(new Text("Lowest of " + key3.toString()), new IntWritable(lowest));
    }
}

