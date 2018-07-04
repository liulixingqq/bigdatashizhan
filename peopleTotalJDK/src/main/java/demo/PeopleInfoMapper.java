package demo;

/**
 * description
 *
 * @author Llx
 * @version v1.0.0
 * @since 2018/7/4
 */
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PeopleInfoMapper extends Mapper<LongWritable, Text, Text, PeopleInfo> {

    @Override
    protected void map(LongWritable key1, Text value1, Context context)
            throws IOException, InterruptedException {

        String data = value1.toString();
        String[] words = data.split(" ");

        //生成PeopleInfo对象
        PeopleInfo info = new PeopleInfo();
        info.setPeopleID(Integer.parseInt(words[0]));
        info.setGender(words[1]);
        info.setHeight(Integer.parseInt(words[2]));

        //输出： key2 性别  value2 info
        context.write(new Text(info.getGender()), info);
    }
}
