package demo;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class HotIPTotalBolt extends BaseRichBolt {

    //使用Map集合存储结果
    private Map<String, Integer> result = new HashMap<String, Integer>();

    private OutputCollector collector;

    public void execute(Tuple tuple) {
        //取出数据
        String ip = tuple.getStringByField("ip");
        int count = tuple.getIntegerByField("count");

        //求和
        if(result.containsKey(ip)){
            //如果已经存在，累加
            int total = result.get(ip);
            result.put(ip, total+count);
        }else{
            //这是一个新ip
            result.put(ip, count);
        }

        //输出到屏幕：每个IP的热度
        System.out.println("统计的结果是：" + result);

        //输出给下一个组件                                               单词           总频率
        this.collector.emit(new Values(ip,result.get(ip)));

        this.collector.ack(tuple);
    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declare) {
        //申明输出格式：两个字段（ip,1）
        declare.declare(new Fields("ip","total"));
    }

}