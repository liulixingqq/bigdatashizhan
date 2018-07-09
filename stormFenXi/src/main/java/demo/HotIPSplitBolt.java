package demo;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class HotIPSplitBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void execute(Tuple tuple) {
        // 1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
        String log = tuple.getString(0);

        //进行分词，解析出user_ip
        String[] words = log.split(",");

        //过滤不满足要求的日志信息
        if(words.length == 6){
            //System.out.println("解析的ip是：" + words[1]);
            //每个ip记一次数
            this.collector.emit(new Values(words[1],1));
        }

        this.collector.ack(tuple);
    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
        this.collector = collector;
    }

    public void declareOutputFields(OutputFieldsDeclarer declare) {
        //申明输出格式：两个字段（ip,1）
        declare.declare(new Fields("ip","count"));
    }

}