package window;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

public class BlackListTotalByWindowBolt extends BaseWindowedBolt {

    //使用Map集合存储结果
    private Map<Integer, Integer> result = new HashMap<Integer, Integer>();

    private OutputCollector collector;

    public void execute(TupleWindow inputWindow) {
        //获取窗口中的内容
        List<Tuple> input = inputWindow.get();

        //处理该该窗口中的每个Tuple
        for(Tuple tuple:input){
            //取出数据
            int userID = tuple.getIntegerByField("userID");
            int count = tuple.getIntegerByField("count");

            //求和
            if(result.containsKey(userID)){
                //如果已经存在，累加
                int total = result.get(userID);
                result.put(userID, total+count);
            }else{
                //这是一个新用户ID
                result.put(userID, count);
            }

            //输出到屏幕：每个IP的热度
            System.out.println("统计的结果是：" + result);

            this.collector.ack(tuple);

            //过滤统计的结果，进行黑名单检查
            if(result.get(userID) > 6){
                //输出给下一个组件
                //                            单词           总频率
                this.collector.emit(new Values(userID,result.get(userID)));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userid","PV"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
}

