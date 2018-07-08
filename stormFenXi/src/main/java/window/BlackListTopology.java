package window;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

public class BlackListTopology {

    public static void main(String[] args) {
        //zookeeper的服务器地址
        String zks = "192.168.157.21:2181";
        //消息的topic
        String topic = "mytopic2";
        //strom在zookeeper上的根
        String zkRoot = "/storm";
        String id = "mytopic2";
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkServers = Arrays.asList(new String[] {"192.168.157.21"});
        spoutConf.zkPort = 2181;

        TopologyBuilder builder = new TopologyBuilder();

        //指定的任务的spout组件，从Kafka中获取数据
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConf));

        //指定任务的第一个bolt组件，解析log信息，进行分词
        builder.setBolt("split_blot", new BlackListSplitBolt()).shuffleGrouping("kafka-reader");

        //指定任务的第二个bolt组件，创建窗口，计算窗口内的hot ip
        builder.setBolt("blacklist_blot", new BlackListTotalByWindowBolt()
                .withWindow(BaseWindowedBolt.Duration.seconds(30),   //窗口的长度
                        BaseWindowedBolt.Duration.seconds(10)))  //窗口的滑动距离
                .fieldsGrouping("split_blot", new Fields("userID"));

        //指定任务的第三个Bolt组件，将结果写入MySQL
//        builder.setBolt("blacklist_mysql_bolt", createMySQLBolt())
//        	   .shuffleGrouping("blacklist_blot");
        builder.setBolt("blacklist_mysql_bolt", new BlackListMySQLBolt())
                .shuffleGrouping("blacklist_blot");


        Config conf = new Config();
        //要保证超时时间大于等于窗口长度+滑动间隔长度
        conf.put("topology.message.timeout.secs", 40000);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("mydemo", conf, builder.createTopology());
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        cluster.shutdown();

    }

    //创建MySQLBolt组件，将结果插入MySQL
    private static IRichBolt createMySQLBolt() {
        ConnectionProvider connectionProvider = new MyConnectionProvider();
        return new JdbcInsertBolt(connectionProvider, new SimpleJdbcMapper("myresult", connectionProvider))
                .withTableName("myresult").withQueryTimeoutSecs(3);
    }
}

class MyConnectionProvider implements ConnectionProvider{
    private static String driver = "com.mysql.jdbc.Driver";
    private static String url = "jdbc:mysql://192.168.157.21:3306/demo";
    private static String user = "demo";
    private static String password = "Welcome_1";
    static{
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private Connection conn = null;

    public Connection getConnection() {
        try {
            return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public void prepare() {
    }

    public void cleanup() {
    }
}








