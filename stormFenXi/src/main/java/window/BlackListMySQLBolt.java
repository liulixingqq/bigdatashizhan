package window;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class BlackListMySQLBolt extends BaseRichBolt {
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

    public void execute(Tuple tuple) {
        int userid = tuple.getIntegerByField("userid");
        int PV = tuple.getIntegerByField("PV");

        String sql = "insert into myresult(userid,PV) values("+userid+","+PV+") on duplicate key update PV=PV+"+PV;
        Connection conn = null;
        Statement st = null;
        try{
            conn = DriverManager.getConnection(url, user, password);
            st = conn.createStatement();
            st.execute(sql);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(st != null){
                try {
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
        // TODO Auto-generated method stub

    }

    public void declareOutputFields(OutputFieldsDeclarer arg0) {
        // TODO Auto-generated method stub

    }

}
