package bigdatashizhan.demo.window

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object BlackUserList {

  //定义HotIP表
  case class HotIP(user_id:Int,pv:Int)

  //定义用户信息表，这里为了简单我们只需要用户的id和name即可
  case class UserInfo(user_id:Int,username:String)

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SparkFlumeNGWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    //由于需要使用Spark SQL进行分析，创建SQLContext对象
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._

    //装载UserInfo表
    val userInfo = sc.textFile("hdfs://192.168.157.21:8020/input/userinfo.txt").map(_.split(","))
      .map(x=> new UserInfo(x(0).toInt,x(1))).toDF()
    userInfo.createTempView("userinfo")


    //创建topic名称，1表示一次从这个topic中获取一条记录
    val topics = Map("mytopic" -> 1)

    //创建Kafka的输入流，指定ZooKeeper的地址
    val kafkaStream = KafkaUtils.createStream(ssc, "192.168.157.21:2181", "mygroup", topics)

    //4,201.105.101.107,http://mystore.jsp/?productid=1,2017020025,1,1
    val logRDD = kafkaStream.map(_._2)

    //直接从log日志中，解析出用户的ip地址，并使用窗口函数进行统计
    //4,201.105.101.107,http://mystore.jsp/?productid=1,2017020025,1,1
    /*
            第一个参数：执行运算
            第二个参数：窗口的大小
            第三个参数：窗口滑动的距离

            例子：每10秒钟，把过去30秒的数据进行统计  ----> 出错
            注意：第二个参数  第三个参数 必须是采用频率的整数倍

            例子：每9秒钟，把过去30秒的数据进行统计
     */
    //这里我们从日志中解析出用户的ID，然后每个ID记一次数
    val hotip = logRDD.map(_.split(",")).map(x=>(x(0),1))
      .reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(30),Seconds(10))

    //过滤出访问频率大于5的用户ID和PV值
    val result = hotip.filter(x=> x._2 > 10)
    result.foreachRDD(rdd => {
      //将每一条数据注册成HotIP表的数据，使用Spark SQL进行查询
      val hotIPTable = rdd.map(x=>new HotIP(x._1.toInt,x._2)).toDF()
      hotIPTable.createOrReplaceTempView("hotip")
      //sqlContext.sql("select * from hotip").show()

      sqlContext.sql("select userinfo.user_id,userinfo.username,hotip.pv from hotip,userinfo where hotip.user_id=userinfo.user_id").show()
    })

    ssc.start()
    ssc.awaitTermination();
  }
}

