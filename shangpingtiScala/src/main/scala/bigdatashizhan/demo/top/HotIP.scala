package bigdatashizhan.demo.top

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object HotIP {

  //经过清洗后的用户点击数据   需要从url中解析出product_id
  case class LogInfo(user_id: String, user_ip: String, product_id: String,
                     click_time: String, action_type: String, area_id: String)

  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SparkFlumeNGWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    //由于需要使用Spark SQL进行分析，创建SQLContext对象
    val sqlContext = new SQLContext(ssc.sparkContext)
    import sqlContext.implicits._

    //创建topic名称，1表示一次从这个topic中获取一条记录
    val topics = Map("mytopic" -> 1)

    //创建Kafka的输入流，指定ZooKeeper的地址
    val kafkaStream = KafkaUtils.createStream(ssc, "192.168.157.21:2181", "mygroup", topics)

    //4,201.105.101.107,http://mystore.jsp/?productid=1,2017020025,1,1
    val logRDD = kafkaStream.map(_._2)
    logRDD.foreachRDD((rdd: RDD[String], time: Time) => {
      val result = rdd.map(_.split(",")).map(x => new LogInfo(x(0), x(1), x(2), x(3), x(4), x(5))).toDF

      result.createOrReplaceTempView("clicklog")

      sqlContext.sql("select user_ip as IP,count(user_ip) as PV from clicklog group by user_ip").show
    })

    ssc.start()
    ssc.awaitTermination();
  }
}
