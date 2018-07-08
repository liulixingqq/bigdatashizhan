package bigdatashizhan.demo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object CleanData {
  //定义main方法
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //设置环境变量
    System.setProperty("hadoop.home.dir", "D:\\tools\\hadoop-2.4.1")
    System.setProperty("HADOOP_USER_NAME","hdfs")

    val conf = new SparkConf().setAppName("Project04-CleanData").setMaster("local")
    val sc = new SparkContext(conf)

    //读取数据
    val fileRDD = sc.textFile("hdfs://192.168.157.21:8020/flume/20180530/events-.1527671337460")

    /*	1,201.105.101.102,http://mystore.jsp/?productid=1,2017020020,1,1
        2,201.105.101.103,http://mystore.jsp/?productid=2,2017020022,1,1
        3,201.105.101.105,http://mystore.jsp/?productid=3,2017020023,1,2
        4,201.105.101.107,http://mystore.jsp/?productid=1,2017020025,1,1
        1,201.105.101.102,http://mystore.jsp/?productid=4,2017020021,3,1
        1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
        1,201.105.101.102, ,2017020029,2,1
        1,201.105.101.102, ,2017020029,2
     */

    //清洗数据    条件1：过滤不满足6个字段的数据   条件2：过滤URL为空的数据，即：过滤出包含http开头的日志信息
    val cleanDataRDD = fileRDD.map(_.split(",")).filter(_(2).startsWith("http")).filter(_.length == 6)

    //测试输出的结果
    //cleanDataRDD.foreach { x => println(x.mkString("|")) }
    //将结果保存到HDFS
    cleanDataRDD.map(x=>x.mkString(",")).saveAsTextFile("hdfs://192.168.157.21:8020/cleandata/project04")

    sc.stop()

    println("Finish")
  }
}
