package bigdatashizhan.demo

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * description
  *
  * @author Llx
  * @version v1.0.0
  * @since 2018/7/4
  */
object AnnualTotal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AnnualTotal").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //读入数据
    val myData = sc.textFile("hdfs://192.168.56.71:9000/data/sales").map(line => {
      //处理该行数据，取出年份、金额
      val words = line.split(",")

      (Integer.parseInt(words(2).substring(0, 4)),java.lang.Double.parseDouble(words(6)))
    }
    ).map(d => OrderInfo(d._1,d._2)).toDF()

    /*
     * 这里得到的表结构如下：
       +--------+---------+-------+
       |col_name|data_type|comment|
       +--------+---------+-------+
       |    year|      int|   null|
       |  amount|      int|   null|
       +--------+---------+-------+
     */
    myData.createTempView("annualorder")

    sqlContext.sql("select year,count(amount),sum(amount) from annualorder group by year").show()

    sc.stop()

  }
  case class OrderInfo(year:Int,amount:Double)
}

