package bigdatashizhan.demo

/**
  * description
  *
  * @author Llx
  * @version v1.0.0
  * @since 2018/7/4
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object ProductSalesInfo {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("ProductSalesInfo").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //取出商品数据
    val productInfo = sc.textFile("hdfs://192.168.56.71:9000/data/products").map(line=>{
      val words = line.split(",")

      //返回：商品ID、商品名称
      (Integer.parseInt(words(0)),words(1))
    }
    ).map(d=>Product(d._1,d._2))
      .toDF()

    //取出销售订单数据
    val orderInfo = sc.textFile("hdfs://192.168.56.71:9000/data/sales").map(line => {
      //处理该行数据，取出商品ID、年份、金额
      val words = line.split(",")

      (Integer.parseInt(words(0)),     //商品ID
        Integer.parseInt(words(2).substring(0, 4)),  //年份
        java.lang.Double.parseDouble(words(6))) //金额
    }
    ).map(d=>SaleOrder(d._1,d._2,d._3))
      .toDF()


    //注册成表
    productInfo.createOrReplaceTempView("product")
    orderInfo.createOrReplaceTempView("salesorder")

    //执行查询，得到第一步的结果
    val result = sqlContext.sql("select prod_name,year_id,sum(amount) from product,salesorder where product.prod_id=salesorder.prod_id group by prod_name,year_id order by 1")
      .toDF("prod_name","year_id","total")
    result.createOrReplaceTempView("result")

    //第二步：将列值转成列名
    val finalResult = sqlContext.sql("select prod_name,sum(case year_id when 1998 then total else 0 end),sum(case year_id when 1999 then total else 0 end),sum(case year_id when 2000 then total else 0 end),sum(case year_id when 2001 then total else 0 end) from result group by prod_name")
      .toDF("prod_name","1998","1999","2000","2001")
    finalResult.show()

    sc.stop()

  }
  //商品信息
  case class Product(prod_id:Int,prod_name:String)

  //订单信息
  case class SaleOrder(prod_id:Int,year_id:Int,amount:Double)
}


