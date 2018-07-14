package scalaTJ

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS

object PredictProduct {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setMaster("local").setAppName("predict")
    val sc = new SparkContext(conf)

    val data = sc.textFile("D:\\download\\data\\sales.data")
    val parseData = data.filter(_.length() > 0).map{line =>
      val parts = line.split("@")
      LabeledPoint(parts(0).trim().toDouble,Vectors.dense(parts(1).split(",").map(_.trim().toDouble)))
    }

    //有两种最优化算法可以求解逻辑回归问题并求出最优参数：
    //（1）mini-batch gradient descent(梯度下降法）：只支持二分类
    //（2）L-BFGS法：支持多分类，而且能够更快聚合
    //我们更推荐使用L-BFGS，因为它能更快聚合,而且现在spark2.1.0已经放弃LogisticRegressionWithLSGD()模式了
    //注意：setNumClasses表示输出类型的可能性，这里设置为2，表示的是二分类
    val model= new LogisticRegressionWithLBFGS().setNumClasses(2).run(parseData)
    val target = Vectors.dense(0,30,182,6)

    val result = model.predict(target)
    println("买还是不买？  " + result)
    sc.stop()
  }
}
