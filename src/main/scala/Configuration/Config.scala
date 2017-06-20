package main.scala.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Config {
  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null
  var minSupport = 0.7d //giá trị mặc định, giá trị chính xác sẽ được gán trong hàm main do tham số người dùng truyền vào
}