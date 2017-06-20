package main.scala.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object Config {
  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null
  //val classToSerialize = Array(classOf[CoocurrenceGraph], classOf[Graph], classOf[TestGraph], classOf[DFSCode], classOf[FinalDFSCode], classOf[EdgeCode], classOf[Edge], classOf[GraphBuilder])
  val defaultStorageLevel = StorageLevel.MEMORY_ONLY_SER
  var minSupport = 0.7d //giá trị mặc định, giá trị chính xác sẽ được gán trong hàm main do tham số người dùng truyền vào
}