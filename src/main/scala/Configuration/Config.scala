package main.scala.Configuration

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

object Config {
  //---------------CONFIGURATION-----------------
  val appName = "GraphBasedMining"
  val master = "local[*]"
  val serializer = "org.apache.spark.serializer.KryoSerializer"
  //---------------------------------------------
  
  //------------------VARIABLE-------------------
  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null
  //val classToSerialize = Array(classOf[CoocurrenceGraph], classOf[Graph], classOf[TestGraph], classOf[DFSCode], classOf[FinalDFSCode], classOf[EdgeCode], classOf[Edge], classOf[GraphBuilder])
  val defaultStorageLevel = StorageLevel.MEMORY_ONLY_SER
  var minSupport = 0.7d //giá trị mặc định, giá trị chính xác sẽ được gán trong hàm main do tham số người dùng truyền vào
  var minDistance = 0.7d
  var stopwordFilePath = "./SupportFiles/stopwords_en.txt"
  //---------------------------------------------
  
  //------------------ORIENTDB-------------------
  var hostType = "remote:"
  //if hostType is "memory:", set it empty
  var hostAddress = "localhost" //"/home/duytri/Downloads/Apps/orientdb-community-importers-2.2.30/databases"
  var database = "CoOccurrenceGraph"
  var dbUser = "admin"
  var dbPassword = "admin"
  //var port     = 2424
  var userRoot = "root"
  var pwdRoot = "12345"
  //---------------------------------------------
}