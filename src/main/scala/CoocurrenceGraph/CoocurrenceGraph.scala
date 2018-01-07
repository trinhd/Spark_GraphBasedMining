package main.scala.CoocurrenceGraph

import scala.util.Properties

import main.scala.Input.HDFSReader
import main.scala.Output.OutputFileWriter
import org.apache.spark.rdd.RDD
import main.scala.Input.FileReader
import main.scala.Configuration.Config
import main.scala.OrientDBUtils
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool
import com.orientechnologies.orient.core.record.impl.ODocument
import scala.collection.mutable.ListBuffer

class CoocurrenceGraph {
  def createCoocurrenceGraphSet(folderPath: String): RDD[Graph] = {
    val rddDoc = HDFSReader.hdfsFolderReader(folderPath)
    val rddGraph = rddDoc.map(doc => {
      val arr = doc._2.split(Properties.lineSeparator)
      var graph = new Graph()
      for (i <- 0 to arr.length - 2) {
        graph.addOrUpdateVertex(arr(i), arr(i + 1))
      }
      graph
    })
    rddGraph
  }

  def createCoocurrenceGraphSetFromLocal(folderPath: String): RDD[Graph] = {
    val arrDoc = FileReader.folderReader(folderPath)
    val arrGraph = arrDoc.map(doc => {
      val arr = doc._2
      var graph = new Graph()
      for (i <- 0 to arr.length - 2) {
        graph.addOrUpdateVertex(arr(i), arr(i + 1))
      }
      graph
    })
    Config.sparkContext.parallelize(arrGraph)
  }
  
  def getAllTopicFromOrientDB(): List[String] = {
    val OrientDBUtilsDoc = new OrientDBUtils(Config.hostType, Config.hostAddress, Config.database, Config.dbUser, Config.dbPassword, Config.userRoot, Config.pwdRoot)
    val connectionPool = OrientDBUtilsDoc.connectDBUsingDocAPI
    val arrDoc = OrientDBUtilsDoc.queryDoc(connectionPool, "SELECT distinct(topic) as topic FROM alldoc")
    var arrTopic: ListBuffer[String] = ListBuffer()
    for (i <- 0 until arrDoc.size) {
      val topic = arrDoc.get(i).field("topic").toString
      arrTopic.append(topic)
    }
    arrTopic.toList
  }

  def createCoocurrenceGraphSetFromOrientDB(strTopic: String): RDD[Graph] = {
    val OrientDBUtilsDoc = new OrientDBUtils(Config.hostType, Config.hostAddress, Config.database, Config.dbUser, Config.dbPassword, Config.userRoot, Config.pwdRoot)
    val connectionPool = OrientDBUtilsDoc.connectDBUsingDocAPI
    val arrDoc = OrientDBUtilsDoc.queryDoc(connectionPool, "SELECT content FROM alldoc WHERE topic = \"" + strTopic + "\"")
    var arrGraph: ListBuffer[Graph] = ListBuffer()
    for (i <- 0 until arrDoc.size) {
      val arr = arrDoc.get(i).field("content").toString.split(Properties.lineSeparator)
      var graph = new Graph()
      for (i <- 0 to arr.length - 2) {
        graph.addOrUpdateVertex(arr(i), arr(i + 1))
      }
      arrGraph.append(graph)
    }
    Config.sparkContext.parallelize(arrGraph)
  }

  def createCoocurrenceGraph(filePath: String): Graph = {
    val arrLine = HDFSReader.hdfsFileReader(filePath).collect()
    var graph = new Graph()
    for (i <- 0 to arrLine.length - 2) {
      graph.addOrUpdateVertex(arrLine(i), arrLine(i + 1))
    }
    graph
  }

  def printTenGraphs(graphs: RDD[Graph]) = {
    val graphCol = graphs.collect

    for (i <- 0 to 9) {
      val g = graphCol(i)
      g.Graph.foreach(r => {
        println("Đỉnh: " + r._1.toString())
        println("Kết nối với các đỉnh khác: " + r._2.mkString(", "))
      })
    }
  }
}