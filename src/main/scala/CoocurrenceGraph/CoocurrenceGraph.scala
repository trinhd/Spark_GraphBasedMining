package main.scala.CoocurrenceGraph

import scala.util.Properties

import main.scala.Input.HDFSReader
import main.scala.Output.OutputFileWriter
import org.apache.spark.rdd.RDD

class CoocurrenceGraph {
  def createCoocurrenceGraphSet(folderPath: String): RDD[Graph] = {
    val rddDoc = HDFSReader.hdfsReader(folderPath)
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