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
}