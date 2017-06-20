package main.scala.CoocurrenceGraph

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.util.Properties
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.rdd.RDD

import main.scala.Input.HDFSReader

class TestGraph {
  def createTestGraphSet(folderPath: String): RDD[Graph] = {
    val rddDoc = HDFSReader.hdfsReader(folderPath)
    val rddGraph = rddDoc.flatMap(doc => {
      val tempGraphList = ListBuffer[Graph]()
      val arr = doc._2.split(Properties.lineSeparator)
      var graph: Map[String, ArrayBuffer[String]] = null
      var mapVertices = Map[String, String]()
      breakable(
        for (i <- 0 to arr.length - 1) {
          val items = arr(i).split(" ")
          val lineType = items(0)
          lineType match {
            case "t" => {
              // New Graph
              if (items(2).equals("-1")) break
              if (graph != null) {
                val temp = new Graph()
                temp.addGraphTest(graph)
                tempGraphList += temp
              }
              graph = Map[String, ArrayBuffer[String]]()
              mapVertices = mapVertices.empty
            }
            case "v" => {
              val vertexID = items(1)
              val vertexLabel = items(2)
              graph += (vertexLabel -> new ArrayBuffer[String]())
              mapVertices += (vertexID -> vertexLabel)
            }
            case "e" => {
              val fromId = items(1)
              val toId = items(2)
              val fromLabel = mapVertices.get(fromId).get
              val toLabel = mapVertices.get(toId).get
              graph.get(fromLabel).get += toLabel
            }
          }
        })
      val temp = new Graph()
      temp.addGraphTest(graph)
      tempGraphList += temp
      tempGraphList
    })
    rddGraph
  }
}