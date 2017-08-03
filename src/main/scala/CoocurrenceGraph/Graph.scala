package main.scala.CoocurrenceGraph

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap

class Graph extends Serializable {
  var Graph: Map[String, ArrayBuffer[String]] = new HashMap()

  def addOrUpdateVertex(vertexName1: String, vertexName2: String) = {
    val arrTemp1 = Graph.get(vertexName1)
    val arrTemp2 = Graph.get(vertexName2)

    var arrTemp3 = ArrayBuffer[String]()
    if (arrTemp1 != None) {
      arrTemp3 = arrTemp1.get
      if (!arrTemp3.contains(vertexName2)) {
        arrTemp3 += vertexName2
        Graph.put(vertexName1, arrTemp3)
      }
    } else {
      arrTemp3 = ArrayBuffer[String](vertexName2)
      Graph.put(vertexName1, arrTemp3)
    }

    /*var arrTemp4 = ArrayBuffer[String]()
    if (arrTemp2 != None) {
      arrTemp4 = arrTemp2.get
      if (!arrTemp4.contains(vertexName1)) arrTemp4 += vertexName1
    } else arrTemp4 = ArrayBuffer[String](vertexName1)

    Graph.put(vertexName2, arrTemp4)*/

    if (arrTemp2 == None) {
      var arrTemp4 = ArrayBuffer[String]()
      Graph.put(vertexName2, arrTemp4)
    }
  }

  def printGraph(): String = {
    var sRes = ""
    Graph.foreach(g => {
      sRes += "Đỉnh: " + g._1.toString() + "\n"
      sRes += "Kết nối với các đỉnh khác: " + g._2.mkString(", ") + "\n"
    })
    sRes
  }

  def printGraphMini(): String = {
    var graphTemp = Graph
    var sRes = ""
    var arrTemp = Array[String]()
    var arrHis = Array[(String, Int)]()
    sRes += graphTemp.head._1
    arrTemp :+ graphTemp.head._2
    arrHis :+ (graphTemp.head._1, graphTemp.head._2.length)
    graphTemp.remove(graphTemp.head._1)
    while (!arrTemp.isEmpty) {
      if (arrHis.last._2 != 0) {
        arrHis(arrHis.length - 1) = (arrHis.last._1, arrHis.last._2 - 1)
      } else {
        arrHis = arrHis.take(arrHis.length - 1)
        sRes += ", " + arrHis.last._1
      }
      val te = arrTemp.last
      arrTemp = arrTemp.take(arrTemp.length - 1)
      sRes += " ==> " + te
      val children = graphTemp.get(te).get
      graphTemp.remove(te)
      if (children != None) {
        arrTemp :+ children
        arrHis :+ (te, children.length)
      } else {
        arrHis :+ (te, 0)
      }
    }
    sRes
  }

  def jsonGraph(): String = {
    var sRes = "{\n\"nodes\": [\n"
    var sLinks = "\n],\n\"links\": [\n"
    Graph.foreach(v => {
      sRes += "{\"id\": \"" + v._1.toString() + "\"},\n"
      v._2.foreach { des => sLinks += "{\"source\": \"" + v._1.toString() + "\", \"target\": \"" + des.toString() + "\"},\n" }
    })
    sRes = sRes.slice(0, sRes.lastIndexOf("\n") - 1);
    sLinks = sLinks.slice(0, sLinks.lastIndexOf("\n") - 1);
    sRes += sLinks + "\n]\n}"
    sRes
  }

  def addGraphTest(graph: Map[String, ArrayBuffer[String]]) = {
    Graph = graph
  }
}