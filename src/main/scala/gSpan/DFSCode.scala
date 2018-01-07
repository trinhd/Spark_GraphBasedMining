package main.scala.gSpan

import scala.collection.mutable.Map
import main.scala.CoocurrenceGraph.Graph

class DFSCode(val arrEdgeCode: Array[EdgeCode], val graphSet: List[(Int, Map[Int, Int])], val support: Int) extends Serializable

class FinalDFSCode(val arrEdgeCode: Array[EdgeCode], val support: Int) extends Serializable {
  def extractInfo(index: Int, verticesMapping: Array[(String, Int, Int)]): String = {
    var sInfo = ""
    var sVerTemp = ""
    arrEdgeCode.foreach(ec => {
      if (sInfo.equals("")) {
        sInfo += "Đồ thị con số " + index + ":\n"
        sVerTemp = verticesMapping.find(_._3 == ec.lbTo).get._1
        sInfo += verticesMapping.find(_._3 == ec.lbFrom).get._1 + " ==> " + sVerTemp
      } else {
        val sVerStart = verticesMapping.find(_._3 == ec.lbFrom).get._1
        val sVerEnd = verticesMapping.find(_._3 == ec.lbTo).get._1
        if (sVerStart.equals(sVerTemp))
          sInfo += " ==> " + sVerEnd
        else sInfo += ", " + sVerStart + " ==> " + sVerEnd
        sVerTemp = sVerEnd
      }
    })
    sInfo += "\nVới độ phổ biến là: " + support
    sInfo
  }

  def extractGraph(verticesMapping: Array[(String, Int, Int)]) = {
    var graph = new Graph
    arrEdgeCode.foreach(ec => {
      val sVerStart = verticesMapping.find(_._3 == ec.lbFrom).get._1
      val sVerEnd = verticesMapping.find(_._3 == ec.lbTo).get._1
      graph.addOrUpdateVertex(sVerStart, sVerEnd)
    })
    graph
  }
}