package main.scala.gSpan

import scala.collection.mutable.Map

class DFSCode(val arrEdgeCode: Array[EdgeCode], val graphSet: List[(Int, Map[Int, Int])], val support: Int) extends Serializable

class FinalDFSCode(val arrEdgeCode: Array[EdgeCode], val support: Int) extends Serializable {
  def extractInfo(index: Int, verticesMapping: Array[(String, Int)]): String = {
    var sInfo = ""
    var sVerTemp = ""
    arrEdgeCode.foreach(ec => {
      if (sInfo.equals("")) {
        sInfo += "Đồ thị con số " + index + ":\n"
        sVerTemp = verticesMapping.find(_._2.equals(ec.lbTo)).get._1
        sInfo += verticesMapping.find(_._2.equals(ec.lbFrom)).get._1 + " ==> " + sVerTemp
      } else {
        val sVerStart = verticesMapping.find(_._2.equals(ec.lbFrom)).get._1
        val sVerEnd = verticesMapping.find(_._2.equals(ec.lbTo)).get._1
        if (sVerStart.equals(sVerTemp))
          sInfo += " ==> " + sVerEnd
        else sInfo += ", " +sVerStart + " ==> " + sVerEnd
        sVerTemp = sVerEnd
      }
    })
    sInfo += "\nVới độ phổ biến là: " + support
    sInfo
  }
}