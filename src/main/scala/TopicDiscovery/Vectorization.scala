package main.scala.TopicDiscovery

import main.scala.Input.HDFSReader
import scala.collection.mutable.ArrayBuffer
import main.scala.Output.OutputtoHDFS
import scala.Array

class Vectorization {
  def createDictionary(folderPath: String, outputPath: String) = {
    var sFinalRes = "File ID:\n"
    val rddDoc = HDFSReader.hdfsReader(folderPath).zipWithIndex()
    val rddDic = rddDoc.flatMap {
      case ((link, doc), id) => {
        var arrTemp = ArrayBuffer[(Long, String)]()
        val arrLine = doc.split("\n")
        var i = 0
        while (i < arrLine.length) {
          if (arrLine(i).contains("Các đỉnh phổ biến là:")) {
            i = i + 1
            while ((i < arrLine.length) && !arrLine(i).contains("Các đồ thị con phổ biến là:")) {
              arrTemp += ((id, arrLine(i)))
              i = i + 1
            }
            if ((i < arrLine.length) && arrLine(i).contains("Các đồ thị con phổ biến là:")) {
              i = i + 1
              var fNewGraph = true
              var sGraph = ""
              while (i < arrLine.length) {
                if (arrLine(i) contains "Đồ thị con số ") {
                  if (!fNewGraph) {
                    arrTemp += ((id, sGraph))
                  }
                  fNewGraph = true
                  sGraph = ""
                }
                if (arrLine(i) contains "==>") {
                  if (!sGraph.equals("")) sGraph += " :: " + arrLine(i) else sGraph = arrLine(i)
                  fNewGraph = false
                }
                i = i + 1
                if (i == arrLine.length) {
                  arrTemp += ((id, sGraph))
                }
              }
            }
          }
          i = i + 1
        }
        arrTemp
      }
    }.sortBy(_._1, true).zipWithIndex()
    
    val vectorLength = rddDic.count()
    
    val rddVector = rddDic.map(e => (e._1._1, e._2)).groupByKey().map(e => {
      var min = Long.MaxValue
      e._2.foreach(l => {
        if (l < min) min = l
      })
      var vector = Array.fill(min.toInt)(0) ++ Array.fill(e._2.size)(1) ++ Array.fill(vectorLength.toInt - min.toInt - e._2.size)(0)
      var sVector = e._1 + " ==> " + vector.mkString(", ")
      sVector
    })
    
    sFinalRes += rddDoc.map(e => e._2 + " ==> " + e._1._1).collect().mkString("\n") + "\n"
    sFinalRes += "Dimension ID:\n" + rddDic.map(e => e._2 + " || " + e._1._1 + " || " + e._1._2).collect().mkString("\n") + "\n"
    sFinalRes += "Topic Vector:\n" + rddVector.collect().mkString("\n")
    
    if (OutputtoHDFS.writeFile(outputPath, sFinalRes)) println("Kết quả tính được ghi thành công xuống tập tin " + outputPath)
    println(sFinalRes)
  }
}