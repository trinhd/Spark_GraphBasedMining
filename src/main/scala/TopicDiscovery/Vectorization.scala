package main.scala.TopicDiscovery

import main.scala.Input.HDFSReader
import scala.collection.mutable.ArrayBuffer
import main.scala.Output.OutputtoHDFS
import scala.Array
import main.scala.CoocurrenceGraph.Graph
import scala.collection.mutable.HashMap
import util.control.Breaks._

class Vectorization {
  def createDictionary(folderPath: String, maxDimension: Int, outputPath: String) = {
    //maxDimension: 0 - ko lay word, -1 - lay het toan bo word, >0 - lay theo do
    var sFinalRes = "File ID:\n"
    val rddDoc = HDFSReader.hdfsFolderReader(folderPath).zipWithIndex()
    val rddDic = rddDoc.flatMap {
      case ((link, doc), id) => {
        var wordNo, graphNo, remainWords = 0
        var arrTemp = ArrayBuffer[(Long, String)]()
        val arrLine = doc.split("\n")
        var i = 0
        while (i < arrLine.length) {
          if (i == 1) wordNo = arrLine(i).split(" ")(3).trim.toInt
          if (i == 2) {
            graphNo = arrLine(i).split(" ")(1).trim.toInt
            if (maxDimension > 0 && maxDimension > graphNo) {
              remainWords = maxDimension - graphNo
            } else {
              remainWords = 0
            }
          }
          if (arrLine(i).contains("Các đỉnh phổ biến là:")) {
            i = i + 1
            while ((i < arrLine.length) && !arrLine(i).contains("Các đồ thị con phổ biến là:")) {
              if (remainWords > 0) {
                arrTemp += ((id, arrLine(i).split("::")(0).trim))
                remainWords -= 1
              }
              else if (maxDimension == -1) {
                arrTemp += ((id, arrLine(i).split("::")(0).trim))
              }
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

  def readDictionary(filePath: String): (HashMap[Int, String], HashMap[Int, Graph], HashMap[Int, Array[Boolean]]) = {
    val arrDicFile = HDFSReader.hdfsFileReader(filePath).collect()
    var mTopic = HashMap[Int, String]()
    var mDimension = HashMap[Int, Graph]()
    var mTopicVector = HashMap[Int, Array[Boolean]]()
    var i = 0
    while (i < arrDicFile.length) {
      if (arrDicFile(i) contains ("File ID:")) {
        i = i + 1
        while (i < arrDicFile.length && !arrDicFile(i).contains("Dimension ID:")) {
          if (arrDicFile(i) contains (" ==> ")) {
            val arrTemp = arrDicFile(i).split("==>").map(_.trim)
            mTopic += ((arrTemp(0).toInt, arrTemp(1)))
          }
          i = i + 1
        }
        if (arrDicFile(i) contains ("Dimension ID:")) {
          i = i + 1
          while (i < arrDicFile.length && !arrDicFile(i).contains("Topic Vector:")) {
            if (arrDicFile(i) contains (" || ")) {
              val gr = new Graph
              val arrTemp2 = arrDicFile(i).split(s"\\|\\|").map(_.trim)
              if (arrTemp2(2) contains ("==>")) {
                var arrBranch = arrTemp2(2).split("::").map(_.trim)
                var mTemp = HashMap[String, ArrayBuffer[String]]()
                arrBranch.foreach(br => {
                  var lr = br.split("==>").map(_.trim)
                  if (lr(1).equals("{}")) {
                    mTemp += ((lr(0), ArrayBuffer[String]()))
                  } else {
                    var r = ArrayBuffer[String]()
                    r ++= lr(1).split(",").map(_.trim)
                    mTemp += ((lr(0), r))
                  }
                })
                gr.addGraphDirect(mTemp)
              } else {
                gr.addGraphOneVertex(arrTemp2(2))
              }
              mDimension += ((arrTemp2(0).toInt, gr))
            }
            i = i + 1
          }
          if (arrDicFile(i) contains ("Topic Vector:")) {
            i = i + 1
            while (i < arrDicFile.length) {
              if (arrDicFile(i) contains (" ==> ")) {
                val arrTemp3 = arrDicFile(i).split("==>").map(_.trim)
                val arrRight = arrTemp3(1).split(",").map(_.trim)
                var arrVector = new Array[Boolean](arrRight.length)
                for (i <- 0 until arrRight.length) {
                  if (arrRight(i).equals("0")) arrVector(i) = false
                  else arrVector(i) = true
                }
                mTopicVector += ((arrTemp3(0).toInt, arrVector))
              }
              i = i + 1
            }
          }
        }
      }
      i = i + 1
    }
    (mTopic, mDimension, mTopicVector)
  }

  def createVector(graph: Graph, mDimension: HashMap[Int, Graph]): Array[Boolean] = {
    var vector = new Array[Boolean](mDimension.size)
    mDimension.foreach(e => {
      if (graphContain(graph, e._2)) vector(e._1) = true
      else vector(e._1) = false
    })
    vector
  }

  def graphContain(parent: Graph, child: Graph): Boolean = {
    val childGraph = child.Graph
    val parentGraph = parent.Graph
    var fRes = true
    breakable {
      childGraph.foreach(e => {
        val vertex = parentGraph.find(_._1.equals(e._1)).getOrElse(null)
        if (vertex == null) {
          fRes = false
          break
        }
        e._2.foreach(v => {
          if (!vertex._2.contains(v)) {
            fRes = false
            break
          }
        })
      })
    }
    fRes
  }
}