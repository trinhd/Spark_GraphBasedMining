package main.scala.GraphCharacteristic

import main.scala.CoocurrenceGraph.Graph
import main.scala.Configuration.Config
import main.scala.gSpan.gSpan
import main.scala.Input.HDFSReader
import main.scala.Output.OutputtoHDFS
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import main.scala.OrientDBUtils

class CharacteristicExtract {
  def characteristicExtract(folderPath: String, outputPath: String) = {
    val rddDoc = HDFSReader.hdfsFolderReader(folderPath)
    val arrFreq = rddDoc.map {
      case (link, doc) => {
        val output_link = outputPath + "/" + link.split("/").last + "_filted"
        var arrOne = ArrayBuffer[(String, Int)]()
        var arrGraph = ArrayBuffer[Graph]()
        val arrLine = doc.split("\n")
        var i = 0
        while (i < arrLine.length - 4) {
          if (arrLine(i).contains("Các đỉnh phổ biến là:")) {
            i = i + 1
            while (!arrLine(i).contains("Các đồ thị con phổ biến là:") && (i < arrLine.length - 4)) {
              val arrLineTemp = arrLine(i).split("::")
              arrOne += ((arrLineTemp(0).trim, arrLineTemp(1).trim.toInt))
              i = i + 1
            }
            if (arrLine(i).contains("Các đồ thị con phổ biến là:")) {
              i = i + 1
              var fNewGraph = true
              var gr: Graph = null
              while (i < arrLine.length - 4) {
                if (arrLine(i) contains "Đồ thị con số ") {
                  if (!fNewGraph) {
                    arrGraph += gr
                  }
                  fNewGraph = true
                  gr = new Graph
                }
                if (arrLine(i) contains "==>") {
                  val arrBranch = arrLine(i).split(",")
                  fNewGraph = false
                  for (branch <- arrBranch) {
                    val arrTemp = branch.split("==>").map(_.trim)
                    var j = 0
                    while (j < arrTemp.length - 1) {
                      gr.addOrUpdateVertex(arrTemp(j), arrTemp(j + 1))
                      j = j + 1
                    }
                  }
                }
                i = i + 1
                if (i == arrLine.length - 5) {
                  arrGraph += gr
                  i = i + 1
                }
              }
            }
          }
          i = i + 1
        }
        (output_link, arrOne, arrGraph)
      }
    }.collect()

    var arrFinalRes = new Array[(String, ArrayBuffer[(String, Int)], ArrayBuffer[Graph])](arrFreq.length)

    var arrMatrixRes = ListBuffer[(Int, Int, Int, Int, Double)]()

    for (i <- 0 until arrFreq.length) {
      var arrOne = arrFreq(i)._2
      var arrGraph = arrFreq(i)._3.map((_, 1d)) //arrFreq(i)._3
      for (j <- 0 until arrFreq.length) {
        if (i != j) {
          val arrOneTemp = arrFreq(j)._2.map(_._1)
          arrOne = arrOne.filterNot(p => {
            arrOneTemp.contains(p._1)
          })

          var arrGraphCompute = arrFreq(j)._3
          for (x <- 0 until arrGraph.length) {
            for (y <- 0 until arrGraphCompute.length) {
              var distance = 1d
              if (!arrMatrixRes.exists(e => (e._1 == j && e._2 == i))) {
                distance = graphDistance(arrGraph(x)._1, arrGraphCompute(y))
                arrMatrixRes += ((i, j, x, y, distance))
              } else {
                distance = arrMatrixRes.find(e => (e._1 == j && e._2 == i && e._3 == y && e._4 == x)).get._5
              }
              if (distance < arrGraph(x)._2) {
                arrGraph(x) = (arrGraph(x)._1, distance)
              }
            }
          }
        }
      }

      arrFinalRes(i) = (arrFreq(i)._1, arrOne, arrGraph.filter(_._2 > Config.minDistance).map(_._1))
    }

    for (t <- arrFinalRes) {
      var s = ((t._2.length + t._3.length)) + " đồ thị con phổ biến.\n"
      s += "Trong đó có " + t._2.length + " đỉnh phổ biến.\n"
      if (!t._3.isEmpty) s += "Và " + t._3.length + " đồ thị con phổ biến được tạo thành từ ít nhất một cạnh.\n"
      s += "Các đỉnh phổ biến là:\n"
      s += t._2.map(f => f._1 + " :: " + f._2).mkString("\n")
      if (!t._3.isEmpty) {
        s += "\nCác đồ thị con phổ biến là:"
        var n = 0
        while (n < t._3.length) {
          s += "\nĐồ thị con số " + n + ":\n"
          s += t._3(n).printGraphMatrix()
          n = n + 1
        }
      }
      if (OutputtoHDFS.writeFile(t._1, s)) println("Kết quả tính được ghi thành công xuống tập tin " + t._1)
    }
  }
  
  def characteristicExtractCont(arrFreq: ListBuffer[(String, Array[(String, Int)], Array[(Graph, Int)])]) = {
    var arrFinalRes = new Array[(String, Array[(String, Int)], Array[(Graph, Int)])](arrFreq.length)

    var arrMatrixRes = ListBuffer[(Int, Int, Int, Int, Double)]()

    for (i <- 0 until arrFreq.length) {
      var arrOne = arrFreq(i)._2
      var arrGraph = arrFreq(i)._3.map((_, 1d)) //arrFreq(i)._3
      for (j <- 0 until arrFreq.length) {
        if (i != j) {
          val arrOneTemp = arrFreq(j)._2.map(_._1)
          arrOne = arrOne.filterNot(p => {
            arrOneTemp.contains(p._1)
          })

          var arrGraphCompute = arrFreq(j)._3
          for (x <- 0 until arrGraph.length) {
            for (y <- 0 until arrGraphCompute.length) {
              var distance = 1d
              if (!arrMatrixRes.exists(e => (e._1 == j && e._2 == i))) {
                distance = graphDistance(arrGraph(x)._1._1, arrGraphCompute(y)._1)
                arrMatrixRes += ((i, j, x, y, distance))
              } else {
                distance = arrMatrixRes.find(e => (e._1 == j && e._2 == i && e._3 == y && e._4 == x)).get._5
              }
              if (distance < arrGraph(x)._2) {
                arrGraph(x) = (arrGraph(x)._1, distance)
              }
            }
          }
        }
      }

      arrFinalRes(i) = (arrFreq(i)._1, arrOne, arrGraph.filter(_._2 > Config.minDistance).map(_._1))
    }

    /*for (t <- arrFinalRes) {
      var s = ((t._2.length + t._3.length)) + " đồ thị con phổ biến.\n"
      s += "Trong đó có " + t._2.length + " đỉnh phổ biến.\n"
      if (!t._3.isEmpty) s += "Và " + t._3.length + " đồ thị con phổ biến được tạo thành từ ít nhất một cạnh.\n"
      s += "Các đỉnh phổ biến là:\n"
      s += t._2.map(f => f._1 + " :: " + f._2).mkString("\n")
      if (!t._3.isEmpty) {
        s += "\nCác đồ thị con phổ biến là:"
        var n = 0
        while (n < t._3.length) {
          s += "\nĐồ thị con số " + n + ":\n"
          s += t._3(n).printGraphMatrix()
          n = n + 1
        }
      }
      if (OutputtoHDFS.writeFile(t._1, s)) println("Kết quả tính được ghi thành công xuống tập tin " + t._1)
    }*/
    
    val OrientDBUtilsGraph = new OrientDBUtils(Config.hostType, Config.hostAddress, Config.database, Config.dbUser, Config.dbPassword)
    val factory = OrientDBUtilsGraph.connectDBUsingGraphAPI
    arrFinalRes.foreach(res =>{
      res._2.foreach(one => {
        OrientDBUtilsGraph.insertVertex(factory, res._1, one._2, one._1)
      })
      res._3.foreach { case (gr, freq) => {
        val vTemp = gr.Graph.keys.map(v => ((v, freq), res._1)).toMap[(String, Int), String]
        val verDic = OrientDBUtilsGraph.insertVertexInBatches(factory, vTemp)
        gr.Graph.foreach(v => {
          val verStart = verDic.find(p => p._1 == v._1).get._4
          v._2.foreach(end => {
            val verEnd = verDic.find(p => p._1 == end).get._4
            OrientDBUtilsGraph.insertEdge(factory, verStart, verEnd)
          })
        })
      }}
    })
    factory.close
  }

  def graphSize(graph: Graph): Int = {
    var size = graph.Graph.size
    for (row <- graph.Graph) {
      size += row._2.length
    }
    size
  }

  def maximalCommonSubgraph(graph1: Graph, graph2: Graph): Int = {
    //var grRes = new Graph
    var maxSize = 0
    val rddGraphs = Config.sparkContext.parallelize(List(graph1, graph2))
    val minSupBK = Config.minSupport
    Config.minSupport = 1d

    val gspan = new gSpan
    val (s, frequentVertices) = gspan.frequentSubgraphMining(rddGraphs)

    if (!frequentVertices.isEmpty) {
      if (!s.isEmpty) {
        for (gr <- s) {
          var grTemp = new Graph
          for (ec <- gr.arrEdgeCode) {
            grTemp.addOrUpdateVertex(frequentVertices.find(_._3 == ec.lbFrom).get._1, frequentVertices.find(_._3 == ec.lbTo).get._1)
          }

          var grTempSize = graphSize(grTemp)
          if (grTempSize > maxSize) {
            //grRes = grTemp
            maxSize = grTempSize
          }
        }
      } else {
        maxSize = 1
      }
    }

    Config.minSupport = minSupBK

    //grRes
    maxSize
  }

  def graphDistance(graph1: Graph, graph2: Graph): Double = {
    val graphSize1 = graphSize(graph1).toDouble
    val graphSize2 = graphSize(graph2).toDouble
    val mcs = maximalCommonSubgraph(graph1, graph2)
    val graphSizeMCS = mcs.toDouble //if (mcs.Graph.isEmpty) 0d else graphSize(mcs).toDouble

    1d - (graphSizeMCS / (if (graphSize1 > graphSize2) graphSize1 else graphSize2))
  }
}