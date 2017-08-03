package main.scala.CharacteristicGraph

import main.scala.CoocurrenceGraph.Graph
import main.scala.Configuration.Config
import main.scala.gSpan.gSpan
import main.scala.Input.HDFSReader
import main.scala.Output.OutputtoHDFS

class CharacteristicExtract {
  def characteristicExtract(folderPath: String, outputPath: String) = {
    val rddDoc = HDFSReader.hdfsReader(folderPath)
    val arrFreq = rddDoc.map {
      case (link, doc) => {
        val output_link = outputPath + "/" + link.split("/").last + "_filted"
        var arrOne = Array[String]()
        var arrGraph = Array[Graph]()
        val arrLine = doc.split("\n")
        var i = 0
        while (i < arrLine.length - 4) {
          if (arrLine(i).equals("Các đỉnh phổ biến là:")) {
            i = i + 1
            while (!arrLine(i).equals("Các đồ thị con phổ biến là:") && (i < arrLine.length - 4)) {
              arrOne :+ arrLine(i)
              i = i + 1
            }
            if (arrLine(i).equals("Các đồ thị con phổ biến là:")) {
              i = i + 2
              while (i < arrLine.length - 5) {
                if (arrLine(i).contains("==>")) {
                  var gr = new Graph
                  val arrTemp = arrLine(i).split("==>").map(_.trim)
                  var j = 0
                  while (j < arrTemp.length - 2) {
                    gr.addOrUpdateVertex(arrTemp(j), arrTemp(j + 1))
                    j = j + 1
                  }
                  arrGraph :+ gr
                }
                i = i + 3
              }
            }
          }
          i = i + 1
        }
        (output_link, arrOne, arrGraph)
      }
    }.collect()
    
    var arrFinalRes = new Array[(String, Array[String], Array[Graph])](arrFreq.length)

    //var arrMatrixRes = Array[(Int, Int, Int, Int, Double)]()
    var i = 0
    while (i < arrFreq.length - 1) {
      var arrOne = arrFreq(i)._2
      var arrGraph = arrFreq(i)._3.map((_, 1d)) //arrFreq(i)._3
      var j = 0
      while (j < arrFreq.length - 1) {
        if (i != j) {
          arrOne = arrOne.diff(arrFreq(j)._2)
          /*arrFinalRes(i) = (arrFreq(i)._1, arrOne, null)
          
          var arrGraphCompute = arrFreq(j)._3
          var x, y = 0
          while (x < arrGraph.length - 1) {
            while (y < arrGraphCompute.length - 1) {
              if (!arrMatrixRes.exists(_._1 == j) || !arrMatrixRes.exists(_._2 == i) || !arrMatrixRes.exists(_._3 == y) || !arrMatrixRes.exists(_._4 == x)) {
                arrMatrixRes :+ (i, j, x, y, graphDistance(arrGraph(x), arrGraphCompute(y)))
              }
            }
          }*/
          
          var arrGraphCompute = arrFreq(j)._3
          var x, y = 0
          while (x < arrGraph.length - 1) {
            while (y < arrGraphCompute.length - 1) {
              val distance = graphDistance(arrGraph(x)._1, arrGraphCompute(y))
              if (distance < arrGraph(x)._2) {
                arrGraph(x) = (arrGraph(x)._1, distance)
              }
            }
          }
        }
        j = j + 1
      }
      arrFinalRes(i) = (arrFreq(i)._1, arrOne, arrGraph.filter(_._2 > Config.minDistance).map(_._1))
      i = i + 1
    }
    
    /*i = 0
    while (i < arrFreq.length - 1){
      i = i + 1
    }*/
    
    for (t <- arrFinalRes){
      var s = (t._2.length + t._3.length) + " đồ thị con phổ biến.\n"
      s += "Trong đó có "+ t._2.length +" đỉnh phổ biến.\n"
      if (!t._3.isEmpty) s += "Và "+ t._3.length +" đồ thị con phổ biến được tạo thành từ ít nhất một cạnh.\n"
      s += "Các đỉnh phổ biến là:\n"
      s += t._2.mkString("\n")
      if (!t._3.isEmpty){
        s += "\nCác đồ thị con phổ biến là:"
        var n = 0
        while (n < t._3.length - 1){
          s += "\nĐồ thị con số "+ n +":\n"
          s += t._3(n).printGraphMini()
          n = n + 1
        }
      }
      if (OutputtoHDFS.writeFile(t._1, s)) println("Kết quả tính được ghi thành công xuống tập tin " + t._1)
    }
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
            grTemp.addOrUpdateVertex(frequentVertices.find(_._2 == ec.lbFrom).get._1, frequentVertices.find(_._2 == ec.lbTo).get._1)
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