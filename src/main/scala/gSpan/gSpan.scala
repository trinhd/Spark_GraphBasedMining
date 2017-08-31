package main.scala.gSpan

import scala.collection.mutable.ListBuffer

import org.apache.spark.rdd.RDD

import main.scala.Configuration.Config
import main.scala.CoocurrenceGraph.Graph

class gSpan {
  /**
   * Hàm khai phá đồ thị con phổ biến theo thuật toán gSpan
   * @param rddGraphs: danh sách tất cả đồ thị trên RDD
   * @return danh sách đồ thị con phổ biến và danh sách đỉnh phổ biến đã gán nhãn, dùng để tra cứu từ nhãn ngược lại tên đỉnh
   */
  def frequentSubgraphMining(rddGraphs: RDD[Graph]): (ListBuffer[FinalDFSCode], Array[(String, Int, Int)]) = {
    val graphCount = rddGraphs.count().toDouble
    val minSupInt = math.ceil((graphCount * Config.minSupport)).toInt
    println("---------INPUT---------")
    println("Number of graphs: " + graphCount)
    println("Minimum Support: " + Config.minSupport)
    println("MinSup Integer: " + minSupInt)
    //return (null, null)
    //println("Đồ thị ban đầu:")
    //println(rddGraphs.map(g => g.printGraph()).collect().mkString("\n"))

    val rddGraphsIndexed = rddGraphs.zipWithIndex().map(g => (g._1, g._2.toInt))

    val graphBuilder = new GraphBuilder()

    val s = new ListBuffer[FinalDFSCode]
    val frequentVertices = graphBuilder.filterFrequentVertex(rddGraphs, minSupInt).collect()

    if (graphCount == 1d) {
      (new ListBuffer[FinalDFSCode], frequentVertices)
    } else {
      if (!frequentVertices.isEmpty) {
        val (reconstructedGraph, frequentEdges) = graphBuilder.reconstructGraphSet(rddGraphsIndexed, Config.sparkContext.broadcast(frequentVertices.map(v => { (v._1, v._3) })))
        reconstructedGraph.persist(Config.defaultStorageLevel)

        /*reconstructedGraph.foreach(g => {
      println("Đồ thị: " + g._1)
      println("Đỉnh: " + g._2.map(v => frequentVertices.find(_._2 == v).get._1).mkString(", "))
      println("Cạnh: " + g._3.map(tuple => frequentVertices.find(_._2 == tuple._1).get._1 + " => " + frequentVertices.find(_._2 == tuple._2).get._1).mkString(", "))
    	})
    	println("Cạnh phổ biến trong tập đồ thị là: ")
    	println(frequentEdges.map(e => frequentVertices.find(_._2 == e._1).get._1 + " => " + frequentVertices.find(_._2 == e._2).get._1).mkString("\n"))*/

        val S1 = graphBuilder.buildOneEdgeCode(Config.sparkContext.parallelize(frequentEdges)).sortBy(_.lbFrom, true).collect()
        val reconstructedGraphSet = reconstructedGraph.collect()

        for (edgeCode <- S1) {
          val dfsGraphSet = graphBuilder.projectWithOneEdge(reconstructedGraph, Config.sparkContext.broadcast(edgeCode))
          dfsGraphSet.persist(Config.defaultStorageLevel)

          val support = dfsGraphSet.map(_._1).distinct.count.toInt

          val dfsCode = new DFSCode(Array(edgeCode), dfsGraphSet.collect().toList, support)
          dfsGraphSet.unpersist()

          var graphSet = reconstructedGraphSet.clone()

          graphBuilder.subgraphMining(graphSet, s, dfsCode, minSupInt)

          graphSet = graphBuilder.shrink(Config.sparkContext.parallelize(graphSet), Config.sparkContext.broadcast(edgeCode)).collect()
        }

        (s, frequentVertices)
      } else {
        //println("Dinh pho bien rong")
        (new ListBuffer[FinalDFSCode], Array[(String, Int, Int)]())
      }
    }
  }
}