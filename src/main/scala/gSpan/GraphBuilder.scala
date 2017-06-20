package main.scala.gSpan

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.Queue
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import main.scala.Configuration.Config
import main.scala.CoocurrenceGraph.Graph

class GraphBuilder extends Serializable {
  /*
  /**
   * Hàm lọc bỏ những cạnh không phổ biến
   * @param graphs: tập các ma trận kề của đồ thị đồng hiện đã tạo
   * @param minSup: ngưỡng độ hỗ trợ nhỏ nhất
   * @return Danh sách cạnh đã lọc kèm theo index của nó theo thứ tự frequency giảm dần
   */
  def filterFrequentEdge(graphs: RDD[Graph], minSup: Int): RDD[(Edge, Int)] = {
    var rddEdgeIndexed = graphs.flatMap(g => {
      var listEdge: ListBuffer[Edge] = ListBuffer()
      for (entry <- g.Graph) {
        for (end <- entry._2) {
          val edge = new Edge(entry._1, end)
          if (!listEdge.contains(edge)) listEdge += edge
        }
      }
      listEdge
    }).map(e => (e, 1)).reduceByKey(_ + _).filter(count => count._2 >= minSup).sortBy(-_._2).zipWithIndex().map(e => (e._1._1, e._2.toInt))
    rddEdgeIndexed
  }
  */

  /**
   * Hàm lọc bỏ những đỉnh không phổ biến
   * @param graphs: tập các ma trận kề của đồ thị đồng hiện đã tạo
   * @param minSup: ngưỡng độ hỗ trợ nhỏ nhất
   * @return Danh sách đỉnh đã lọc kèm theo index của nó theo thứ tự frequency giảm dần
   */
  def filterFrequentVertex(graphs: RDD[Graph], minSup: Int): RDD[(String, Int)] = {
    var rddVetexIndexed = graphs.flatMap(g => g.Graph.keys).map(v => (v, 1)).reduceByKey(_ + _).filter(c => c._2 >= minSup).sortBy(-_._2).zipWithIndex().map(v => (v._1._1, v._2.toInt))
    rddVetexIndexed
  }

  /**
   * Hàm tạo lại tập đồ thị loại bỏ các thành phần không phổ biến và áp dụng nhãn đỉnh mới
   * @param graphs: tập đồ thị cần lọc bỏ thành phần, có gán sẵn ID cho từng đồ thị
   * @param vertices: tập đỉnh phổ biến đã gán nhãn theo thứ tự
   * @return Đồ thị đã lọc bỏ bao gồm (ID đồ thị gốc, tập đỉnh phổ biến theo nhãn, tập cạnh phổ biến theo nhãn) và tập cạnh phổ biến của toàn bộ graphset
   */
  def reconstructGraphSet(graphs: RDD[(Graph, Int)], vertices: Broadcast[Array[(String, Int)]]): (RDD[(Int, Array[Int], Array[(Int, Int)])], Array[(Int, Int)]) = {
    val rddNewGraphSet = graphs.map(g => {
      val frequentVertices = vertices.value
      var arrVertices = Array[Int]()
      var arrEdges = Array[(Int, Int)]()
      val graphTemp = g._1.Graph.filterKeys(v => frequentVertices.exists(_._1.equals(v)))
      if (graphTemp.isEmpty) (-1, null, null)
      else {
        var iCount = 0
        //println("Đồ thị: " + g._2)
        arrVertices = graphTemp.keys.map(v => frequentVertices.find(_._1.equals(v)).get._2).toArray
        arrEdges = graphTemp.flatMap(row => {
          //println("From Edge: " + row._1)
          val fromVerID = frequentVertices.find(_._1.equals(row._1)).get._2
          var arrEdge = ArrayBuffer[(Int,(Int, Int))]()
          //println("ROW._2: " + row._2.mkString(", "))
          for (v <- row._2) {
            //println("vertex: " + v)
            val toVer = frequentVertices.find(_._1.equals(v))
            if (toVer != None) {
              //println("ADDED!!!" + row._1 + " => " + v)
              arrEdge += ((iCount, (fromVerID, toVer.get._2)))
              //println("Count: "+ arrEdge.length)
              iCount += 1
            }
          }
          arrEdge
        }).toArray.map(_._2)
        (g._2, arrVertices, arrEdges)
      }
    }).filter(_._1 != -1)
    var oneEdge = rddNewGraphSet.flatMap(g => {
      g._3.distinct
    }).distinct().collect()
    (rddNewGraphSet, oneEdge)
  }

  /**
   * Hàm tạo mã cho 1-cạnh phổ biến
   * @param edges: tập cạnh phổ biến đã gán nhãn
   * @param vertices: tập đỉnh phổ biến đã gán nhãn
   * @return DFS Code cho đồ thị 1-cạnh phổ biến
   */
  def buildOneEdgeCode(edges: RDD[(Int, Int)]): RDD[EdgeCode] = {
    val rddOneEdge = edges.map(e => {
      new EdgeCode(0, 1, e._1, 0, e._2)
    })
    rddOneEdge
  }

  /**
   * Hàm chiếu edgecode vào tập đồ thị
   * @param graphSet: tập đồ thị đã đánh nhãn lại
   * @param edgeCode: mã cạnh cần lọc
   * @return danh sách đồ thị và map dùng tra cứu edgecode đầu vào ở đâu trong đồ thị
   */
  def projectWithOneEdge(graphSet: RDD[(Int, Array[Int], Array[(Int, Int)])], edgeCode: Broadcast[EdgeCode]): RDD[(Int, Map[Int, Int])] = {
    graphSet.flatMap(g => {
      val ec = edgeCode.value
      g._3.filter(e => (e._1, 0, e._2) == (ec.lbFrom, ec.lbEdge, ec.lbTo)).map(e => {
        (g._1, Map(ec.vFrom -> e._1, ec.vTo -> e._2))
      })
    })
  }

  /**
   * Hàm loại bỏ mã cạnh khỏi tập đồ thị
   * @param graphSet: tập đồ thị
   * @param edgeCode: mã cạnh cần loại bỏ
   * @return tập đồ thị đã loại bỏ mã cạnh
   */
  def shrink(graphSet: RDD[(Int, Array[Int], Array[(Int, Int)])], edgeCode: Broadcast[EdgeCode]): RDD[(Int, Array[Int], Array[(Int, Int)])] = {
    graphSet.map(g => {
      val ec = edgeCode.value
      val edges = g._3.filterNot(e => (e._1, 0, e._2) == (ec.lbFrom, ec.lbEdge, ec.lbTo))
      (g._1, g._2, edges)
    })
  }

  /**
   * Hàm tìm đường đi phải nhất
   * @param dfsCode: mã DFS cần tìm đường phải nhất
   * @return mảng ID của các đỉnh theo thứ tự duyệt từ gốc tới rightmost-node
   */
  def findRightMostPath(dfsCode: DFSCode): Array[Int] = {
    val rightMostNode = dfsCode.arrEdgeCode.last
    var rightMostID = if (rightMostNode.vFrom < rightMostNode.vTo) rightMostNode.vTo else rightMostNode.vFrom

    val path = new ArrayBuffer[Int]
    path += rightMostID
    for (code <- dfsCode.arrEdgeCode.reverse) {
      if (code.vTo == rightMostID && code.vFrom < code.vTo) {
        rightMostID = code.vFrom
        path += code.vFrom
      }
    }
    path.reverse.toArray
  }

  /**
   * Hàm tìm ứng viên theo hướng thêm cạnh lùi
   * @param graph: đồ thị để tìm cạnh
   * @param mapping: từ điển tra cứu edgecode là đỉnh nào trong đồ thị
   * @param dfsCode: mã DFS của đồ thị
   * @param rightMostPath: đường phải nhất của mã DFS
   * @return các mã DFS từ cạnh lùi có thể thêm vào mã DFS có sẵn để tạo thành ứng viên mới
   */
  def findBackwardGrowth(graph: (Int, Array[Int], Array[(Int, Int)]), mapping: Map[Int, Int], dfsCode: DFSCode, rightMostPath: Array[Int]): Array[((Int, Int, Int, Int, Int), Map[Int, Int])] = {
    val bound = if (dfsCode.arrEdgeCode.last.vFrom > dfsCode.arrEdgeCode.last.vTo) dfsCode.arrEdgeCode.last.vTo else -1
    val rightMostNode = rightMostPath.last

    val originalRightMostVertex = graph._3.filter(p => p._1 == mapping.get(rightMostNode).get)

    rightMostPath.dropRight(2).filter(_ > bound).flatMap(nodeId => {
      val possibleVertexId = mapping.get(nodeId).get
      originalRightMostVertex.filter(p => p._2 == possibleVertexId).map(e => ((rightMostNode, nodeId, e._1, 0, e._2), mapping))
    })
  }

  /**
   * Hàm tìm ứng viên theo hướng thêm cạnh tiến
   * @param graph: đồ thị để tìm cạnh
   * @param mapping: từ điển tra cứu edgecode là đỉnh nào trong đồ thị
   * @param dfsCode: mã DFS của đồ thị
   * @param rightMostPath: đường phải nhất của mã DFS
   * @return các mã DFS từ cạnh tiến có thể thêm vào mã DFS có sẵn để tạo thành ứng viên mới
   */
  def findForwardGrowth(graph: (Int, Array[Int], Array[(Int, Int)]), mapping: Map[Int, Int], dfsCode: DFSCode, rightMostPath: Array[Int]): Array[((Int, Int, Int, Int, Int), Map[Int, Int])] = {
    val rightMostNode = rightMostPath.last
    val mappedIds = mapping.values.toSet

    rightMostPath.flatMap(nodeId => {
      val originalVertex = graph._3.filter(p => p._1 == mapping.get(nodeId).get)
      originalVertex.filter(e => !mappedIds.contains(e._2))
        .map(e => ((nodeId, rightMostNode + 1, e._1, 0, e._2), mapping + ((rightMostNode + 1) -> e._2)))
    })
  }

  /**
   * Hàm so sánh 2 edgecode
   * @param edgeCode1
   * @param edgeCode2
   * @return -1 for <, 0 for ==, 1 for >
   */
  def edgeCodeCompare(edgeCode1: (Int, Int, Int, Int, Int), edgeCode2: (Int, Int, Int, Int, Int)): Int = {
    val forward1 = edgeCode1._1 < edgeCode1._2
    val forward2 = edgeCode2._1 < edgeCode2._2
    if (forward1 && forward2) {
      if (edgeCode1._1 != edgeCode2._1) {
        edgeCode2._1 - edgeCode1._1 //edgeCode2 - edgeCode1 !
      } else if (edgeCode1._3 != edgeCode2._3) {
        edgeCode1._3 - edgeCode2._3
      } else if (edgeCode1._4 != edgeCode2._4) {
        edgeCode1._4 - edgeCode2._4
      } else if (edgeCode1._5 != edgeCode2._5) {
        edgeCode1._5 - edgeCode2._5
      } else {
        0
      }
    } else if (!forward1 && !forward2) {
      if (edgeCode1._2 != edgeCode2._2) {
        edgeCode1._2 - edgeCode2._2
      } else if (edgeCode1._4 != edgeCode2._4) {
        edgeCode1._4 - edgeCode2._4
      } else {
        0
      }

    } else {
      if (forward1) 1 else -1
    }
  }

  /**
   * Hàm kiểm tra minDFS
   * @param arrEdgeCode: chuỗi mã DFS cần kiểm tra (DFS Code = array Edge Code)
   * @return <code>true</code> nếu là minDFS, <code>false</code> nếu ngược lại
   */
  def isMinDFSCode(arrEdgeCode: Array[EdgeCode]): Boolean = {
    def search(queue: Queue[(List[Int], Map[Int, Int], Int, Int)], edges: ArrayBuffer[(Int, Int, Int, Int, Int)]): Boolean = {
      while (queue.nonEmpty) {
        breakable {
          var (sequence, mapping, nextPosition, lastVisit) = queue.dequeue()
          val lastVertex = sequence.last
          val backwardEdges = edges.filter(edge => (edge._1 == lastVertex) && (edge._2 != lastVisit) && mapping.contains(edge._2))
            .map(edge => (mapping.get(edge._1).get, mapping.get(edge._2).get, edge._3, edge._4, edge._5))
            //.collect()
            .sortWith((e1, e2) => edgeCodeCompare(e1, e2) < 0)

          // Test backward edges
          for ((edgeCode, index) <- backwardEdges.zipWithIndex) {
            val result = edgeCodeCompare(edgeCode, arrEdgeCode(nextPosition + index).toTuple)
            if (result < 0) {
              return false
            } else if (result > 0) {
              break // For continue
            }
          }

          // Update next position
          nextPosition += backwardEdges.size
          var forwardFlag = false

          // check forward edges
          breakable {
            while (sequence.nonEmpty) {
              val lastVertex = sequence.last
              for (edge <- edges.filter(_._1 == lastVertex)) { //.collect()) {
                breakable {
                  if (!mapping.contains(edge._2)) {
                    val newCode = (mapping(edge._1), mapping.size, edge._3, edge._4, edge._5)
                    val result = edgeCodeCompare(newCode, arrEdgeCode(nextPosition).toTuple)
                    if (result < 0) {
                      return false
                    } else if (result > 0) {
                      break //continue
                    }
                    val updatedMapping = mapping + ((edge._2, mapping.size))
                    val updatedSequence = sequence :+ edge._2
                    queue.enqueue((updatedSequence, updatedMapping, nextPosition + 1, lastVertex))
                    forwardFlag = true
                  }
                }
              }

              if (forwardFlag) {
                break // break out of the while loop
              } else {
                sequence = sequence.init
              }
            }
          }

        }
      }
      true
    }

    //    val rddDFSCode = Config.sparkContext.parallelize(arrEdgeCode)
    //    rddDFSCode.persist(StorageLevel.MEMORY_AND_DISK)
    //    var tempEdges = rddDFSCode.map(ec => (ec.vFrom, ec.vTo, ec.lbFrom, ec.lbEdge, ec.lbTo))
    //    var tempVertices = rddDFSCode.flatMap(ec => Map((ec.vFrom -> ec.lbFrom), (ec.vTo -> ec.lbTo)))

    var tempVertices = Map[Int, Int]() //Array.fill[Vertex](90)(null)
    var tempEdges = ArrayBuffer[(Int, Int, Int, Int, Int)]()
    //var nodeNum = 0

    for (edgeCode <- arrEdgeCode) {
      val fromId = edgeCode.vFrom
      val toId = edgeCode.vTo
      val fromLabel = edgeCode.lbFrom
      val edgeLabel = edgeCode.lbEdge
      val toLabel = edgeCode.lbTo

      //val fromEdge = new Edge(fromId, toId, fromLabel, edgeLabel, toLabel)
      //val toEdge = new Edge(toId, fromId, toLabel, edgeLabel, fromLabel)
      tempEdges += ((fromId, toId, fromLabel, edgeLabel, toLabel))

      if (!tempVertices.contains(fromId)) {
        tempVertices += (fromId -> fromLabel)
      }
      if (!tempVertices.contains(toId)) {
        tempVertices += (toId -> toLabel)
      }
      //tempVertices(fromId).addEdge(fromEdge)
      //tempVertices(toId).addEdge(toEdge)
      //nodeNum = math.max(fromId + 1, math.max(toId + 1, nodeNum))

    }

    val queue = new Queue[(List[Int], Map[Int, Int], Int, Int)]
    val vertices = tempVertices.clone()
    val edges = tempEdges.clone()
    vertices.foreach(v => queue.enqueue((List(v._1), Map(v._1 -> 0), 0, -1))) // real id => edge code

    //    val queue = new Queue[(List[Int], Map[Int, Int], Int, Int)]
    //    tempVertices.collect.sortBy(_._1).foreach(v => queue.enqueue((List(v._1), Map(v._1 -> 0), 0, -1))) // real id => edge code
    //    val edges = tempEdges.collect()

    search(queue, edges)
  }

  def filterImpossible(child: (Int, Int, Int, Int, Int), forwardMapping: Array[(Int, (Int, Int))]): Boolean = {
    val findChild2 = forwardMapping.find(_._1 == child._2)
    val findChild1 = forwardMapping.find(_._1 == child._1)
    if (child._1 > child._2 && (findChild2 != None)) {
      if (findChild2.get._2 > (child._4, child._3)) {
        return false
      }
    } else if (child._1 < child._2 && (findChild1 != None)) {
      if (findChild1.get._2 > (child._4, child._5)) {
        return false
      }
    }
    true
  }

  def subgraphMining(graphSet: Array[(Int, Array[Int], Array[(Int, Int)])], s: ListBuffer[FinalDFSCode], dfsCode: DFSCode, minSupport: Int): Unit = {
    if (isMinDFSCode(dfsCode.arrEdgeCode)) {

      val support = dfsCode.support
      if (support >= minSupport) {
        s += new FinalDFSCode(dfsCode.arrEdgeCode, support)
        val (childrenGraphSet, childrenCounting) = enumerateSubGraph(graphSet, dfsCode)
        //childrenCounting.persist(StorageLevel.MEMORY_AND_DISK)

        val forwardMapping = dfsCode.arrEdgeCode //Config.sparkContext.parallelize(dfsCode.arrEdgeCode)
          .filter(ec => ec.vFrom < ec.vTo)
          .map(ec => (ec.vFrom, (ec.lbEdge, ec.lbTo)))
        //.collect()

        val supportedChild = childrenCounting
          .filter(_._2 >= minSupport)
          .keys
          .filter(child => filterImpossible(child, forwardMapping))
          //.collect()
          .toList
          .sortWith((e1, e2) => edgeCodeCompare(e1, e2) < 0)
        //val childrenCountingMap = childrenCounting.collectAsMap()

        for (child <- supportedChild) {
          val edgeCode = new EdgeCode(child._1, child._2, child._3, child._4, child._5)
          val codes = dfsCode.arrEdgeCode :+ edgeCode
          val projectedGraphSet = childrenGraphSet.get(child).get.toList
          val extendedDFSCode = new DFSCode(codes, projectedGraphSet, childrenCounting.get(child).get)
          subgraphMining(graphSet, s, extendedDFSCode, minSupport)
        }
      }
    }

  }

  def enumerateSubGraph(graphSet: Array[(Int, Array[Int], Array[(Int, Int)])], dfsCode: DFSCode): (Map[(Int, Int, Int, Int, Int), ListBuffer[(Int, Map[Int, Int])]], Map[(Int, Int, Int, Int, Int), Int]) = {
    def aux(graphId: Int, mapping: Map[Int, Int], rightMostPath: Array[Int]) = {
      val graph = graphSet.find(_._1 == graphId).get
      val backwardGrowth = findBackwardGrowth(graph, mapping, dfsCode, rightMostPath)
      val forwardGrowth = findForwardGrowth(graph, mapping, dfsCode, rightMostPath)
      (graphId, forwardGrowth, backwardGrowth)
    }

    val rightMostPath = findRightMostPath(dfsCode)

    val rddDFSCodeGraphSet = Config.sparkContext.parallelize(dfsCode.graphSet)
    val result = rddDFSCodeGraphSet.map(gs => aux(gs._1, Map[Int, Int]() ++ gs._2, rightMostPath))
    result.persist(StorageLevel.MEMORY_AND_DISK)

    /*val childrenGraphSet = new HashMap[(Int, Int, Int, Int, Int), ListBuffer[(Int, Map[Int, Int])]]
    val graphIdSet = new HashMap[(Int, Int, Int, Int, Int), HashSet[Int]]
    
    for ((graphId, forwardGrowth, backwardGrowth) <- result) {
      for ((growth, mapping) <- backwardGrowth) {
        if (!childrenGraphSet.contains(growth)) {
          childrenGraphSet.put(growth, new ListBuffer[(Int, Map[Int, Int])])
          graphIdSet.put(growth, new HashSet[Int])
        }
        childrenGraphSet(growth) += ((graphId, mapping))
        graphIdSet(growth) += graphId
      }

      for ((growth, mapping) <- forwardGrowth) {
        if (!childrenGraphSet.contains(growth)) {
          childrenGraphSet.put(growth, new ListBuffer[(Int, Map[Int, Int])])
          graphIdSet.put(growth, new HashSet[Int])
        }
        childrenGraphSet(growth) += ((graphId, mapping))
        graphIdSet(growth) += graphId
      }
    }*/

    val childGraphSetTemp = result.map {
      case (graphId, forwardGrowth, backwardGrowth) => {
        val childrenGraphSet = new HashMap[(Int, Int, Int, Int, Int), ListBuffer[(Int, Map[Int, Int])]]
        val graphIdSet = new HashMap[(Int, Int, Int, Int, Int), HashSet[Int]]
        for ((growth, mapping) <- backwardGrowth) {
          if (!childrenGraphSet.contains(growth)) {
            childrenGraphSet.put(growth, new ListBuffer[(Int, Map[Int, Int])])
            graphIdSet.put(growth, new HashSet[Int])
          }
          childrenGraphSet(growth) += ((graphId, mapping))
          graphIdSet(growth) += graphId
        }
        for ((growth, mapping) <- forwardGrowth) {
          if (!childrenGraphSet.contains(growth)) {
            childrenGraphSet.put(growth, new ListBuffer[(Int, Map[Int, Int])])
            graphIdSet.put(growth, new HashSet[Int])
          }
          childrenGraphSet(growth) += ((graphId, mapping))
          graphIdSet(growth) += graphId
        }
        (childrenGraphSet, graphIdSet)
      }
    }
    childGraphSetTemp.persist(StorageLevel.MEMORY_AND_DISK)

    val childrenGraphSet = Map[(Int, Int, Int, Int, Int), ListBuffer[(Int, Map[Int, Int])]]() ++ childGraphSetTemp.flatMap(_._1).reduceByKey((a, b) => a ++= b).collectAsMap()

    val childrenCount = Map[(Int, Int, Int, Int, Int), Int]() ++ childGraphSetTemp.flatMap(_._2).reduceByKey((a, b) => a ++= b).map(pair => (pair._1, pair._2.size)).collectAsMap()
    (childrenGraphSet, childrenCount)
  }
}