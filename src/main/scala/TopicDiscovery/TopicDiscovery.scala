package main.scala.TopicDiscovery

import main.scala.CoocurrenceGraph.CoocurrenceGraph
import scala.collection.mutable.HashMap
import scala.math._
import main.scala.Configuration.Config

class TopicDiscovery extends Serializable {
  def topicDiscover(filePath: String, dictionaryPath: String): Array[String] = {
    val coocurrence = new CoocurrenceGraph
    val graph = coocurrence.createCoocurrenceGraph(filePath)
    val vectorization = new Vectorization
    val (mTopic, mDimension, mTopicVector) = vectorization.readDictionary(dictionaryPath)
    val graphVector = vectorization.createVector(graph, mDimension)
    val topicIndex = kNearestNeighbor(graphVector, mTopicVector)
    
    val topic = topicIndex.map(id => {
      mTopic.find(_._1 == id).get._2
    })
    
    topic
  }
  
  def kNearestNeighbor(graphVector: Array[Boolean], mTopicVector: HashMap[Int, Array[Boolean]]): Array[Int] = {
    val rddTopicVector = Config.sparkContext.parallelize(mTopicVector.toList)
    val rddDistance = rddTopicVector.map(e => {
      (e._1, vectorDistance(e._2, graphVector))
    })
    rddDistance.persist(Config.defaultStorageLevel)
    
    val minDistance = rddDistance.map(_._2).min
    val topic = rddDistance.filter(_._2 == minDistance).map(_._1).collect()
    rddDistance.unpersist()
    
    topic
  }
  
  def vectorDistance(vector1: Array[Boolean], vector2: Array[Boolean]): Double = {
    val length = vector1.length
    /*if (vector2.length != length) {
      println("LENGTH FAIL!!!!!")
      println("1: " + length + " vs 2: " + vector2.length)
    }*/
    var sum = 0d
    for (i <- 0 until length) {
      if (vector1(i) != vector2(i)) sum += 1d
    }
    sqrt(sum)
  }
}