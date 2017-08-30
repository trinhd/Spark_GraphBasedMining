package main.scala.TopicDiscovery

import main.scala.CoocurrenceGraph.CoocurrenceGraph
import scala.collection.mutable.HashMap
import scala.math._
import main.scala.Configuration.Config
import org.apache.hadoop.fs.FileSystem
import main.scala.Input.HDFSReader
import scala.collection.mutable.ArrayBuffer
import main.scala.Input.FileReader

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

  def topicDiscoverForFolder(folderPath: String, dictionaryPath: String): ArrayBuffer[(String, Array[String])] = {
    val listFiles = HDFSReader.getAllFileFromFolder(folderPath, false)
    val coocurrence = new CoocurrenceGraph
    val vectorization = new Vectorization
    val (mTopic, mDimension, mTopicVector) = vectorization.readDictionary(dictionaryPath)
    val res = listFiles.map(file => {
      val graph = coocurrence.createCoocurrenceGraph(file.toString())
      val graphVector = vectorization.createVector(graph, mDimension)
      val topicIndex = kNearestNeighbor(graphVector, mTopicVector)
      val topic = topicIndex.map(id => {
        mTopic.find(_._1 == id).get._2
      })
      (file.getName, topic)
    })
    res
  }
  
  def topicDiscoverForLocalFolder(folderPath: String, dictionaryPath: String): Array[(String, Array[String])] = {
    val listFiles = FileReader.getAllFilesOnFolder(folderPath)
    val coocurrence = new CoocurrenceGraph
    val vectorization = new Vectorization
    val (mTopic, mDimension, mTopicVector) = vectorization.readDictionary(dictionaryPath)
    val res = listFiles.map(file => {
      val graph = coocurrence.createCoocurrenceGraph(file.getCanonicalPath)
      val graphVector = vectorization.createVector(graph, mDimension)
      val topicIndex = kNearestNeighbor(graphVector, mTopicVector)
      val topic = topicIndex.map(id => {
        mTopic.find(_._1 == id).get._2
      })
      (file.getName, topic)
    })
    res
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