package main.scala.Main

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

import main.scala.Configuration.Config
import main.scala.CoocurrenceGraph.CoocurrenceGraph
import main.scala.CoocurrenceGraph.TestGraph
import main.scala.ExecutionTime.Timer
import main.scala.Output.OutputtoHDFS
import main.scala.gSpan.EdgeCode
import main.scala.gSpan.FinalDFSCode
import main.scala.gSpan.gSpan
import main.scala.GraphCharacteristic.CharacteristicExtract
import main.scala.TopicDiscovery.Vectorization
import main.scala.TopicDiscovery.TopicDiscovery
import main.scala.Input.HDFSReader
import main.scala.Input.FileReader
import java.io.File
import scala.collection.mutable.ArrayBuffer

object MainProgram {
  def main(args: Array[String]) = {
    println("**************************************************************")
    println("*        CHƯƠNG TRÌNH KHAI PHÁ CHỦ ĐỀ CỦA TẬP VĂN BẢN        *")
    println("**************************************************************")
    try {
      //đọc vào các biến môi trường để thực hiện các chức năng cần thiết
      if (args(0) == "--help" || args(0) == "-h") {
        printHelp()
      } else {
        Config.sparkConf = new SparkConf().setAppName(Config.appName).setMaster(Config.master)
        Config.sparkConf.set("spark.serializer", Config.serializer)
        if (Config.serializer.equals("org.apache.spark.serializer.KryoSerializer")) Config.sparkConf.registerKryoClasses(Array(classOf[CoocurrenceGraph], classOf[TestGraph], classOf[FinalDFSCode], classOf[EdgeCode], classOf[TopicDiscovery]))

        Config.sparkContext = new SparkContext(Config.sparkConf)
        if (args(0) == "--gSpan" || args(0) == "-gs") {
          Config.minSupport = args(2).toDouble
          //-----------GRAPH THẬT------------
          val cooccurrenceGraph = new CoocurrenceGraph
          val (createGraphTime, rddGraphs) = Timer.timer(cooccurrenceGraph.createCoocurrenceGraphSet(args(1)))
          //---------------------------------

          //cooccurrenceGraph.printTenGraphs(rddGraphs)

          //-----------GRAPH TEST------------
          //val testGraph = new TestGraph
          //val (createGraphTime, rddGraphs) = Timer.timer(testGraph.createTestGraphSet(args(1)))
          //---------------------------------

          rddGraphs.persist(Config.defaultStorageLevel)
          
          println("Thư mục đang được xử lý là: " + args(1))
          
          val gspan = new gSpan
          val (miningTime, (s, frequentVertices)) = Timer.timer(gspan.frequentSubgraphMining(rddGraphs))
          println("---------OUTPUT---------")
          var sRes = resultToString(s, frequentVertices)
          sRes += ("\n---------TIMER---------\nThời gian đọc dựng đồ thị là: " + createGraphTime / 1000000000d + " giây.")
          sRes += ("\nThời gian tìm đồ thị con phổ biến là: " + miningTime / 1000000000d + " giây.")
          sRes += ("\nTổng thời gian là: " + (createGraphTime + miningTime) / 1000000000d + " giây.")
          println(sRes)
          if (OutputtoHDFS.writeFile(args(3), sRes)) println("Kết quả tính được ghi thành công xuống tập tin " + args(3))
          println("----------END----------")
          Config.sparkContext.stop

        } else if (args(0) == "--gSpanDirectory" || args(0) == "-gsd") {
          Config.minSupport = args(2).toDouble
          //-----------GRAPH THẬT------------
          val cooccurrenceGraph = new CoocurrenceGraph

          if (HDFSReader.checkFolderExist(args(1))) {
            //println("IN HDFS!!!")
            val subFolder = HDFSReader.getAllSubFolder(args(1))
            //println("SUBFOLDER LENGTH: " + subFolder.length)
            subFolder.foreach(path => {
              val strPath = path.toString()
              val (createGraphTime, rddGraphs) = Timer.timer(cooccurrenceGraph.createCoocurrenceGraphSet(strPath))
              rddGraphs.persist(Config.defaultStorageLevel)

              println("Thư mục đang được xử lý là: " + strPath)
              
              val gspan = new gSpan
              val (miningTime, (s, frequentVertices)) = Timer.timer(gspan.frequentSubgraphMining(rddGraphs))
              rddGraphs.unpersist()
              println("---------OUTPUT---------")
              var sRes = resultToString(s, frequentVertices)
              sRes += ("\n---------TIMER---------\nThời gian đọc dựng đồ thị là: " + createGraphTime / 1000000000d + " giây.")
              sRes += ("\nThời gian tìm đồ thị con phổ biến là: " + miningTime / 1000000000d + " giây.")
              sRes += ("\nTổng thời gian là: " + (createGraphTime + miningTime) / 1000000000d + " giây.")
              println(sRes)
              val outputPath = args(3) + File.separator + path.getName.split("_")(0) + "_res"
              if (OutputtoHDFS.writeFile(outputPath, sRes)) println("Kết quả tính được ghi thành công xuống tập tin " + outputPath)
              println("----------END----------")
            })
          } else if (FileReader.checkFolderExist(args(1))) {
            //println("IN LOCAL!!!")
            val subFolder = FileReader.getAllSubFolderPath(args(1))
            //println("SUBFOLDER LENGTH: " + subFolder.length)
            subFolder.foreach(file => {
              val strPath = file.getCanonicalPath
              val (createGraphTime, rddGraphs) = Timer.timer(cooccurrenceGraph.createCoocurrenceGraphSetFromLocal(strPath))
              rddGraphs.persist(Config.defaultStorageLevel)

              println("Thư mục đang được xử lý là: " + strPath)
              
              val gspan = new gSpan
              val (miningTime, (s, frequentVertices)) = Timer.timer(gspan.frequentSubgraphMining(rddGraphs))
              rddGraphs.unpersist()
              println("---------OUTPUT---------")
              var sRes = resultToString(s, frequentVertices)
              sRes += ("\n---------TIMER---------\nThời gian đọc dựng đồ thị là: " + createGraphTime / 1000000000d + " giây.")
              sRes += ("\nThời gian tìm đồ thị con phổ biến là: " + miningTime / 1000000000d + " giây.")
              sRes += ("\nTổng thời gian là: " + (createGraphTime + miningTime) / 1000000000d + " giây.")
              println(sRes)
              val outputPath = args(3) + File.separator + file.getName.split("_")(0) + "_res"
              if (OutputtoHDFS.writeFile(outputPath, sRes)) println("Kết quả tính được ghi thành công xuống tập tin " + outputPath)
              println("----------END----------")
            })
          } else {
            println("ERROR: KHÔNG TỒN TẠI THƯ MỤC ĐẦU VÀO TRÊN CẢ HDFS VÀ LOCAL!")
            printHelp()
          }
          Config.sparkContext.stop
        } else if (args(0) == "--characteristicExtract" || args(0) == "-ce") {
          Config.minDistance = args(2).toDouble
          val characteristicExtract = new CharacteristicExtract
          val (extractTime, _) = Timer.timer(characteristicExtract.characteristicExtract(args(1), args(3)))
          println("Thời gian thực thi là: " + extractTime / 1000000000d + " giây.")
          Config.sparkContext.stop
        } else if (args(0) == "--createDictionary" || args(0) == "-cd") {
          val vectorization = new Vectorization
          val (extractTime, _) = Timer.timer(vectorization.createDictionary(args(1), args(2).toInt, args(3)))
          println("Thời gian thực thi là: " + extractTime / 1000000000d + " giây.")
          Config.sparkContext.stop
        } else if (args(0) == "--topicDiscovery" || args(0) == "-td") {
          val topicDiscovery = new TopicDiscovery
          val (extractTime, topic) = Timer.timer(topicDiscovery.topicDiscover(args(1), args(2)))
          println("TOPIC:\n" + topic.mkString("\n"))
          println("Thời gian thực thi là: " + extractTime / 1000000000d + " giây.")
          Config.sparkContext.stop
        } else if (args(0) == "--topicDiscoveryForDir" || args(0) == "-tdd") {
          val topicDiscovery = new TopicDiscovery
          if (HDFSReader.checkFolderExist(args(1))) {
            val (extractTime, topic) = Timer.timer(topicDiscovery.topicDiscoverForFolder(args(1), args(2)))
            var sRes = topic.map(file => {
              file._1 + " :: " + file._2.mkString(" :: ")
            }).mkString("\n")
            println(sRes)
            if (OutputtoHDFS.writeFile(args(3), sRes)) println("Kết quả tính được ghi thành công xuống tập tin " + args(3))
            println("Thời gian thực thi là: " + extractTime / 1000000000d + " giây.")
          } else if (FileReader.checkFolderExist(args(1))) {
            val (extractTime, topic) = Timer.timer(topicDiscovery.topicDiscoverForLocalFolder(args(1), args(2)))
            var sRes = topic.map(file => {
              file._1 + " :: " + file._2.mkString(" :: ")
            }).mkString("\n")
            println(sRes)
            if (OutputtoHDFS.writeFile(args(3), sRes)) println("Kết quả tính được ghi thành công xuống tập tin " + args(3))
            println("Thời gian thực thi là: " + extractTime / 1000000000d + " giây.")
          } else {
            println("ERROR: KHÔNG TỒN TẠI THƯ MỤC ĐẦU VÀO TRÊN CẢ HDFS VÀ LOCAL!")
            printHelp()
          }

          Config.sparkContext.stop
        } else {
          if (Config.sparkContext != null) Config.sparkContext.stop
          printHelp()
        }
      }
    } catch {
      case t: Throwable => {
        t.printStackTrace() // TODO: handle error
        if (Config.sparkContext != null) Config.sparkContext.stop
        printHelp()
      }
    }
  }

  def resultToString(dfsFinalCode: ListBuffer[FinalDFSCode], frequentVertices: Array[(String, Int, Int)]) = {
    var resString = ""
    val iCount = dfsFinalCode.length + frequentVertices.length
    resString += iCount + " đồ thị con phổ biến.\n"
    if (frequentVertices.length > 0) {
      resString += "Trong đó có " + frequentVertices.length + " đỉnh phổ biến.\n"
      resString += "Và " + dfsFinalCode.length + " đồ thị con phổ biến được tạo thành từ ít nhất một cạnh.\nCác đỉnh phổ biến là:\n"
      resString += frequentVertices.map(v => v._1 + " :: " + v._2).mkString("\n")
      if (dfsFinalCode.length > 0) {
        resString += "\nCác đồ thị con phổ biến là: \n"
        resString += dfsFinalCode.zipWithIndex.map { case (code, index) => code.extractInfo(index, frequentVertices) }.mkString("\n")
      }
    }
    resString
  }

  def printHelp() = {
    println("Usage: ProgramJarFile [Option] [Arguments]")
    println("       Option:")
    println("              --gSpan -gs : Frequent Subgraph Mining Using gSpan Algorithm. Arguments: FolderInputPath MinSupport OutputFilePath")
    println("              --gSpanDirectory -gsd : Frequent Subgraph Mining Using gSpan Algorithm For All SubFolder. Arguments: FolderInputPath MinSupport OutputFolderPath")
    println("              --characteristicExtract -ce : Extract topic characteristic. Arguments: FolderInputPath MinDistance FolderOutputPath")
    println("              --createDictionary -cd : Create dictionary from all topic characteristic. Arguments: FolderInputPath MaxDimension OutputFilePath")
    println("              --topicDiscovery -td : Discover topic of given document. Arguments: DocumentPath DictionaryPath")
    println("              --topicDiscoveryForDir -tdd : Discover topic of given document on directory. Arguments: FolderPath DictionaryPath ResultOutputPath")
    println("              --help -h : Print this help")
  }
}