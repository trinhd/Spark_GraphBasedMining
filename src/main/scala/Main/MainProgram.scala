package main.scala.Main

import main.scala.gSpan.gSpan
import main.scala.CoocurrenceGraph.CoocurrenceGraph
import scala.collection.mutable.ListBuffer
import main.scala.gSpan.FinalDFSCode
import main.scala.Output.OutputtoHDFS
import main.scala.Configuration.Config
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import main.scala.CoocurrenceGraph.TestGraph
import main.scala.ExecutionTime.Timer

object MainProgram {
  def main(args: Array[String]) = {
    println("**************************************************************")
    println("*        CHƯƠNG TRÌNH KHAI PHÁ CHỦ ĐỀ CỦA TẬP VĂN BẢN        *")
    println("**************************************************************")
    try {
      //đọc vào các biến môi trường để thực hiện các chức năng cần thiết
      if (args(0) == "--help" || args(0) == "-h") {
        printHelp()
      } else if (args(0) == "--gSpan" || args(0) == "-gs") {
        Config.sparkConf = new SparkConf().setAppName("GraphBasedMining").setMaster("local[*]")
        Config.sparkContext = new SparkContext(Config.sparkConf)
        Config.minSupport = args(2).toDouble
        //val cooccurrenceGraph = new CoocurrenceGraph
        //val rddGraphs = cooccurrenceGraph.createCoocurrenceGraphSet(args(1))
        val testGraph = new TestGraph
        val (createGraphTime, rddGraphs) = Timer.timer(testGraph.createTestGraphSet(args(1)))
        rddGraphs.persist(StorageLevel.MEMORY_AND_DISK)

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
      } else {
        printHelp()
      }
    } catch {
      case t: Throwable => {
        t.printStackTrace() // TODO: handle error
        if (Config.sparkContext != null) Config.sparkContext.stop
        printHelp()
      }
    }
  }

  def resultToString(dfsFinalCode: ListBuffer[FinalDFSCode], frequentVertices: Array[(String, Int)]) = {
    var resString = ""
    val iCount = dfsFinalCode.length + frequentVertices.length
    resString += iCount + " đồ thị con phổ biến.\n"
    resString += "Trong đó có " + frequentVertices.length + " đỉnh phổ biến.\n"
    resString += "Và " + dfsFinalCode.length + " đồ thị con phổ biến được tạo thành từ ít nhất một cạnh.\nCác đỉnh phổ biến là:\n"
    resString += frequentVertices.map(v => v._1).mkString("\n")
    resString += "\nCác đồ thị con phổ biến là: \n"
    resString += dfsFinalCode.zipWithIndex.map { case (code, index) => code.extractInfo(index, frequentVertices) }.mkString("\n")
    resString
  }

  def printHelp() = {
    println("Usage: ProgramJarFile [Option] [Arguments]")
    println("       Option:")
    println("              --gSpan -gs : Frequent Subgraph Mining Using gSpan Algorithm. Arguments: FolderInputPath MinSupport OutputFilePath")
    println("              --help -h : Print this help")
  }
}