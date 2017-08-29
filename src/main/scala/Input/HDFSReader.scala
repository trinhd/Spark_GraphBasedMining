package main.scala.Input

import main.scala.Configuration.Config
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object HDFSReader {
  /**
   * Hàm đọc các tập tin từ thư mục HDFS đầu vào
   * @param inputPath đường dẫn đến thư mục chứa dữ liệu
   * @return RDD gồm 2 thành phần đường dẫn đến tập tin và nội dung tập tin
   */
  def hdfsFolderReader(inputPath: String): RDD[(String, String)] = {
    val rdd = Config.sparkContext.wholeTextFiles(inputPath)
    rdd
  }

  /**
   * Hàm đọc tập tin từ đường dẫn HDFS đầu vào
   * @param inputPath đường dẫn đến tập tin cần đọc
   * @return RDD chứa tập hợp các dòng trong tập tin
   */
  def hdfsFileReader(inputPath: String): RDD[String] = {
    val rdd = Config.sparkContext.textFile(inputPath)
    rdd
  }

  /**
   * Hàm xuất danh sách tất cả tập tin trong thư mục HDFS
   * @param folderPath đường dẫn thư mục cần tìm
   * @param recursive có đọc các thư mục con hay không?
   * @return danh sách đường dẫn tất cả các tập tin trong thư mục đầu vào
   */
  def getAllFileFromFolder(folderPath: String, recursive: Boolean): ArrayBuffer[Path] = {
    var arr = new ArrayBuffer[Path]

    val hdfs: FileSystem = FileSystem.get(Config.sparkContext.hadoopConfiguration)
    val status = hdfs.listFiles(new Path(folderPath), recursive)
    while (status.hasNext()) {
      arr += status.next().getPath
    }
    arr
  }

  def getAllSubFolder(folderPath: String): ArrayBuffer[Path] = {
    var arr = new ArrayBuffer[Path]

    val hdfs: FileSystem = FileSystem.get(Config.sparkContext.hadoopConfiguration)
    val status = hdfs.listLocatedStatus(new Path(folderPath))
    while (status.hasNext()) {
      val next = status.next()
      if (next.isDirectory()) {
        arr += next.getPath
      }
    }
    arr
  }

  def checkFolderExist(folderPath: String): Boolean = {
    val hdfs: FileSystem = FileSystem.get(Config.sparkContext.hadoopConfiguration)
    hdfs.exists(new Path(folderPath))
  }
}