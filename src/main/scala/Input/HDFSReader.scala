package main.scala.Input

import main.scala.Configuration.Config
import org.apache.spark.rdd.RDD

object HDFSReader {
  /**
   * Hàm đọc các tập tin từ thư mục HDFS đầu vào
   * @param inputPath đường dẫn đến thư mục chứa dữ liệu
   * @return RDD gồm 2 thành phần đường dẫn đến tập tin và nội dung tập tin
   */
  def hdfsReader(inputPath: String): RDD[(String, String)] = {
    val rdd = Config.sparkContext.wholeTextFiles(inputPath)
    rdd
  }
}