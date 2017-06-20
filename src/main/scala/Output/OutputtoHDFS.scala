package main.scala.Output

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import java.io.BufferedOutputStream
import main.scala.Configuration.Config

object OutputtoHDFS {
  def writeFile(path: String, content: String): Boolean = {
    try {
      val hdfs: FileSystem = FileSystem.get(Config.sparkContext.hadoopConfiguration)
      //val writer = hdfs.create(new Path(path))
      //writer.writeUTF(content)
      
      val outputstream = hdfs.create(new Path(path))
      val buffer = new BufferedOutputStream(outputstream)
      buffer.write(content.getBytes("UTF-8"))
      buffer.flush()
      buffer.close()
      
      hdfs.close()
      true
    } catch {
      case t: Throwable => {
        println("LỖI!!! KHÔNG GHI ĐƯỢC TẬP TIN")
        t.printStackTrace() // TODO: handle error
        false
      }
    }
  }
}