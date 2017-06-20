package main.scala.Output

import java.io.OutputStreamWriter
import java.io.FileOutputStream
import java.io.BufferedWriter
import java.io.File

class OutputFileWriter {
  def writeFile(path: String, whether_append: Boolean, content: String): Boolean = {
    try {
      /*val writer = new FileWriter(path, whether_append)
      writer.write(content)
      writer.close()*/
      
      val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(path), whether_append), "UTF-8"))
      writer.write(content)
      writer.flush()
      writer.close()
      
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