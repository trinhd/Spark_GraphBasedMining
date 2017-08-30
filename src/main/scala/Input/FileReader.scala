package main.scala.Input

import scala.io.Source
import java.io.File
import scala.collection.mutable.ArrayBuffer

object FileReader {
  def fileReader(inputPath: String): Array[String] = {
    try {
      val arrResult = Source.fromFile(inputPath)("UTF-8").getLines().toArray
      arrResult
    } catch {
      case t: Throwable => {
        println("Lỗi không đọc được tập tin ở đường dẫn: " + new java.io.File(inputPath).getCanonicalPath)
        t.printStackTrace() // TODO: handle error
        null
      }
    }
  }
  
  def checkFolderExist(folderPath: String): Boolean = {
    new File(folderPath).exists()
  }
  
  def getAllSubFolderPath(folderPath: String): Array[File] = {
    var folder = new File(folderPath)
    folder.listFiles().filter(_.isDirectory())
  }
  
  def getAllFilesOnFolder(folderPath: String): Array[File] = {
    var folder = new File(folderPath)
    folder.listFiles().filter(_.isFile())
  }
  
  def folderReader(folderPath: String): Array[(String, Array[String])] = {
    var folder = new File(folderPath)
    folder.listFiles().filter(_.isFile()).map(file => {
      val Path = file.getCanonicalPath
      (Path, fileReader(Path))
    })
  }
}