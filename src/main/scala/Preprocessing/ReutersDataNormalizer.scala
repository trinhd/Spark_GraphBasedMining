package main.scala

import main.scala.Configuration.Config
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.File
import scala.collection.mutable.Map
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.BufferedReader
import java.io.FileReader
import java.nio.charset.StandardCharsets
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import java.util.StringTokenizer
import scala.reflect.io.Path

class ReutersDataNormalizer {
  def ReutersDataPreprocess(inputPath: String, stopwordsPath: String): Unit = {
    // Processing
    //println("Processing...")
    val inputTopicFolders = (new File(inputPath)).listFiles()

    println("Input path: " + inputPath)

    // Analyzing
    //println("Processing ...")
    val specialChars = Array((" "), (";"), ("/"), ("."), (","), ("\""), ("\t"), ("#"), ("\u00a0"), ("("), (")"), ("["), ("]"), ("!"), ("?"), ("'"), (":"), ("&"), ("="), ("-"), ("<"), (">"), ("–"), ("{"), ("}"), ("\\"), ("..."), ("*"), ("+"), ("$"), ("@"), ("\u00a9"), ("\u00d7"), ("\u00ae"), ("\u00ad"), ("\u2028"), ("\u0323"), ("\u0300"), ("\u0301"), ("\u0302"), ("\u0303"), ("\u0309"), ("”"), ("“"), ("\""), ("\","), ("\":"), (":/"), ("\">"), ("=\"\""), ("=\"\">"), ("=\""), ("\"."), ("?\""), ("=”"), ("=“"), ("”,"), ("”."), ("“,"), ("“."), ("〞,"), ("〞."), ("〝,"), ("〝."), ("?”."), ("://"), ("_"), ("…"), ("’"), ("--"))
    val stopWords = Source.fromFile(Path.string2path(stopwordsPath).toCanonical.toString)("UTF-8").getLines().toArray

    val firstChars = Array(('"'), ('''), ('('), ('<'), ('#'), ('.'), ('”'), ('“'))
    val firstChars2 = Array(("</"), ("&#"))
    val lastChars = Array(('"'), ('''), (')'), ('>'), (','), ('.'), ('?'), ('!'), (';'), ('”'), ('“'))
    val lastChars2 = Array((",\""), (">,"), (".,"), (".>"), (".\""), ("),"))

    //~~~~~~~~~~ Processing ~~~~~~~~~~
    inputTopicFolders.foreach(topicFolder => {
      //val outputTopicDir = outputPath + File.separator + topicFolder.getName
      //val outputTopicFolder = new File(outputTopicDir)
      //if (!outputTopicFolder.exists) outputTopicFolder.mkdirs()
      val topic = topicFolder.getName
      val listFiles = topicFolder.listFiles()
      listFiles.foreach(file => {
        val filePath = file.getCanonicalPath
        println("Processing: " + filePath)
        var lines = Source.fromFile(file)("UTF-8").getLines().toArray
        var words = new ArrayBuffer[String]
        lines.foreach(line => {
          val tokens = new StringTokenizer(line.toLowerCase.trim, " ")
          while (tokens.hasMoreTokens()) {
            var w = tokens.nextToken()
            if (w.length > 1) {
              //println("First:\t\\u%04X".format(w(0).toInt))
              //println("Last:\t\\u%04X".format(w(w.length - 1).toInt))
              // trim first char
              if (firstChars2.contains(w.slice(0, 2))) {
                w = w.slice(2, w.length)
              }
              if (firstChars.contains(w(0))) {
                w = w.slice(1, w.length)
              }

              // trim last char
              if (lastChars2.contains(w.slice(w.length - 2, w.length))) {
                w = w.slice(0, w.length - 2)
              }
              if (lastChars.contains(w(w.length - 1))) {
                w = w.slice(0, w.length - 1)
              }

              if (!specialChars.contains(w) && !stopWords.contains(w)) {
                words.append(w)
              }
            }
          }
        })

        // file has less than 20 words is useless
        if (words.length >= 20) {
          println(Config.hostType)
          val OrientDBUtilsDoc = new OrientDBUtils(Config.hostType, Config.hostAddress, Config.database, Config.dbUser, Config.dbPassword, Config.userRoot, Config.pwdRoot)
          val connectionPool = OrientDBUtilsDoc.connectDBUsingDocAPI
          val docContent = List(("orgpath", file.getCanonicalPath), ("content", words.mkString("\n")))
          OrientDBUtilsDoc.insertDoc(connectionPool, topic, docContent)
          println("Inserted to Database!")
          connectionPool.close
        } else {
          println("File too short!")
        }
      })
    })
    println("Finished!!!")
  }
}