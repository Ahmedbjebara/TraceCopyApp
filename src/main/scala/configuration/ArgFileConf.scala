package configuration

import java.io.InputStream

import org.apache.hadoop.fs.FSDataInputStream

import scala.xml.XML

object ArgFileConf {


  def loadConfig(configPath: String ): Config = {

    val argFile = XML.loadString(configPath)

    val sourceDirectory=  (argFile \ "sourceDirectory").text
    val destinationDirectory = (argFile \ "destinationDirectory").text
    val tracePath = (argFile \ "tracePath").text
    val traceFileName = (argFile \ "traceFileName").text
    val schemaFile = (argFile \ "schemaFile").text
    val resultFile = (argFile \ "resultFile").text
    val readMode = (argFile \ "readMode").text
    val partitionColumn = (argFile \ "partitionColumn").text
    val sourceFileFormat = (argFile \ "sourceFileFormat").text
    val tracemethod = (argFile \ "tracemethod").text



    Config(
      sourceDirectory,
      destinationDirectory,
      tracePath,
      traceFileName,
      schemaFile,
      resultFile,
      readMode,
      partitionColumn,
      sourceFileFormat,
      tracemethod)
  }
}
