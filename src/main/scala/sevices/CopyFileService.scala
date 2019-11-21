package sevices

import java.io.{File, FileInputStream, FileWriter}
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}

import hadoopIO.HDFSHelper
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession

import scala.io.Source


object CopyFileService {

  val hdfsHelper = new HDFSHelper[File]("")

  def tracedCopy(sourceDirectory: String, destinationDirectory: String, tracePath: String, traceFileName: String)(implicit spark: SparkSession): Unit = {
    val traceFilePath = tracePath + traceFileName
    initTraceFile(traceFilePath) // 2 APPEL //OK
    val fileList = hdfsHelper.listFilesFrom(sourceDirectory) // 1 APPEL //OK
    // TODO: useless rdd
    val tracedFilesChecksum = getTracedFilesChecksum(traceFilePath) // 3 APPEL //OK
    val fileRdd = spark.sparkContext.parallelize(fileList)
    fileRdd.foreach(file =>
      tracedMove(file, sourceDirectory, destinationDirectory, traceFilePath, tracedFilesChecksum) // 4 APPEL
    )
  }

  //  private def listFilesFrom(directory: String): List[File] = {
  //    val directoryFile: File = new File(directory)
  //    if (directoryFile.exists && directoryFile.isDirectory) {
  //      directoryFile.listFiles.filter(_.isFile).toList
  //    } else {
  //      List[File]()
  //    }
  //  }

  //private def isFileEmpty(file: String) = Source.fromFile(file).isEmpty


  private def initTraceFile(traceFilePath: String): Unit = {
    val header: String = "File;Source;Destination;State;Cheksum;Message;Size;LastModifiedDate"
    if (hdfsHelper.isFileEmpty(traceFilePath)) {
      hdfsHelper.writeInto(header, traceFilePath)
    }
  }


  private def computeHash(path: String): String = {
    val stream = hdfsHelper.hdfs.open(new Path(path))
    org.apache.hadoop.io.MD5Hash.digest(stream).toString
  }


  private def getTracedFilesChecksum(traceFilePath: String)(implicit spark: SparkSession): Array[String] = {
    val traceFile = spark.read.option("delimiter", ";").option("header", "true").csv(traceFilePath)
    traceFile
      .select("Cheksum")
      .collect()
      .map(x => x.getString(0))

  }

  private def traceWriter(traceFileWriter: FileWriter, traceFilePath: String, message: String) = {
    //traceFileWriter.write(message)
    hdfsHelper.writeInto(message,traceFilePath)
  }

  // case class TracedMoveAction(file: File, sourceDirectory: String, destinationDirectory: String,traceFilePath: String,
  //   tracedFilesChecksum : Array[String])
  //  {
  private def tracedMove(file: FileStatus, sourceDirectory: String, destinationDirectory: String, traceFilePath: String,
                         tracedFilesChecksum: Array[String]) = {


    val traceFileWriter = new FileWriter(new File(traceFilePath), true)
    val hash = computeHash(sourceDirectory + file.getPath.getName) //check du nveau fichier  // APPEL //ok
    val exists = hdfsHelper.hdfs.exists(new Path(destinationDirectory + file.getPath.getName)) // true ou false existe dans le rep destination


    // complexity is proportional to tracedFilesChecksum s length

    (exists, tracedFilesChecksum.contains(hash)) match {
      case (false, false) => {

        hdfsHelper.move(
          sourceDirectory + file.getPath.getName,
          destinationDirectory + file.getPath.getName
        )
        val messageSuccess: String = file.getPath.getName + ";" + file.getPath.toString + ";" + destinationDirectory + file.getPath.getName + ";MOVE SUCCESS: File's Name  dosen't exist yet !" + ";" + hash + ";Cheksum dosen't exist yet !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageSuccess)
      }

      case (true, false) => {

        val messageFileNameExists = file.getPath.getName + ";" + file.getPath.toString + ";" + destinationDirectory + file.getPath.getName + ";MOVE FAILED: File's Name Already Exists" + ";" + hash + "; " + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageFileNameExists)
      }

      case (false, true) => {

        val messageChecksumExists = file.getPath.getName + ";" + file.getPath.toString + ";" + destinationDirectory + file.getPath.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum Exists Already !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageChecksumExists)
      }

      case (true, true) => {

        val messageChecksum_NameExists = file.getPath.getName + ";" + file.getPath.toString + ";" + destinationDirectory + file.getPath.getName + ";MOVE FAILED" + ";" + hash + ";Cheksum AND Name Exists Already !" + String.format("%n")
        traceWriter(traceFileWriter, traceFilePath, messageChecksum_NameExists)
      }
    }
    traceFileWriter.close()

  }

}
