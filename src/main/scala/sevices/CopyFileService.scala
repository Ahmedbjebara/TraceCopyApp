package sevices

import java.io.{File, FileInputStream, FileWriter}
import java.nio.file.{Files, Paths}
import java.security.{DigestInputStream, MessageDigest}

import hadoopIO.HDFSHelper
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.io.Source


case class CopyFileService (uri : String) extends Serializable {

  val hdfsHelper = new HDFSHelper[File](uri)

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
    val hdfsHelper = new HDFSHelper[File](uri)
    val header: String = "File;Source;Destination;State;Cheksum;Message;Size;LastModifiedDate"
    if (hdfsHelper.isFileEmpty(traceFilePath)) {
      hdfsHelper.writeInto(header, traceFilePath,hdfsHelper.hdfs)
    }
  }


  private def computeHash(path: String): String = {
    val hdfsHelper = new HDFSHelper[File](uri)
    println("hash "+path+" hdfs "+hdfsHelper)
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

  private def traceWriter(traceFilePath: String, message: String, fs : FileSystem) = {
    //traceFileWriter.write(message)
    hdfsHelper.writeInto(message,traceFilePath,fs)
  }

  // case class TracedMoveAction(file: File, sourceDirectory: String, destinationDirectory: String,traceFilePath: String,
  //   tracedFilesChecksum : Array[String])
  //  {
  private def tracedMove(file: (String,String), sourceDirectory: String, destinationDirectory: String, traceFilePath: String,
                         tracedFilesChecksum: Array[String]) = {

    val hdfsHelper = new HDFSHelper[File](uri)
    val hash = computeHash(sourceDirectory + file._2) //check du nveau fichier  // APPEL //ok
    val exists = hdfsHelper.hdfs.exists(new Path(destinationDirectory + file._2)) // true ou false existe dans le rep destination


    // complexity is proportional to tracedFilesChecksum s length

    (exists, tracedFilesChecksum.contains(hash)) match {
      case (false, false) => {

        hdfsHelper.move(
          sourceDirectory + file._2,
          destinationDirectory + file._2
        )
        val messageSuccess: String = file._2 + ";" + file._1 + ";" + destinationDirectory + file._2 + ";MOVE SUCCESS: File's Name  dosen't exist yet !" + ";" + hash + ";Cheksum dosen't exist yet !" + String.format("%n")
        traceWriter(traceFilePath, messageSuccess,hdfsHelper.hdfs)
      }

      case (true, false) => {

        val messageFileNameExists = file._2 + ";" + file._1 + ";" + destinationDirectory + file._2 + ";MOVE FAILED: File's Name Already Exists" + ";" + hash + "; " + String.format("%n")
        traceWriter( traceFilePath, messageFileNameExists,hdfsHelper.hdfs)
      }

      case (false, true) => {

        val messageChecksumExists = file._2 + ";" + file._1 + ";" + destinationDirectory + file._2 + ";MOVE FAILED" + ";" + hash + ";Cheksum Exists Already !" + String.format("%n")
        traceWriter( traceFilePath, messageChecksumExists,hdfsHelper.hdfs)
      }

      case (true, true) => {

        val messageChecksum_NameExists = file._2 + ";" + file._1 + ";" + destinationDirectory + file._2 + ";MOVE FAILED" + ";" + hash + ";Cheksum AND Name Exists Already !" + String.format("%n")
        traceWriter( traceFilePath, messageChecksum_NameExists,hdfsHelper.hdfs)
      }
    }

  }

}
