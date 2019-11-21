package hadoopIO

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream, ObjectOutputStream, PrintWriter}
import java.net.URI
import org.apache.hadoop.fs.FSDataOutputStream
import java.io.PrintWriter

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileStatus, FileSystem, Path}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class HDFSHelper[T](uri: String) extends Serializable {
  val conf = new Configuration()
  conf.set("fs.defaultFSi", uri)

  val hdfs: FileSystem = FileSystem.get(new URI(uri), conf)

  def write(data: T, filePath: String): Unit = {
    Try {
      val path = new Path(filePath)
      hdfs.create(path)
    } match {
      case Success(dataOutputStream) =>
        dataOutputStream.write(serialize(data))
        dataOutputStream.close()
      case Failure(e) => e.printStackTrace()
    }
  }

  def writeInto(data: String, filePath: String): Unit = {
    val os = hdfs.create(new Path(filePath))
    os.write(data.getBytes)
    os.close()
  }

  def read(filePath: String): T = {
    Try {
      val path = new Path(filePath)
      val inputStream: FSDataInputStream = hdfs.open(path)
      val out = deserialize(IOUtils.toByteArray(inputStream))
      inputStream.close()
      //hdfs.close()
      out
    } match {
      case Success(value) => value
      case Failure(ex) => throw ex
    }
  }

  def serialize(data: T): Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    } catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  def serializeST(data: String): Array[Byte] = {
    try {
      val byteOut = new ByteArrayOutputStream()
      val objOut = new ObjectOutputStream(byteOut)
      objOut.writeObject(data)
      objOut.close()
      byteOut.close()
      byteOut.toByteArray
    } catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  def deserialize(bytes: Array[Byte]): T = {
    try {
      val byteIn = new ByteArrayInputStream(bytes)
      val objIn = new ObjectInputStream(byteIn)
      val obj = objIn.readObject().asInstanceOf[T]
      byteIn.close()
      objIn.close()
      obj
    } catch {
      case ex: Exception => throw new Exception(ex.getMessage)
    }
  }

  def listFilesFrom(path: String): Seq[FileStatus] = {
    val conf = new Configuration()
    conf.set("fs.defaultFSi", uri)
    val fs: FileSystem = FileSystem.get(new URI(uri), conf)
    val files = fs.listStatus(new Path(path))
    files.filterNot(x => x.isDirectory)
  }

  def ls(path: String): List[String] = {
    val status = hdfs.listStatus(new Path(path))
    status.map(x => x.getPath.toString).toList
  }

  def isFileEmpty(file: String) = {
    val stream = hdfs.open(new Path(file))
    if (stream.read().equals(-1)) true
    else false
  }

  def move(srcPath : String,dstPath : String): Unit ={
    org.apache.hadoop.fs.FileUtil.copy(
      hdfs,
      new Path(srcPath),
      hdfs,
      new Path(dstPath),
      true,
      conf
    )
  }

}
