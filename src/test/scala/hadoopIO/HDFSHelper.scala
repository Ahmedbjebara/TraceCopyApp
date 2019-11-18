package hadoopIO

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

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

  def read(filePath: String): T = {
    Try {
      val path = new Path(filePath)
      val inputStream: FSDataInputStream = hdfs.open(path)
      val out = deserialize(IOUtils.toByteArray(inputStream))
      inputStream.close()
      hdfs.close()
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

  def getAllFiles(path: String): Seq[String] = {
    hdfs.open(new Path(path))
    val files = hdfs.listStatus(new Path(path))
    files.map(_.getPath().toString)
  }

  def ls(path: String): List[String] = {
    val conf = new Configuration()
    conf.set("fs.defaultFSi", uri)
    val fs: FileSystem = FileSystem.get(new URI(uri), conf)
    fs.mkdirs(new Path(uri + "/test/nid"))
    println("-----------------------" + fs.getWorkingDirectory.toString)
    val files = fs.listFiles(new Path(uri + "/test/"),false)

    val status = fs.listStatus(new Path(uri + "/test/"))
    status.foreach(x => println(x.getPath.toString))

    val filenames = ListBuffer[String]()
    while (files.hasNext) filenames += files.next().getPath().toString()
    filenames.toList
  }
}
