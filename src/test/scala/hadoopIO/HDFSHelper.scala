package hadoopIO

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import scala.util.{Failure, Success, Try}

case class HDFSHelper[T](uri: String) extends Serializable {
  val conf = new Configuration()
  conf.set("fs.defaultFSi","hdfs://127.0.0.1:8020/user/hdfs/folder/file.csv")
  val hdfs: FileSystem = FileSystem.get(conf)

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

}
