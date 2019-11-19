package hadoopIO

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import sevices.CopyFileService

import scala.collection.mutable.ListBuffer
import scala.io.Source

class HDFSHelperSpec extends WordSpec with HDFSCluster with BeforeAndAfterAll {
  override protected def beforeAll(): Unit = {
    startHDFS
  }

  override protected def afterAll(): Unit = {
    shutdownHDFS
  }

  "miniDFSClusterSpec" should {
    "write and read data from miniDFS cluster" in {
      val url = getNameNodeURI
      val dir = getNameNodeURI + "/test/"
      val hdfsHelper = new HDFSHelper[File](url)
      hdfsHelper.ls(dir).foreach(x => println(x))
      val data = new File("src/test/resources/HDFSTestFile.txt")
      val testFile = new File("src/test/resources/HDFSTestFile.txt")
      hdfsHelper.write(testFile, dir)
      val hdfsHelper2 = new HDFSHelper[String](url)
      hadoopIO.HdfsHelper2.setFileSystem(hdfsHelper.hdfs)
      hadoopIO.HdfsHelper2.writeToHdfsFile("test tttttttttt",dir+"/HDFSTestFile.txt")
      val result = hdfsHelper.read("hdfs://localhost:9000/test/HDFSTestFile.txt")
      /////

      val fileContents = Source.fromFile(result).getLines.mkString
      println("file content : \n" + fileContents)
      println("----------------------------------")
      //      val status = hdfsHelper.hdfs.listStatus(new Path(dir))
      //      status.foreach(x => println(x.isDirectory))
      hdfsHelper.listFilesFrom(dir).foreach(x => println("file : " + x.getLen))

      //////


      //assert(data == result)
    }
  }
}
