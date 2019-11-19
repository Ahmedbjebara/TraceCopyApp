package hadoopIO

import java.io.{File, FileWriter}
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

    val url = getNameNodeURI
    val dir = getNameNodeURI + "/test"
    val hdfsHelper = new HDFSHelper[File](url)

    hdfsHelper.hdfs.mkdirs(new Path(url + "/test/source"))
    hdfsHelper.hdfs.mkdirs(new Path(url + "/test/destination"))

    val testFile = new File("src/test/resources/HDFSTestFile.txt")
    hdfsHelper.write(testFile, dir + "/source/HDFSTestFile.txt")

    hdfsHelper.listFilesFrom(dir + "/source").foreach(x => println("file : " + x.getPath.toString))
  }

  override protected def afterAll(): Unit = {
    shutdownHDFS
  }

  "miniDFSClusterSpec" should {
    "write and read data from miniDFS cluster" in {

//      //hdfsHelper.ls(dir).foreach(x => println(x))
//      val data = new File("src/test/resources/HDFSTestFile.txt")
//
//      val hdfsHelper2 = new HDFSHelper[String](url)
//      // hdfsHelper2.write("test tttttttttt",dir+"/HDFSTestFile.txt")
//      val result = hdfsHelper.read("hdfs://localhost:9000/test/HDFSTestFile.txt")
//      /////
//      val fw = new FileWriter(result, true)
//      try {
//        fw.write("test tttttttttt")
//      }
//      finally fw.close()
//      hdfsHelper.write(result, dir + "/HDFSTestFile.txt")
//      val result2 = hdfsHelper.read("hdfs://localhost:9000/test/HDFSTestFile.txt")
//      val fileContents = Source.fromFile(result2).getLines.mkString
//      println("file content : \n" + fileContents)
//      println("----------------------------------")
//      //      val status = hdfsHelper.hdfs.listStatus(new Path(dir))
//      //      status.foreach(x => println(x.isDirectory))
//      hdfsHelper.listFilesFrom(dir).foreach(x => println("file : " + x.getLen))
//
//      //////


//      assert(data == result)
    }
  }
}
