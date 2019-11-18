package hadoopIO

import java.io.File
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.collection.mutable.ListBuffer

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
      val dir = getNameNodeURI + "/user"
      val hdfsHelper = new HDFSHelper[File](url)
      val data = new File("src/test/resources/HDFSTestFile.txt")
      val testFile = new File("src/test/resources/HDFSTestFile.txt")
      hdfsHelper.write(testFile, dir)
      val result = hdfsHelper.read(dir)

      /////


//      val status = hdfsHelper.hdfs.listStatus(new Path(dir))
//      status.foreach(x => println(x.isDirectory))
      hdfsHelper.ls(dir).foreach(x=>println(x))

      //////


      assert(data == result)
    }
  }
}
