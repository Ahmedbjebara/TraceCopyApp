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
    val hdfsHelper = new HDFSHelper(url)

    hdfsHelper.hdfs.mkdirs(new Path(url + "/test/source"))
    hdfsHelper.hdfs.mkdirs(new Path(url + "/test/destination"))
    hdfsHelper.hdfs.mkdirs(new Path(url + "/test/trace"))

    hdfsHelper.hdfs.create(new Path(dir + "/source/HDFSTestFile2.txt"))
    hdfsHelper.hdfs.create(new Path(dir + "/source/HDFSTestFile3.txt"))
    hdfsHelper.hdfs.create(new Path(dir + "/source/HDFSTestFile4.txt"))
    hdfsHelper.hdfs.create(new Path(dir + "/trace/trace.txt"))

    hdfsHelper.listFilesFrom(dir + "/source").foreach(x => println("file : " + x._1))
  }

  override protected def afterAll(): Unit = {
    shutdownHDFS
  }

  "miniDFSClusterSpec" should {
    "write and read data from miniDFS cluster" in {
      val url = getNameNodeURI
      val dir = getNameNodeURI + "/test"
      val hdfsHelper = new HDFSHelper(url)
      val os = hdfsHelper.hdfs.create(new Path(dir + "/source/HDFSTestFile3.txt"))
      os.write("This is a test".getBytes)
      os.close()

      println(hdfsHelper.isFileEmpty(dir + "/source/HDFSTestFile2.txt"))
//      def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))
//
//      //This example checks line for null and prints every existing line consequentally
//      readLines.takeWhile(_ != null).foreach(line => println(line))
      //      //hdfsHelper.ls(dir).foreach(x => println(x))
      //      val data = new File("src/test/resources/HDFSTestFile.txt")
      //
      //      val hdfsHelper2 = new HDFSHelper[String](url)
      //      // hdfsHelper2.write("test tttttttttt",dir+"/HDFSTestFile.txt")
      //      val result = hdfsHelper.read(dir + "/source/HDFSTestFile3.txt")
      //      /////
      //      val fw = new FileWriter(result, true)
      //      try {
      //        fw.write("test tttttttttt")
      //      }
      //      finally fw.close()
      //      hdfsHelper.write(result, dir + "/source/HDFSTestFile3.txt")
      //      val result2 = hdfsHelper.read(dir + "/source/HDFSTestFile3.txt")
      //      val fileContents = Source.fromFile(result2).getLines.mkString
      //      println("file content : \n" + fileContents)
      //      println("----------------------------------")
      //      //      val status = hdfsHelper.hdfs.listStatus(new Path(dir))
      //      //      status.foreach(x => println(x.isDirectory))
      //      hdfsHelper.listFilesFrom(dir).foreach(x => println("file : " + x.getLen))
      //
      //      //////

      //      assert(data == result)
//      val stream = hdfsHelper.hdfs.open(new Path(dir + "/source/HDFSTestFile2.txt"))
//      val source = Source.fromInputStream(stream)
//      source.getLines() // Iterator[String]
//      org.apache.hadoop.io.MD5Hash.digest(stream)
    }
  }
}
