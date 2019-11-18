package hadoopIO

import java.io.File

import org.scalatest.{BeforeAndAfterAll, Suite}

trait HDFSTest extends BeforeAndAfterAll with HDFSCluster {

  this: Suite =>

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startHDFS

    val url = getNameNodeURI
    val dir = getNameNodeURI + "/user"
    val testFile : File = new File("src/test/resources/HDFSTestFile.txt")
    val hdfsHelper = new HDFSHelper[File](url)
    val data: Int = 10
    hdfsHelper.write(testFile, dir)
    val result = hdfsHelper.read(dir)
    assert(data == result)

  }

  override protected def afterAll(): Unit = {

    shutdownHDFS

//    if (cluster != null) {
//      cluster.shutdown();
//      cluster = null;
//    }
//    if (_sparkSession != null) {
//      _sparkSession.stop();
//      _sparkSession = null;
//    }
  }
}
