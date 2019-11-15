package hadoopIO

import java.io.File

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.{HdfsConfiguration, MiniDFSCluster}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait HdfsTest extends BeforeAndAfterAll {

  this: Suite =>


  var conf: HdfsConfiguration = new HdfsConfiguration
  var fs: FileSystem = _
  var cluster: MiniDFSCluster = _
  var _sparkSession: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val testDataPath = new File(getClass.getResource("/minicluster").getFile)

    System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA)

    val testDataCluster1: File = new File(testDataPath, "cluster1")
    val c1PathStr = testDataCluster1.getAbsolutePath
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, "c1PathStr")
    cluster = new MiniDFSCluster.Builder(conf).build

    fs = FileSystem.get(conf)

    _sparkSession = SparkSession
      .builder()
      .appName("SparkSchema")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()


  }

  override protected def afterAll(): Unit = {
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
    if (_sparkSession != null) {
      _sparkSession.stop();
      _sparkSession = null;
    }
  }
}
