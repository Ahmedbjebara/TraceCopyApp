package sevices

import java.io.File
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.apache.hadoop.test.PathUtils


trait HDFSCluster {
  @transient private var hdfsCluster: MiniDFSCluster = _
  def startHDFS: Unit = {

    println("Starting HDFS Cluster...")
    val baseDir = new File(PathUtils.getTestDir(getClass()), "miniHDFS")
    print(baseDir)
    val conf = new Configuration()
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath())
    conf.setBoolean("dfs.webhdfs.enabled", true)
    val builder = new MiniDFSCluster.Builder(conf)
    hdfsCluster = builder.nameNodePort(9000).manageNameDfsDirs(true).manageDataDfsDirs(true).format(true).build()
    hdfsCluster.waitClusterUp()
  }
  def getNameNodeURI: String = "hdfs://localhost:" + hdfsCluster.getNameNodePort

  def shutdownHDFS: Unit = {
    println("Shutting down HDFS Cluster...")
    hdfsCluster.shutdown
  }
}
