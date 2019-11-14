package sevices

import org.scalatest.FlatSpec

class CopyFileServiceSpec extends FlatSpec with HDFSCluster {
  startHDFS

  behavior of "CopyFileServiceSpec"

  it should "tracedCopy" in {

  }
//  Thread.sleep(12000)
//  shutdownHDFS
}
