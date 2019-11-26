package hadoopIO

import org.scalatest.{BeforeAndAfterAll, Suite}

trait HDFSTest extends BeforeAndAfterAll with HDFSCluster {

  this: Suite =>

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

}
