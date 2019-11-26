import configuration.{ArgFileConf, Config}
import hadoopIO.{HDFSCluster, HDFSHelper}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import sevices.{CopyFileService, CopyHiveTableService}

class HiveTracedCopySpec extends FlatSpec with Matchers with HDFSCluster {

  behavior of "TracedCopy"

  //BeforeAll

  startHDFS

  val clusterURI = getNameNodeURI
  val rootDirectory = getNameNodeURI + "/test"
  val hdfsHelper = HDFSHelper(clusterURI)

  hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/source"))
  hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/destination"))
  hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/trace"))

  hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile2.txt"))
  hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile3.txt"))

  hdfsHelper.hdfs.create(new Path(rootDirectory + "/trace/trace.csv"))
  hdfsHelper.hdfs.create(new Path(rootDirectory + "/config.xml"))

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkApp")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()


  spark.sql("TRUNCATE TABLE TRACETABLE")


  hdfsHelper.writeInto("HDFSTestFile2 content", rootDirectory + "/source/HDFSTestFile2.txt", hdfsHelper.hdfs)
  hdfsHelper.writeInto("HDFSTestFile3 content", rootDirectory + "/source/HDFSTestFile3.txt", hdfsHelper.hdfs)

  ////////////////////////////////////////////////////////////////////////////////
  val configPath = rootDirectory + "/config.xml"
  val configSource = scala.io.Source.fromFile("src/test/resources/fichierArguments.xml")
  val lines = configSource.mkString
  hdfsHelper.writeInto(lines, configPath, hdfsHelper.hdfs)


  val stream = hdfsHelper.hdfs.open(new Path(configPath))

  val configFileContent: String = scala.io.Source.fromInputStream(stream).takeWhile(_ != null).mkString
  val config: Config = ArgFileConf.loadConfig(configFileContent)

  it should "make traced copy with  an empty destination and trace " in {


    CopyHiveTableService(clusterURI).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)



  }
  it should "make traced copy when file checksum already exist in destination " in {

    hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile33.txt"))
    hdfsHelper.writeInto("HDFSTestFile3 content", rootDirectory + "/source/HDFSTestFile33.txt", hdfsHelper.hdfs)

    CopyHiveTableService(clusterURI).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)


  }

  it should "make traced copy when file name already exist in destination " in {

    hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile3.txt"))
    hdfsHelper.writeInto("HDFSTestFile3 new content", rootDirectory + "/source/HDFSTestFile3.txt", hdfsHelper.hdfs)

    CopyHiveTableService(clusterURI).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
    spark.sql("SELECT * FROM TRACETABLE").show()
    shutdownHDFS
  }

  //AfterAll

  //
}
