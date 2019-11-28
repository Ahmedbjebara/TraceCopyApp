import configuration.{ArgFileConf, Config}
import hadoopIO.{HDFSCluster, HDFSHelper}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpec, Suite}

class ProcessDataSpec extends FlatSpec with Suite with HDFSCluster with BeforeAndAfterEach with BeforeAndAfterAll {

  behavior of "ProcessData"

  var clusterURI: String = _
  var rootDirectory: String = _
  var hdfsHelper: HDFSHelper = _
  implicit var spark: SparkSession = _
  var config: Config = _

  override def beforeAll(): Unit = {
    startHDFS()
    clusterURI = getNameNodeURI
    rootDirectory = getNameNodeURI + "/test"
    hdfsHelper = HDFSHelper(clusterURI)

    hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/destination"))
    hdfsHelper.hdfs.mkdirs(new Path(clusterURI + "/test/schema"))


    hdfsHelper.hdfs.create(new Path(rootDirectory + "/config.xml"))
    hdfsHelper.hdfs.create(new Path(rootDirectory + "/schema/schemaout.csv"))

    hdfsHelper.hdfs.create(new Path(clusterURI + "/test/destination/testFile1.csv"))

    spark = SparkSession
      .builder()
      .appName("SparkApp")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val configPath = rootDirectory + "/config.xml"
    val configSource = scala.io.Source.fromFile("src/test/resources/fichierArguments.xml")
    val schemaSource = scala.io.Source.fromFile("src/test/resources/schemaout.txt")
    val testFile1Source = scala.io.Source.fromFile("src/test/resources/exportVersion1.csv")
    val configContent = configSource.mkString
    val schemaContent = schemaSource.mkString
    val testFile1Content = testFile1Source.mkString
    hdfsHelper.writeInto(configContent, configPath, hdfsHelper.hdfs)
    hdfsHelper.writeInto(schemaContent, rootDirectory + "/schema/schemaout.csv", hdfsHelper.hdfs)
    hdfsHelper.writeInto(testFile1Content, clusterURI + "/test/destination/testFile1.csv", hdfsHelper.hdfs)


    val stream = hdfsHelper.hdfs.open(new Path(configPath))

    val configFileContent: String = scala.io.Source.fromInputStream(stream).takeWhile(_ != null).mkString
    config = ArgFileConf.loadConfig(configFileContent)

    super.beforeAll()
  }

  override def afterAll(): Unit = {
   super.afterAll()
  }

  override def beforeEach() {
    super.beforeEach()
  }

  it should "process the data of all files in destination" in {

    val schema = ProcessDataFiles(clusterURI).parseSchema(config.schemaFile)
    val loadedData: DataFrame = ProcessDataFiles(clusterURI).load(config.destinationDirectory, schema, config.readMode, config.sourceFileFormat)
    val processedData: DataFrame = ProcessDataFiles(clusterURI).process(loadedData)
    ProcessDataFiles(clusterURI).write(processedData, config.partitionColumn, config.resultFile)


  }

}
