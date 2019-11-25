import java.io.File

import configuration.{ArgFileConf, Config}
import hadoopIO.{HDFSCluster, HDFSHelper}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import sevices.{CopyFileService, CopyHiveTableService}

class TracedCopySuite extends FunSuite with BeforeAndAfterAll with Matchers with HDFSCluster {


  startHDFS

  val clusterURI = getNameNodeURI
  val rootDirectory = getNameNodeURI + "/test"
  val hdfsHelper = new HDFSHelper(clusterURI)

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

  val configPath = rootDirectory + "/config.xml"

  val lines = scala.io.Source.fromFile("src/test/resources/fichierArguments.xml").mkString

  hdfsHelper.writeInto(lines, configPath,hdfsHelper.hdfs)
  hdfsHelper.writeInto(lines, rootDirectory + "/source/HDFSTestFile2.txt",hdfsHelper.hdfs)
  hdfsHelper.writeInto(lines+"test2", rootDirectory + "/source/HDFSTestFile3.txt",hdfsHelper.hdfs)
  hdfsHelper.writeInto(lines+"test2", rootDirectory + "/destination/HDFSTestFile33.txt",hdfsHelper.hdfs)



  val stream = hdfsHelper.hdfs.open(new Path(configPath))

  def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

  readLines.takeWhile(_ != null).foreach(line => println(line))

  val stream2 = hdfsHelper.hdfs.open(new Path(configPath))

  def readLines2 = scala.io.Source.fromInputStream(stream2)

  val snapshot_id: String = readLines2.takeWhile(_ != null).mkString
  println("************" + snapshot_id + "*************")
  val config: Config = ArgFileConf.loadConfig(snapshot_id)

  config.tracemethod match {
    case "file" => CopyFileService(clusterURI).tracedCopy(config.sourceDirectory, config.destinationDirectory, config.tracePath, config.traceFileName)
    case "hivetable" => {
      CopyHiveTableService(clusterURI).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
      spark.sql("SELECT * FROM TRACETABLE").show()
    }




  }

//  config.tracemethod match {
//    case "file" => CopyFileService(url).tracedCopy(config.sourceDirectory, config.destinationDirectory, config.tracePath, config.traceFileName)
//    case "hivetable" => {
//      CopyHiveTableService(url).tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
//      spark.sql("SELECT * FROM TRACETABLE").show()
//    }
//  }

  //hdfsHelper.writeInto(lines, dir + "/trace/trace.csv",hdfsHelper.hdfs)
  hdfsHelper.hdfs.create(new Path(rootDirectory + "/source/HDFSTestFile33.txt"))
  hdfsHelper.writeInto(lines+"test2", rootDirectory + "/source/HDFSTestFile33.txt",hdfsHelper.hdfs)

 // CopyFileService(url).tracedCopy(config.sourceDirectory, config.destinationDirectory, config.tracePath, config.traceFileName)

  val stream3 = hdfsHelper.hdfs.open(new Path(rootDirectory + "/trace/trace.csv"))

  def readLines3 = scala.io.Source.fromInputStream(stream3)
  val snapshot_id3: String = readLines3.takeWhile(_ != null).mkString
  println("----------\n"+snapshot_id3+"--------------------")

  shutdownHDFS

}
