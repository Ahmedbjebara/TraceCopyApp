import java.io.File

import configuration.{ArgFileConf, Config}
import hadoopIO.{HDFSCluster, HDFSHelper}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import sevices.{CopyFileService, CopyHiveTableService}

class TracedCopySuite extends FunSuite with BeforeAndAfterAll with Matchers with HDFSCluster {


  startHDFS

  val url = getNameNodeURI
  val dir = getNameNodeURI + "/test"
  val hdfsHelper = new HDFSHelper[File](url)

  hdfsHelper.hdfs.mkdirs(new Path(url + "/test/source"))
  hdfsHelper.hdfs.mkdirs(new Path(url + "/test/destination"))
  hdfsHelper.hdfs.mkdirs(new Path(url + "/test/trace"))

  hdfsHelper.hdfs.create(new Path(dir + "/source/HDFSTestFile2.txt"))
//  hdfsHelper.hdfs.create(new Path(dir + "/source/HDFSTestFile3.txt"))
//  hdfsHelper.hdfs.create(new Path(dir + "/source/HDFSTestFile4.txt"))
  hdfsHelper.hdfs.create(new Path(dir + "/trace/trace.csv"))
  hdfsHelper.hdfs.create(new Path(dir + "/config.xml"))

  hdfsHelper.listFilesFrom(dir + "/source").foreach(x => println("file : " + x._1))

  implicit val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkSchema")
    .config("spark.master", "local[*]")
    .enableHiveSupport()
    .getOrCreate()

  val configPath = dir + "/config.xml"

  val lines = scala.io.Source.fromFile("src/test/resources/fichierArguments.xml").mkString

  hdfsHelper.writeInto(lines, configPath,hdfsHelper.hdfs)
  hdfsHelper.writeInto(lines, dir + "/source/HDFSTestFile2.txt",hdfsHelper.hdfs)



  val stream = hdfsHelper.hdfs.open(new Path(configPath))

  def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

  readLines.takeWhile(_ != null).foreach(line => println(line))

  val stream2 = hdfsHelper.hdfs.open(new Path(configPath))

  def readLines2 = scala.io.Source.fromInputStream(stream2)

  val snapshot_id: String = readLines2.takeWhile(_ != null).mkString
  println("************" + snapshot_id + "*************")
  val config: Config = ArgFileConf.loadConfig(snapshot_id)

  config.tracemethod match {
    case "file" => CopyFileService(url).tracedCopy(config.sourceDirectory, config.destinationDirectory, config.tracePath, config.traceFileName)
    case "hivetable" => {
      CopyHiveTableService.tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
      spark.sql("SELECT * FROM TRACETABLE").show()
    }




  }

  val stream3 = hdfsHelper.hdfs.open(new Path(dir + "/trace/trace.csv"))

  def readLines3 = scala.io.Source.fromInputStream(stream3)
  val snapshot_id3: String = readLines3.takeWhile(_ != null).mkString
  println("----------"+snapshot_id3+"--------------------")

  shutdownHDFS

}
