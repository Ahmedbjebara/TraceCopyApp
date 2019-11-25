import java.io.File

import configuration.{ArgFileConf, Config}
import hadoopIO.HDFSHelper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import sevices.{CopyFileService, CopyHiveTableService}

object AppClone {
  def main(args: Array[String]): Unit = {

   // System.setProperty("hadoop.home.dir", "C:\\winutils")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("SparkSchema")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()

    if (args.length < 1) {
      System.err.println(
        "Argument number's is not respected")
      System.exit(1)
    }

    val hdfsHelper = new HDFSHelper("url")
    val stream = hdfsHelper.hdfs.open(new Path(args(0)))
    val config: Config = ArgFileConf.loadConfig("stream")


    config.tracemethod match {
      case "file" => CopyFileService("").tracedCopy(config.sourceDirectory, config.destinationDirectory, config.tracePath, config.traceFileName)
      case "hivetable" => {
        CopyHiveTableService("").tabletracedCopy(config.sourceDirectory, config.destinationDirectory)
        spark.sql("SELECT * FROM TRACETABLE").show()
      }

    }


    val schema = ProcessDataFiles.parseSchema(config.schemaFile)
    val loadedData: DataFrame = ProcessDataFiles.load(config.destinationDirectory, schema, config.readMode, config.sourceFileFormat)
    val processedData: DataFrame = ProcessDataFiles.process(loadedData)
    ProcessDataFiles.write(processedData, config.partitionColumn, config.resultFile)


    Thread.sleep(100000000)

  }
}




