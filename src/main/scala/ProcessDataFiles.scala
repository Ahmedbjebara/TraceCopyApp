import hadoopIO.HDFSHelper
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{input_file_name, _}
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.sql.{DataFrame, SparkSession}


case class ProcessDataFiles(uri: String) {

  val hdfsHelper = HDFSHelper(uri)

  def parseSchema(schemaFile: String): StructType = {
    csvFileToStructType(schemaFile)
  }

  def load(destinationDirectory: String, schema: StructType, readMode: String, sourceFileFormat: String)(implicit spark: SparkSession): DataFrame = {

    spark.read.format("csv")
      .option("header", "true")
      .option("mode", readMode)
      .schema(schema)
      .load(destinationDirectory)
  }

  def process(loadedDataframe: DataFrame)(implicit spark: SparkSession): DataFrame = {
    tagWithFileName(loadedDataframe)
  }

  def tagWithFileName(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    spark.udf.register("file_name", (path: String) => path.substring(path.lastIndexOf("/") + 1, path.lastIndexOf(".")))
    df.withColumn("fileName", callUDF("file_name", input_file_name()))
  }


  def write(processedDataframe: DataFrame, partitionColumn: String, resultFile: String) {

    processedDataframe.show()
    processedDataframe
      .write
      .partitionBy(partitionColumn)
      .parquet(resultFile)
  }

  private def csvFileToStructType(schemaFile: String): StructType = {

    val stream = hdfsHelper.hdfs.open(new Path(schemaFile))

    val readLines = scala.io.Source.fromInputStream(stream)

    val content: String = readLines.takeWhile(_ != null).mkString
    readLines.close()
    val schematoArrayofString = content.split(",").map(line => {
      val name = line.substring(line.indexOf("\"") + 1, line.indexOf(("\""), line.indexOf("\"") + 1))
      val typeField = line.substring(line.indexOf(("\""), line.indexOf(":")) + 1, line.lastIndexOf("\""))

      StructField(name, replaceTypeWithDatatype(typeField), false)

    })
    StructType.apply(schematoArrayofString)

  }


  private def replaceTypeWithDatatype(typeName: String): DataType = {
    typeName match {
      case "String" => StringType
      case "Integer" => IntegerType
      case "Double" => DoubleType
      case "Boolean" => BooleanType

    }
  }

}
