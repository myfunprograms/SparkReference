package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object ReadFiles extends LazyLogging {
  private val JsonPath = "src/main/data/flight-data/json/2015-summary.json"
  private val CsvPath = "src/main/data/flight-data/csv/2015-summary.csv"
  private val WritePath = "temp"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSession()

    val allDFs = readAllDFs(sparkSession)
    allDFs foreach(examineSchema(_))

    writeDF(allDFs(0))
    readWriteParquetFile(sparkSession, allDFs(0))
  }

  private def readAllDFs(sparkSession: SparkSession): Seq[DataFrame] = {
    val jsonDF = readJsonData(sparkSession)
    val jsonDFWithSchema = readJsonDataWithSchema(sparkSession)
    val csvDFInferSchema = readCSVData(sparkSession)
    val csvDF = readCSVDataWithSchema(sparkSession, jsonDF.schema)
    val csvDFGeneric = readCSVDataGeneric(sparkSession, jsonDF.schema)
    val manualDF = createDFWithManualSchema(sparkSession)

    Seq[DataFrame](jsonDF, jsonDFWithSchema, csvDFInferSchema, csvDF, csvDFGeneric, manualDF)
  }

  private def writeDF(df: DataFrame) = {
    logger.info("Method writeDF ...")
    df.write.format("csv")
      .option("mode", "OVERWRITE")
      .option("dateFormat", "yyyy-MM-dd")
      .save(WritePath + "/csv")
  }

  private def readWriteParquetFile(spark: SparkSession,
                                   df:DataFrame) = {
    logger.info("Method readWriteParquetFile ...")
    val path = WritePath + "/parquet"
    df.write.format("parquet")
      .option("mode", "OVERWRITE")
      .option("dateFormat", "yyyy-MM-dd")
      .save(path)
    spark.read.format("parquet")
      .load(path)
      .show(2, false)
  }


  private def readJsonData(spark: SparkSession): DataFrame = {
    spark.read.json(JsonPath)
  }

  private def readJsonDataWithSchema(spark: SparkSession): DataFrame = {
    spark.read
      .schema(defineSchema())
      .json(JsonPath)
  }

  private def readCSVData(spark: SparkSession): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(CsvPath)
  }

  private def readCSVDataWithSchema(spark: SparkSession,
                                    schema: StructType): DataFrame = {
    spark.read
      .schema(schema)
      .option("header", "true")
      .csv(CsvPath)
  }

  private def readCSVDataGeneric(spark: SparkSession,
                                 schema: StructType) = {
    logger.info("Method readCSVDataGeneric ...")
    spark.read.format("csv")
      .schema(schema)
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(CsvPath)
  }

  private def createDFWithManualSchema(session: SparkSession): DataFrame = {
    val row = Seq(Row("dest", "origin", 1L))
    val rdd = session.sparkContext.parallelize(row)
    val df = session.createDataFrame(rdd, defineSchema())
    df.show()
    df
  }

  private def examineSchema(df: DataFrame) = {
    println(df.schema)
  }

  private def defineSchema() = {
    new StructType(Array(
      new StructField("DEST_COUNTRY_NAME", StringType, true),
      new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
      new StructField("count", LongType, false)
    ))
  }

  // To use method from any class, the class needs to implement Serializable
  private def getOrCreateSession() = {
    SparkSession.builder()
      .appName("introduction")
      .master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }
}
