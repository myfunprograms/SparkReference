package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicComplexType extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    runQuery(sparkSession)

    runJSONQuery(sparkSession)
  }

  private def runQuery(sparkSession: SparkSession) = {
    val df = readData(sparkSession)
    df.printSchema()

    structType(df)
    arraySplit(df)
    arrayContains(df)
    arrayExplode(df)
    mapType(df)
    toJSON(df)
    fromJSON(df)
  }

  private def runJSONQuery(sparkSession: SparkSession) = {
    val jsonDF = createJSONDF(sparkSession)
    jsonType(jsonDF)
  }

  // SELECT complex.* FROM complexDF
  private def structType(df: DataFrame) = {
    logger.info("Method structType ...")
    val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
    complexDF.show(2, false)
    complexDF.select("complex.*").show(2, false)
    complexDF.select("complex.Description").show(2, false)
  }

  // SELECT split(Description, ' ') FROM table
  // SELECT split(Description, ' ')[0] FROM table
  private def arraySplit(df: DataFrame) = {
    logger.info("Method arraySplit ...")
    val splitDF = df.select(split(col("Description"), " ").alias("array_col"))
    splitDF.show(2, false)
    splitDF.selectExpr("array_col[0]").show(2, false)
  }

  private def arrayContains(df: DataFrame) = {
    logger.info("Method arrayContains ...")
    val splitted = split(col("Description"), " ")
    df.select(array_contains(splitted, "WHITE").alias("array_col"))
      .show(2)
  }

  private def arrayExplode(df: DataFrame) = {
    logger.info("Method arrayExplode ...")
    val splitted = split(col("Description"), " ")
    df.withColumn("splitted", splitted)
      .withColumn("exploded", explode(col("splitted")))
      .select("Description", "InvoiceNo", "exploded")
      .show(5, false)
  }

  // SELECT map(Description, InvoiceNo) as complex_map
  // FROM table
  // WHERE Description IS NOT NULL
  private def mapType(df: DataFrame) = {
    logger.info("Method mapType ...")
    val complexDF = df.select(map(col("InvoiceNo"), col("Description")).alias("complex_map"))
    complexDF.show(2, false)
    complexDF.selectExpr("complex_map[536365]").show(2, false)
    complexDF.select(explode(col("complex_map"))).show(2, false)
  }

  private def toJSON(df: DataFrame) = {
    logger.info("Method toJSON ...")
    df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")))
      .show(2, false)
  }

  private def fromJSON(df: DataFrame) = {
    logger.info("Method fromJSON ...")
    val schema = new StructType(Array(
      new StructField("InvoiceNo", StringType, true),
      new StructField("Description",StringType,true)))
    val toJSON = df.selectExpr("(InvoiceNo, Description) as myStruct")
      .select(to_json(col("myStruct")).alias("new_JSON"))
    toJSON.select(from_json(col("new_JSON"), schema))
        .show(2, false)
  }

  // SELECT json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as res
  // FROM table
  private def jsonType(df: DataFrame) = {
    logger.info("Method jsonType ...")
    df.select(get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]"),
              json_tuple(col("jsonString"), "myJSONKey"))
      .show(false)
  }

  private def createJSONDF(session: SparkSession): DataFrame = {
    session.range(1)
      .selectExpr(
        """
          |'{"myJSONKey": {"myJSONValue": [1, 2, 3]}}' as jsonString
        """.stripMargin)
  }

  private def readData(session: SparkSession): DataFrame = {
    session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicComplexType")
      .master("local")
      .getOrCreate()
  }
}
