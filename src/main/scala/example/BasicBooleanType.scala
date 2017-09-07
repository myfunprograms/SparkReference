package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object BasicBooleanType extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = readData(sparkSession)
    df.printSchema()

    runQuery(df)
  }

  private def runQuery(df: DataFrame) = {
    booleanEqualTo(df)
    booleanEqualSymbol(df)
    booleanOr(df)
    booleanColumn(df)
    booleanExpression(df)
  }

  private def booleanEqualTo(df: DataFrame) = {
    logger.info("Method booleanEqualTo ...")
    df.where(col("InvoiceNo").equalTo(536365))
      .select("InvoiceNo", "Description")
      .show(5, false)
  }

  private def booleanEqualSymbol(df: DataFrame) = {
    logger.info("Method booleanEqualTo ...")
    df.where(col("InvoiceNo") === 536365)
      .select("InvoiceNo", "Description")
      .show(5, false)
  }

  // SELECT * FROM table
  // WHERE StockCode IN ("DOT")
  //   AND (UnitPrice > 600
  //     OR instr(Description, "POSTAGE") >= 1)
  private def booleanOr(df: DataFrame) = {
    logger.info("Method booleanOr ...")
    val priceFilter = col("UnitPrice") > 600
    val descFilter = col("Description").contains("POSTAGE")
    df.where(col("StockCode").isin("DOT"))
      .where(priceFilter.or(descFilter))
      .show(5)
  }

  // SELECT UnitPrice,
  //        (StockCode in ('DOT') AND
  //          (UnitPrice > 600 OR
  //           instr(Description, "POSTAGE") >=1) AS isExpensive
  // FROM table
  // WHERE isExpensive = true
  private def booleanColumn(df: DataFrame) = {
    logger.info("Method booleanColumn ...")
    val DOTCodeFilter = col("StockCode").isin("DOT")
    val priceFilter = col("UnitPrice") > 600
    val descFilter = col("Description").contains("POSTAGE")
    df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descFilter)))
      .where("isExpensive")
      .select("UnitPrice", "isExpensive")
      .show(5)
  }

  private def booleanExpression(df: DataFrame) = {
    logger.info("Method booleanExpression ...")
    logger.info("Method booleanExpression - Use Expression...")
    df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
      .filter("isExpensive")
      .select("Description", "unitPrice")
      .show(5)

    logger.info("Method booleanExpression - Use DF...")
    df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
      .filter("isExpensive")
      .select("Description", "unitPrice")
      .show(5)
  }

  private def readData(session: SparkSession): DataFrame = {
    session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicDataType")
      .master("local")
      .getOrCreate()
  }
}
