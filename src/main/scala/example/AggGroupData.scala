package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

// Two steps. 1) group on the columns 2) apply the aggregation
object AggGroupData extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = readData(sparkSession)
    df.printSchema()

    runCountQuery(df)
    runOtherQuery(df)
  }

  private def runCountQuery(df: DataFrame) = {
    aggCountAction(df)
    aggCount(df)
  }

  private def runOtherQuery(df: DataFrame) = {
    aggMap(df)
  }

  // SELECT count(*) FROM table
  // GROUP BY InvoiceNo, CustomerId
  private def aggCountAction(df: DataFrame) = {
    logger.info("Method aggCountAction ...")
    df.groupBy("InvoiceNo", "CustomerId")
      .count()
      .show(2, false)
  }

  // SELECT count(Quantity)
  // FROM table
  // GROUP BY InvoiceNo
  private def aggCount(df: DataFrame) = {
    logger.info("Method aggCount ...")
    df.groupBy("InvoiceNo")
      .agg(count("Quantity").alias("quan"),
           expr("count(Quantity)"))
      .show(2, false)
  }

  // Map: the key is the column to aggregate, the value is the function
  // SELECT InvoiceNo, avg(Quantity), stddev_pop(Quantity)
  // FROM table
  // GROUP BY InvoiceNo
  private def aggMap(df: DataFrame) = {
    logger.info("Method aggMap ...")
    df.groupBy("InvoiceNo")
      .agg(
        "Quantity" -> "avg",
        "Quantity" -> "stddev_pop")
      .show(2, false)
  }

  private def readData(session: SparkSession): DataFrame = {
    session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("AggGroupData")
      .master("local")
      .getOrCreate()
  }
}
