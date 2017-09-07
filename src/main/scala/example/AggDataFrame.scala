package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AggDataFrame extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = readData(sparkSession)
    df.printSchema()

    runCountQuery(df)
    runSumQuery(df)
    runOtherQuery(df)
  }

  private def runCountQuery(df: DataFrame) = {
    aggCountAction(df)
    aggCount(df)
    aggCountDistinct(df)
    aggApproxCountDistinct(df)
  }

  private def runSumQuery(df: DataFrame) = {
    aggSum(df)
    aggSumDistinct(df)
  }

  private def runOtherQuery(df: DataFrame) = {
    aggFirstLast(df)
    aggMinMax(df)
    aggAvg(df)
    aggCollection(df)
  }

  // As action, 1) Get total amount 2) For cache in memory
  private def aggCountAction(df: DataFrame) = {
    logger.info("Method aggCountAction ...")
    val countNum = df.count()
    println(s"Total num =$countNum")
  }

  // SELECT count(*) FROM table
  private def aggCount(df: DataFrame) = {
    logger.info("Method aggCount ...")
    df.select(count("StockCode")).show()
    df.select(count("StockCode")).collect()
    df.select(count("*")).show()
  }

  // SELECT COUNT(DISTINCT * FROM table
  private def aggCountDistinct(df: DataFrame) = {
    logger.info("Method aggCountDistinct ...")
    df.select(countDistinct("StockCode")).show()
  }

  // SELECT round(2.5), bround(2.5)
  private def aggApproxCountDistinct(df: DataFrame) = {
    logger.info("Method aggApproxCountDistinct ...")
    df.select(approx_count_distinct("StockCode", 0.1)).show()
  }

  // SELECT first(StockCode), last(StockCode) FROM table
  private def aggFirstLast(df: DataFrame) = {
    logger.info("Method aggFirstLast ...")
    df.select(first("StockCode"), last("StockCode")).show()
  }

  // SELECT min(Quantity), max(Quantity) FROM table
  private def aggMinMax(df: DataFrame) = {
    logger.info("Method aggMinMax ...")
    df.select(min("Quantity"), max("Quantity")).show()
  }

  private def aggSum(df: DataFrame) = {
    logger.info("Method aggSum ...")
    df.select(sum("Quantity")).show()
  }

  private def aggSumDistinct(df: DataFrame) = {
    logger.info("Method aggSumDistinct ...")
    df.select(sumDistinct("Quantity")).show()
  }

  private def aggAvg(df: DataFrame) = {
    logger.info("Method aggAvg ...")
    df.select(count("Quantity").alias("totalCount"),
              sum("Quantity").alias("totalSum"),
              avg("Quantity").alias("totalAvg"),
              expr("mean(Quantity)").alias("mean"))
      .selectExpr("totalSum/totalCount", "totalAvg", "mean")
      .show()
  }

  // SELECT collect_set("Country"), collect_list("Country")
  // FROM table
  private def aggCollection(df: DataFrame) = {
    logger.info("Method aggCollection ...")
    df.agg(collect_set("Country"),
           collect_list("Country"))
      .show(false)
  }

  private def readData(session: SparkSession): DataFrame = {
    session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("AggDataFrame")
      .master("local")
      .getOrCreate()
  }
}
