package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicNumberType extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = readData(sparkSession)
    df.printSchema()

    runQuery(df)
  }

  private def runQuery(df: DataFrame) = {
    numberPow(df)
    numberPowExpr(df)
    numberRound(df)
    numberBround(df)
    numberCorr(df)
    desc(df)
  }

  private def numberPow(df: DataFrame) = {
    logger.info("Method numberPow ...")
    val fabricatedQuatity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(col("CustomerId"), fabricatedQuatity.alias("realQuantity"))
      .show(2)
  }

  // SELECT customerId, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity
  private def numberPowExpr(df: DataFrame) = {
    logger.info("Method numberPowExpr ...")
    val fabricatedQuatity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.selectExpr("CustomerId",
      "(POWER(Quantity * UnitPrice, 2.0) + 5) AS realQuantity ")
      .show(2)
  }

  private def numberRound(df: DataFrame) = {
    logger.info("Method numberRound ...")
    df.select(col("UnitPrice"),
              round(col("UnitPrice"), 1).alias("rounded"))
      .show(2)
  }

  // SELECT round(2.5), bround(2.5)
  private def numberBround(df: DataFrame) = {
    logger.info("Method numberBround ...")
    df.select(round(lit(2.5)), bround(lit(2.5)))
      .show(2)
  }

  // SELECT corr(quantity, UnitPrice) FROM table
  private def numberCorr(df: DataFrame) = {
    logger.info("Method numberCorr ...")
    df.select(corr("Quantity", "UnitPrice"))
      .show()
  }

  private def desc(df: DataFrame) = {
    logger.info("Method desc ...")
    df.describe().show()
  }

  private def readData(session: SparkSession): DataFrame = {
    session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicNumberType")
      .master("local")
      .getOrCreate()
  }
}
