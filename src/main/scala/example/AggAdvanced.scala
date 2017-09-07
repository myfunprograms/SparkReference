package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AggAdvanced extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = readData(sparkSession)
    df.printSchema()

    runQuery(df)
  }

  private def runQuery(df: DataFrame) = {
    aggWindow(df)
    aggRollup(df)
    aggRollupCompareGroupBy(df)
    aggCube(df)
    aggPivot(df)
  }

  // Window function explained:
  // https://stackoverflow.com/documentation/apache-spark/3903/window-functions-in-spark-sql#t=201709050436146239934
  //
  // 1) Create a window specification
  //    partitionBy: similar to group by
  //    orderBy: the ordering within a window
  //    rowsBetween: row to be included
  //
  // SELECT CustomerId, date, Quantity,
  //    rank(Quantity) OVER (PARTITION BY CustomerId, date
  //                         ORDER BY Quantity DESC NULLS LAST
  //                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rank,
  //    dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
  //                         ORDER BY Quantity DESC NULLS LAST
  //                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as dRank,
  //    max(Quantity) OVER (PARTITION BY CustomerId, date
  //                        ORDER BY Quantity DESC NULLS LAST
  //                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as maxPurchase
  // FROM table
  // WHERE CustomerId IS NOT NULL
  // ORDER BY CustomerId
  private def aggWindow(df: DataFrame) = {
    logger.info("Method aggWindow ...")
    val windowSpec = Window
      .partitionBy("customerId", "date")
      .orderBy(col("Quantity").desc)
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val maxQuantity = max(col("Quantity")).over(windowSpec)
    val purchaseDenseRank = dense_rank().over(windowSpec)
    val purchaseRank = rank().over(windowSpec)

    df.where("CustomerId IS NOT NULL")
      .orderBy("CustomerId")
      .select(
        col("CustomerId"), col("date"), col("Quantity"),
        purchaseRank.alias("quantityRank"),
        purchaseDenseRank.alias("quantityDenseRank"),
        maxQuantity.alias("maxQuantity"))
      .show(5, false)
  }

  // Rollup and cube explained
  // https://stackoverflow.com/questions/37975227/what-is-the-difference-between-cube-and-groupby-operators
  private def aggRollup(df: DataFrame) = {
    logger.info("Method aggRollup ...")
    val rollupDF = df.rollup("Date", "Country")
      .agg(sum("Quantity"))
      .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
      .orderBy("Date")
    rollupDF.show(10, false)
    rollupDF.where("Country IS NOT NULL").show(10, false)
    rollupDF.where("Date IS NULL").show(10, false)
  }

  private def aggRollupCompareGroupBy(df: DataFrame) = {
    logger.info("Method aggRollup ...")
    df.groupBy("Date", "Quantity")
      .agg(sum("Quantity"))
      .orderBy("Date")
      .show(10, false)
  }


  private def aggCube(df: DataFrame) = {
    logger.info("Method aggCube ...")
    val rollupDF = df.cube("Date", "Country")
      .agg(sum("Quantity"))
      .select("Date", "Country", "sum(Quantity)")
      .orderBy("Date")
    rollupDF.show(10, false)
  }

  private def aggPivot(df: DataFrame) = {
    logger.info("Method aggPivot ...")
    df.groupBy("Date")
      .pivot("Country")
      .agg("quantity" -> "sum")
      .show(2, false)
  }

  private def readData(session: SparkSession): DataFrame = {
    val df = session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
    df.withColumn("date", col("InvoiceDate").cast("date"))
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("AggAdvanced")
      .master("local")
      .getOrCreate()
  }
}
