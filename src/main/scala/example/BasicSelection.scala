package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicSelection extends LazyLogging {
  def main(args: Array[String]) = {
    val sparkSession = getOrCreateSession()

    val df = readJsonData(sparkSession)

    runDFQuery(sparkSession, df)
  }

  private def runDFQuery(session: SparkSession,
                         df: DataFrame) = {
    dfSimple(df)
    dfSimpleImplicit(session, df)
    dfSame(df)
    dfMultiple(df)
    dfExpr(df)
    dfExprOneline(df)
    dfExprAgg(df)
    dfExprLit(df)
  }

  private def dfSimple(df: DataFrame): Unit = {
    logger.info("Method dfSimple() ... ")
    df.select("DEST_COUNTRY_NAME").show(2)
  }

  private def dfSimpleImplicit(session: SparkSession,
                               df: DataFrame) = {
    logger.info("Method dfSimpleImplicit() ... ")

    import session.implicits._
    df.select($"DEST_COUNTRY_NAME",
              'DEST_COUNTRY_NAME)
      .show(2)
  }

  private def dfSame(df: DataFrame): Unit = {
    logger.info("Method dfSame() ... ")
    df.select(df.col("DEST_COUNTRY_NAME"),
              col("DEST_COUNTRY_NAME"),
              column("DEST_COUNTRY_NAME"),
              expr("DEST_COUNTRY_NAME"))
      .show(2)
  }

  private def dfMultiple(df: DataFrame): Unit = {
    logger.info("Method dfMultiple() ... ")
    df.select("DEST_COUNTRY_NAME",
              "ORIGIN_COUNTRY_NAME")
      .show(2)
  }

  private def dfExpr(df: DataFrame): Unit = {
    logger.info("Method dfExpr() ... ")
    df.select(expr("DEST_COUNTRY_NAME AS destination"))
      .alias("DEST_COUNTRY_NAME")
      .show(2)
  }

  private def dfExprOneline(df: DataFrame): Unit = {
    logger.info("Method dfExprOneline() ... ")
    df.selectExpr("*",
                  "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS WITHIN_COUNTRY ")
      .show(2)
  }

  private def dfExprAgg(df: DataFrame): Unit = {
    logger.info("Method dfExprAgg() ... ")
    df.selectExpr("avg(count)",
                  "count(distinct(DEST_COUNTRY_NAME))")
      .show(1)
  }

  private def dfExprLit(df: DataFrame): Unit = {
    logger.info("Method dfExprLit() ... ")
    df.select(expr("*"),
              lit(1).as("One"))
    df.show(2)
  }

  private def readJsonData(spark: SparkSession): DataFrame = {
    spark.read.json("src/main/data/flight-data/json/2015-summary.json")
  }

  private def getOrCreateSession() = {
    SparkSession.builder()
      .appName("introduction")
      .master("local")
      .getOrCreate()
  }
}
