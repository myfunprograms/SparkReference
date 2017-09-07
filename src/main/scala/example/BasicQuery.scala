package example

import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicQuery {
  val logger: Logger = Logger.getLogger(getClass().getName())

  def main(args: Array[String]) = {
    PropertyConfigurator.configure("src/main/resources/log4j.properties")

    val sparkSession = getOrCreateSession()

    val df = readJsonData(sparkSession)
    runSQLs(sparkSession, df)
    runDFQuery(df)
  }

  private def runSQLs(spark: SparkSession,
                      df: DataFrame) = {
    sqlGroupBy(spark, df)
    sqlMax(spark, df)
    sqlTopFive(spark, df)
  }

  private def sqlGroupBy(spark: SparkSession,
                         df: DataFrame): Unit = {
    val tableName = "flight_data_2015"
    df.createOrReplaceTempView(tableName)
    val sql = spark.sql(
      s"""
        SELECT DEST_COUNTRY_NAME, count(*) AS count_sum
        FROM $tableName
        GROUP BY DEST_COUNTRY_NAME
      """.stripMargin)
    sql.explain()
    sql.show(5)
  }

  private def sqlMax(spark: SparkSession,
                     df: DataFrame): Unit = {
    val tableName = "flight_data_2015"
    df.createOrReplaceTempView(tableName)
    val sql = spark.sql(
      s"""
        SELECT max(count)
        FROM $tableName
      """.stripMargin)
    sql.show(1)
    logger.info(sql.take(1).mkString(","))
  }

  private def sqlTopFive(spark: SparkSession,
                         df: DataFrame): Unit = {
    val tableName = "flight_data_2015"
    df.createOrReplaceTempView(tableName)
    val sql = spark.sql(
      s"""
        SELECT DEST_COUNTRY_NAME, sum(count) as COUNT
        FROM $tableName
        GROUP BY DEST_COUNTRY_NAME
        ORDER BY COUNT DESC
        LIMIT 5
      """.stripMargin)
    val result = sql.collect()
    logger.info(result.mkString(","))
  }

  private def runDFQuery(df: DataFrame) = {
    dfGroupBy(df)
    dfMax(df)
    dfTopFive(df)
  }

  private def dfGroupBy(df: DataFrame): Unit = {
    val query = df.groupBy("DEST_COUNTRY_NAME")
      .count()
    query.explain()
    query.show(5)
  }

  private def dfMax(df: DataFrame): Unit = {
    val query = df.select(max("count"))
    query.show(1)
  }

  private def dfTopFive(df: DataFrame): Unit = {
    val result = df.groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "DEST_TOTAL")
      .sort(desc("DEST_TOTAL"))
      .limit(5)
      .collect()
    logger.info(result.mkString(","))
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
