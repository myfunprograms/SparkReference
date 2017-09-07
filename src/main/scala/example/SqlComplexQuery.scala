package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

object SqlComplexQuery extends LazyLogging {
  private val JsonPath = "src/main/data/flight-data/json/2015-summary.json"

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    val df = readJsonData(spark)

    runQuery(spark, df)
  }

  private def runQuery(spark: SparkSession, df: DataFrame) = {
    selectCase(spark, df)
    selectStruct(spark, df)
    selectListSet(spark, df)
    selectListSetExplode(spark, df)
  }

  private def selectCase(spark: SparkSession,
                         df: DataFrame) = {
    logger.info("Method selectCase ...")
    val view = "flights"
    df.createOrReplaceTempView(view)
    spark.sql(
      s"""
         |SELECT
         |    CASE WHEN DEST_COUNTRY_NAME = 'United States' THEN 1
         |         WHEN DEST_COUNTRY_NAME = 'Egypt' THEN 0
         |         ELSE -1
         |    END AS DEST_COUNTRY_CODE
         |FROM $view
       """.stripMargin)
      .show(5, false)
  }

  private def selectStruct(spark: SparkSession,
                         df: DataFrame) = {
    logger.info("Method selectStruct ...")
    val view = "flights"
    df.createOrReplaceTempView(view)
    spark.sql(
      s"""
         |CREATE TEMP VIEW nested_data
         |AS
         |    SELECT (DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME) AS country,
         |           count
         |    FROM $view
       """.stripMargin)
    spark.sql(
      """
         |SELECT country.DEST_COUNTRY_NAME, count
         |FROM nested_data
       """.stripMargin)
      .show(2, false)
    spark.sql(
      """
        |SELECT country.*, count
        |FROM nested_data
      """.stripMargin)
      .show(2, false)
  }

  // Use collect_set(), collect_list() with aggregation
  private def selectListSet(spark: SparkSession,
                        df: DataFrame) = {
    logger.info("Method selectListSet ...")
    val view = "flights"
    df.createOrReplaceTempView(view)

    spark.sql(
      s"""
         |SELECT DEST_COUNTRY_NAME as new_name,
         |       collect_list(count) AS flight_counts,
         |       collect_set(ORIGIN_COUNTRY_NAME) as origin_set,
         |       collect_list(count)[0] AS first_flight_count
         |FROM $view
         |WHERE DEST_COUNTRY_NAME IN ('United States', 'China')
         |GROUP BY DEST_COUNTRY_NAME
       """.stripMargin)
      .show(false)
  }

  private def selectListSetExplode(spark: SparkSession,
                            df: DataFrame) = {
    logger.info("Method selectListSetExplode ...")
    val view = "flights"
    df.createOrReplaceTempView(view)

    spark.sql(
      s"""
         |CREATE TEMP VIEW flight_view
         |AS
         |  SELECT DEST_COUNTRY_NAME as new_name,
         |         collect_list(count) AS flight_counts,
         |         collect_set(ORIGIN_COUNTRY_NAME) as origin_set,
         |         collect_list(count)[0] AS first_flight_count
         |FROM $view
         |WHERE DEST_COUNTRY_NAME IN ('United States', 'China')
         |GROUP BY DEST_COUNTRY_NAME
       """.stripMargin)

    spark.sql(
      s"""
         |SELECT explode(flight_counts), new_name
         |FROM flight_view
       """.stripMargin)
      .show(false)
  }

  private def readJsonData(spark: SparkSession) = {
    spark.read.json(JsonPath)
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .master("local")
      .appName("SqlComplexQuery")
      .getOrCreate()
  }
}
