package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object SqlSubQuery extends LazyLogging {
  private val JsonPath = "src/main/data/flight-data/json/2015-summary.json"

  private val TableName = "flights"
  private val ViewName = "us_view"

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    createJsonTable(spark)

    runSubQuery(spark)
    dropTable(spark)
  }

  private def runSubQuery(spark: SparkSession) = {
    uncorrelatedQuery(spark)
    correlatedQuery(spark)
    uncorrelatedScalarQuery(spark)
  }

  private def uncorrelatedQuery(spark: SparkSession) = {
    logger.info("Method uncorrelatedQUery ...")
    spark.sql(
      s"""
        |SELECT *
        |FROM $TableName
        |WHERE
        |    ORIGIN_COUNTRY_NAME IN (
        |        SELECT DEST_COUNTRY_NAME
        |        FROM $TableName
        |        GROUP BY DEST_COUNTRY_NAME
        |        ORDER BY sum(count) DESC
        |        LIMIT 5)
      """.stripMargin)
      .show()
  }

  // Use information from the outer scope in the inner scope
  private def correlatedQuery(spark: SparkSession) = {
    logger.info("Method correlatedQuery ...")
    spark.sql(
      s"""
         |SELECT *
         |FROM $TableName f1
         |WHERE
         |    EXISTS (
         |        SELECT 1
         |        FROM $TableName f2
         |        WHERE f1.DEST_COUNTRY_NAME = f2.ORIGIN_COUNTRY_NAME)
         |    AND EXISTS (
         |        SELECT 1
         |        FROM $TableName f2
         |        WHERE f1.ORIGIN_COUNTRY_NAME = f2.DEST_COUNTRY_NAME)
         |LIMIT 5
      """.stripMargin)
        .show(false)
  }

  private def uncorrelatedScalarQuery(spark: SparkSession) = {
    logger.info("Method uncorrelatedScalarQuery ...")
    spark.sql(
      s"""
          |SELECT *,
          |    (SELECT max(count) FROM $TableName) AS maxium
          |FROM $TableName
      """.stripMargin)
      .show(5, false)
  }

  private def createJsonTable(spark: SparkSession) = {
    spark.sql(
      s"""
         |CREATE TABLE $TableName (
         |    DEST_COUNTRY_NAME STRING,
         |    ORIGIN_COUNTRY_NAME STRING,
         |    count LONG)
         |USING JSON
         |OPTIONS (path '$JsonPath')
      """.stripMargin)
    showTables(spark)
  }

  private def showTables(spark: SparkSession) = {
    spark.sql("SHOW TABLES").show()
  }

  private def dropTable(spark: SparkSession) = {
    spark.sql(s"DROP TABLE IF EXISTS $TableName")
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("SqlViewQuery")
      .master("local")
      .getOrCreate()
  }
}
