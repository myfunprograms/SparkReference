package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

// View are equivalent to DataFrame of DataFrame
object SqlViewQuery extends LazyLogging {
  private val JsonPath = "src/main/data/flight-data/json/2015-summary.json"

  private val TableName = "flights"
  private val ViewName = "us_view"

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    createJsonTable(spark)

    runView(spark)
    dropTable(spark)
  }

  private def runView(spark: SparkSession) = {
    createView(spark)
    createViewTemp(spark)
    createViewGlobalTemp(spark)
    explainView(spark)
  }

  private def createView(spark: SparkSession) = {
    logger.info("Method createView ...")
    spark.sql(
      s"""
        |CREATE OR REPLACE VIEW $ViewName AS
        |    SELECT *
        |    FROM $TableName
        |    WHERE DEST_COUNTRY_NAME = 'United States'
      """.stripMargin)
    showTables(spark)
    showTableContent(spark)
    dropView(spark)
    showTables(spark)
  }

  private def createViewTemp(spark: SparkSession) = {
    logger.info("Method createViewTemp ...")
    spark.sql(
      s"""
         |CREATE TEMP VIEW $ViewName AS
         |    SELECT *
         |    FROM $TableName
         |    WHERE DEST_COUNTRY_NAME = 'United States'
      """.stripMargin)
    showTables(spark)
    showTableContent(spark)
    dropView(spark)
    showTables(spark)
  }

  // TODO: Refactor the code, add check after the view is dropped
  // The global view is created at global_view database
  private def createViewGlobalTemp(spark: SparkSession) = {
    logger.info("Method createViewGlobalTemp ...")
    spark.sql(
      s"""
         |CREATE GLOBAL TEMP VIEW $ViewName AS
         |    SELECT *
         |    FROM $TableName
         |    WHERE DEST_COUNTRY_NAME = 'United States'
      """.stripMargin)
    showTables(spark)
    spark.sql(
      s"""
       |SELECT * FROM global_temp.$ViewName
      """.stripMargin)
      .show(5, false)
    spark.sql(
      s"""
         |DROP VIEW IF EXISTS global_temp.$ViewName
      """.stripMargin)
  }

  private def explainView(spark: SparkSession) = {
    logger.info("Method explainView ...")
    spark.sql(
      s"""
        |EXPLAIN
        |    SELECT * FROM $ViewName
      """.stripMargin)
      .show()
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

  private def dropView(spark: SparkSession) = {
    spark.sql(s"DROP VIEW IF EXISTS $ViewName")
  }

  private def showTableContent(spark: SparkSession) = {
    spark.sql(
      s"""
        |SELECT * FROM $ViewName
      """.stripMargin)
      .show(5, false)
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("SqlViewQuery")
      .master("local")
      .getOrCreate()
  }
}
