package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{AnalysisException, SparkSession}

case class Table(database: String, tableName: String, isTemporary: Boolean)

object SqlTableQuery extends LazyLogging {
  private val JsonPath = "src/main/data/flight-data/json/2015-summary.json"
  private val CsvPath = "src/main/data/flight-data/csv/2015-summary.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = createSparkSession()

    runTableQuery(sparkSession)
    runInsertTableQuery(sparkSession)
  }

  private def runTableQuery(spark: SparkSession) = {
    showTable(spark)
    createTableJson(spark)
    createTableCSV(spark)
//    createTableFromOtherTable(spark)
    createTablePartitioned(spark)
    tableMetadata(spark)
  }

  private def runInsertTableQuery(spark: SparkSession) = {

  }

  private def showTable(spark: SparkSession) = {
    logger.info("Method showTable ...")
    import spark.implicits._
    spark.sql("SHOW TABLES IN default")
      .as[Table]
      .collect()
      .map(t =>
        try {
          spark.sql(s"DROP TABLE IF EXISTS ${t.tableName}").collect()
        } catch {
          case e: AnalysisException => {
            spark.sql(s"DROP VIEW IF EXISTS ${t.tableName}").collect()
          }
        }
      )
  }

  private def createTableJson(spark: SparkSession) = {
    logger.info("Method createTableJson ...")
    createJsonTable(spark)
    showTables(spark)
    dropTable(spark, "flights")
    showTables(spark)
  }

  private def createTableCSV(spark: SparkSession) = {
    logger.info("Method createTableCSV ...")
    spark.sql(
      s"""
         |CREATE TABLE flights (
         |    DEST_COUNTRY_NAME STRING,
         |    ORIGIN_COUNTRY_NAME STRING,
         |    count LONG)
         |USING JSON
         |OPTIONS (
         |    inferSchema true,
         |    header true,
         |    path '$CsvPath')
      """.stripMargin)
    showTables(spark)
    dropTable(spark, "flights")
    showTables(spark)
  }

//  Problem: Hive support is required to...
//  private def createTableFromOtherTable(spark: SparkSession) = {
//    logger.info("Method createTableFromOtherTable ...")
//    createJsonTable(spark)
//    val selectTable = spark.sql(
//      """
//        |CREATE TABLE IF NOT EXISTS flights_from_select
//        |AS
//        |    SELECT * FROM flights
//        |    LIMIT 5
//      """.stripMargin)
//    selectTable.show()
//    val insertTable = spark.sql(
//      """
//        |INSERT INTO flights_from_select
//        |    SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count
//        |    FROM flights
//        |    LIMIT 20
//      """.stripMargin)
//    insertTable.collect()
//    spark.sql("DROP TABLE IF EXISTS flights_from_select")
//    spark.sql("SHOW TABLES").show()
//  }

  private def createTablePartitioned(spark: SparkSession) = {
    logger.info("Method createTablePartitioned ...")
    createJsonTable(spark)
    val tableName = "partitioned_flights"
    spark.sql(
      s"""
        |CREATE TABLE $tableName
        |USING parquet
        |PARTITIONED BY (DEST_COUNTRY_NAME)
        |AS
        |    SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME, count
        |    FROM flights
        |    LIMIT 5
      """.stripMargin)
    showTables(spark)
    showTableContent(spark, tableName)
    spark.sql(
      s"""
        |SHOW PARTITIONS $tableName
      """.stripMargin)
      .show(false)
    spark.sql( // No change?
      s"""
         |INSERT INTO $tableName
         |   PARTITION (DEST_COUNTRY_NAME = 'United States')
         |   SELECT ORIGIN_COUNTRY_NAME, count FROM flights
         |   WHERE DEST_COUNTRY_NAME = 'United States'
         |   LIMIT 10
       """.stripMargin
    )
    showTableContent(spark, tableName)
    dropTable(spark, "flights", "partitioned_flights")
    showTables(spark)
  }

  private def tableMetadata(spark: SparkSession) = {
    logger.info("Method tableMetadata ...")
    createJsonTable(spark)
    spark.sql(
      """
        |DESCRIBE TABLE flights
      """.stripMargin)
      .show()
  }

  private def createJsonTable(spark: SparkSession) = {
    spark.sql(
      s"""
         |CREATE TABLE flights (
         |    DEST_COUNTRY_NAME STRING,
         |    ORIGIN_COUNTRY_NAME STRING,
         |    count LONG)
         |USING JSON
         |OPTIONS (path '$JsonPath')
      """.stripMargin)
  }

  private def showTables(spark: SparkSession) = {
    spark.sql("SHOW TABLES").show()
  }

  private def dropTable(spark: SparkSession, tableNames: String*) = {
    tableNames.foreach(tableName =>
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    )
  }

  private def showTableContent(spark: SparkSession, tableName: String) = {
    spark.sql(
      s"""
        |SELECT * FROM $tableName
      """.stripMargin)
      .show()
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("SqlTableQuery")
      .master("local")
      .getOrCreate()
  }
}
