package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

// View are equivalent to DataFrame of DataFrame
object SqlDatabaseQuery extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    showDatabases(spark)
    useDatabase(spark)
  }

  private def showDatabases(spark: SparkSession) = {
    logger.info("Method showDatabases ...")
    showDatabasesQuery(spark)
  }

  private def useDatabase(spark: SparkSession) = {
    logger.info("Method useDatabase ...")
    val name = "test"
    spark.sql(
      s"""
        |CREATE DATABASE test
      """.stripMargin
    )
    showDatabasesQuery(spark)
    spark.sql(
      s"""
         |USE $name
      """.stripMargin
    )
    showCurrentDatabasesQuery(spark)
    spark.sql(
      s"""
        |DROP DATABASE IF EXISTS $name
      """.stripMargin
    )
    showDatabasesQuery(spark)
  }

  private def showDatabasesQuery(spark: SparkSession) = {
    spark.sql(
      """
        |SHOW DATABASES
      """.stripMargin
    ).show(false)
  }

  private def showCurrentDatabasesQuery(spark: SparkSession) = {
    spark.sql(
      """
        |SELECT current_database()
      """.stripMargin
    ).show(false)
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("SqlDatabaseQuery")
      .master("local")
      .getOrCreate()
  }
}
