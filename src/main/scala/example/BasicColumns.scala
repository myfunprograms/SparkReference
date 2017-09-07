package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicColumns extends LazyLogging {
  def main(args: Array[String]) = {
    val sparkSession = getOrCreateSession()

    val df = readJsonData(sparkSession)

    runDFQuery(sparkSession, df)
  }

  private def runDFQuery(session: SparkSession,
                         df: DataFrame) = {
    dfAddColumnSimple(df)
    dfAddColumnExpr(df)
    dfAddColumnRename(df)
    dfRenameColumn(df)
    dfKeyword(df)
    dfDropColumns(df)
    dfChangeSchema(df)
  }

  private def dfAddColumnSimple(df: DataFrame): Unit = {
    logger.info("Method dfAddColumn() ... ")
    df.withColumn("NUM_ONE", lit(1)).show(2)
  }

  private def dfAddColumnExpr(df: DataFrame) = {
    logger.info("Method dfAddColumnExpr() ... ")
    df.withColumn("WITHIN_COUNTRY",
                  expr("DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME"))
      .show(2)
  }

  private def dfAddColumnRename(df: DataFrame): Unit = {
    logger.info("Method dfAddColumnRename() ... ")
    val newDF = df.withColumn("destination",
                  col("DEST_COUNTRY_NAME"))
    println(newDF.columns.mkString(","))
  }

  private def dfRenameColumn(df: DataFrame): Unit = {
    logger.info("Method dfRenameColumn() ... ")
    df.withColumnRenamed("DEST_COUNTRY_NAME", "destination")
      .show(2)
  }

  private def dfKeyword(df: DataFrame): Unit = {
    logger.info("Method dfKeyword() ... ")
    val dfWithLongColName =
      df.withColumn("This Long Column-Name",
                    col("DEST_COUNTRY_NAME"))
    dfWithLongColName.selectExpr("`This Long Column-Name`",
                   "`This Long Column-Name` as `new col`")
      .show(2)
  }

  private def dfDropColumns(df: DataFrame): Unit = {
    logger.info("Method dfDropColumns() ... ")
    df.drop("count", "DEST_COUNTRY_NAME").show(2)
  }

  private def dfChangeSchema(df: DataFrame): Unit = {
    logger.info("Method dfChangeSchema() ... ")
    df.withColumn("count", col("count").cast("int"))
      .printSchema()
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
