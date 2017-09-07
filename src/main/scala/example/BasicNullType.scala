package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BasicNullType extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = createDF(sparkSession)
    df.show()

    runQuery(df)
  }

  private def runQuery(df: DataFrame) = {
    nullDrop(df)
    nullDropAny(df)
    nullDropAll(df)
    nullDropColumns(df)

    nullFill(df)
    nullFillColumns(df)
    nullFillDiff(df)

    nullReplace(df)
  }

  private def nullDrop(df: DataFrame) = {
    logger.info("Method nullDrop ...")
    df.na.drop().show()
  }

  private def nullDropAny(df: DataFrame) = {
    logger.info("Method nullDropAny ...")
    df.na.drop("any").show()
  }

  private def nullDropAll(df: DataFrame) = {
    logger.info("Method nullDropAll ...")
    df.na.drop("all").show()
  }

  private def nullDropColumns(df: DataFrame) = {
    logger.info("Method nullDropColumns ...")
    df.na.drop("all", Seq("col2", "col3")).show()
  }

  private def nullFill(df: DataFrame) = {
    logger.info("Method nullFill ...")
    df.na.fill("Filled Value").show()
  }

  private def nullFillColumns(df: DataFrame) = {
    logger.info("Method nullFillColumns ...")
    df.na.fill("FilledValue", Seq("col2")).show()
  }

  private def nullFillDiff(df: DataFrame) = {
    logger.info("Method nullFillDiff ...")
    val filledValues = Map(
      "col2" -> "Filled2",
      "col3" -> "Filled3"
    )
    df.na.fill(filledValues).show()
  }

  private def nullReplace(df: DataFrame) = {
    logger.info("Method nullReplace ...")
    df.na.replace("col2", Map("" -> "UNKNOWN", "22"->"New 22")).show()
  }

  private def createDF(session: SparkSession): DataFrame = {
    val schema = new StructType(Array(
      new StructField("col1", StringType, false),
      new StructField("col2", StringType, true),
      new StructField("col3", StringType, true))
    )
    val data = Seq(Row("1", "11", "111"),
                   Row("2", "22", null),
                   Row("3", null, "333"),
                   Row("4", null, null),
                   Row("5", "", ""))
    val rdd = session.sparkContext.parallelize(data)
    session.createDataFrame(rdd, schema)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicDataType")
      .master("local")
      .getOrCreate()
  }
}
