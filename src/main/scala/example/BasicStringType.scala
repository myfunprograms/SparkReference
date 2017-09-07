package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicStringType extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = readData(sparkSession)
    df.printSchema()

    runQuery(df)
  }

  private def runQuery(df: DataFrame) = {
    stringInitcap(df)
    stringUpperLower(df)
    stringTrim(df)
    stringRegexpExtract(df)
    stringRegexpReplace(df)
    stringTranslate(df)
    stringContains(df)
    stringContainsVarArgs(df)
  }

  // SELECT initcap(Description) FROM table
  private def stringInitcap(df: DataFrame) = {
    logger.info("Method stringInitCap ...")
    df.select(initcap(col("Description"))).show(2)
  }

  // SELECT Description, upper(Description), lower(Description)
  // FROM table
  private def stringUpperLower(df: DataFrame) = {
    logger.info("Method stringUpperLower ...")
    df.select(col("Description"),
              upper(col("Description")),
              lower(col("Description")))
      .show(2)
  }

  // SELECT
  // ltrim('     HELLO     '),
  // rtrim('     HELLO     '),
  // trim('     HELLO     '),
  // lpad('HELLO', 10 , ' '),
  // rpad('HELLO', 10 , ' ')
  private def stringTrim(df: DataFrame) = {
    logger.info("Method stringTrim ...")
    df.select(
      ltrim(lit("    HELLO    ")).as("ltrim"),
      rtrim(lit("    HELLO    ")).as("rtrim"),
      trim(lit("    HELLO    ")).as("trim"),
      lpad(lit("HELLO"), 10, " ").as("lpad"),
      rpad(lit("HELLO"), 10, " ").as("rpad"))
    .show(2)
  }

  // SELECT
  //   regexp_replace(Description,
  //                  '(BLACK|WHITE|RED|GREEN|BLUE)',
  //                  1) as color_cleaned,
  //   Description
  // FROM table
  private def stringRegexpExtract(df: DataFrame) = {
    logger.info("Method stringRegexpExtract ...")
    val simpleColors = Seq("blue", "black", "red", "white", "green")
    val regexString = simpleColors.map(_.toUpperCase()).mkString("(", "|", ")")
    df.select(
      col("Description"),
      regexp_extract(col("Description"), regexString, 1).alias("first_color"))
      .show(2, false)
  }

  // SELECT
  //   regexp_replace(Description,
  //                  'BLACK|WHITE|RED|GREEN|BLUE',
  //                  'COLOR') as color_cleaned,
  //   Description
  // FROM table
  private def stringRegexpReplace(df: DataFrame) = {
    logger.info("Method stringRegexpReplace ...")
    val simpleColors = Seq("blue", "black", "red", "white", "green")
    val regexString = simpleColors.map(_.toUpperCase()).mkString("|")
    df.select(
      col("Description"),
      regexp_replace(col("Description"), regexString, "COLOR").alias("color_cleaned"))
      .show(2)
  }

  // SELECT Description,
  //        translate(Description, 'LEET', '1337'))
  // FROM table
  private def stringTranslate(df: DataFrame) = {
    logger.info("Method stringTranslate ...")
    df.select(col("Description"),
      translate(col("Description"), "HANGING", "1337"))
      .show(2, false)
  }

  // SELECT Description FROM table
  // WHERE instr(Description, "BLACK") >= 1
  //    OR instr(Description, "WHITE") >= 1
  private def stringContains(df: DataFrame) = {
    logger.info("Method stringContains ...")
    val containsBlack = col("Description").contains("BLACK")
    val containsWhite = col("Description").contains("WHITE")
    df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
      .filter("hasSimpleColor")
      .select("Description")
      .show(2)
  }

  private def stringContainsVarArgs(df: DataFrame) = {
    logger.info("Method stringContainsVarArgs ...")
    val simpleColors = Seq("blue", "black", "red", "white", "green")
    val selectedColumns = simpleColors.map(color => {
      col("Description").contains(color.toUpperCase()).alias(s"is_$color")
    }) :+ expr("*")
    df.select(selectedColumns: _*)
      .where(col("is_white").or(col("is_red")))
      .select("Description")
      .show(3, false)
  }

  private def readData(session: SparkSession): DataFrame = {
    session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicStringType")
      .master("local")
      .getOrCreate()
  }
}
