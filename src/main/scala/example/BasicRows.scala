package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BasicRows extends LazyLogging {
  def main(args: Array[String]) = {
    val sparkSession = getOrCreateSession()

    val df = readJsonData(sparkSession)

    runDFQuery(sparkSession, df)
  }

  private def runDFQuery(session: SparkSession,
                         df: DataFrame) = {
    dfFilter(df)
    dfWhere(df)
    dfCount(df)
    dfCountDistinct(df)
    dfSample(df)
    dfRandomSplit(df)
    dfUnion(session, df)
    dfSort(df)
    dfSortDesc(df)
    dfOrderBy(df)
    dfOrderByMultiple(df)
    dfLimit(df)

    dfPartitionAndCoalesce(df)
  }

  private def dfFilter(df: DataFrame): Unit = {
    logger.info("Method dfFilter() ... ")
    df.filter(col("count") > 2).show(2)
  }

  private def dfWhere(df: DataFrame) = {
    logger.info("Method dfWhere() ... ")
    df.where(col("count") > 2)
      .where(col("ORIGIN_COUNTRY_NAME") =!= "Cortia")
      .show(2)
  }

  private def dfCount(df: DataFrame): Unit = {
    logger.info("Method dfCount() ... ")
    val number = df.select("ORIGIN_COUNTRY_NAME",
              "DEST_COUNTRY_NAME")
      .count()
    println(number)
  }

  private def dfCountDistinct(df: DataFrame): Unit = {
    logger.info("Method dfCountDistinct() ... ")
    val number = df.select("DEST_COUNTRY_NAME")
        .distinct()
        .count()
    println(number)
  }

  private def dfSample(df: DataFrame): Unit = {
    logger.info("Method dfSample() ... ")
    val seed = 1
    val withReplacement = false
    val fraction = 0.01
    df.sample(withReplacement, fraction, seed).show(2)
  }

  private def dfRandomSplit(df: DataFrame): Unit = {
    logger.info("Method dfRandomSplit() ... ")
    val seed = 1
    val dfs = df.randomSplit(Array(0.75, 0.25), seed)
    println(dfs(0).count())
    println(dfs(1).count())
  }

  private def dfUnion(session: SparkSession,
                      df: DataFrame): Unit = {
    logger.info("Method dfUnion() ... ")
    val schema = df.schema

    val rdd = session.sparkContext
      .parallelize(
        Seq(Row("Country 1", "Country 2", 100L),
            Row("Country 1", "Country 3", 200L))
      )
    val newDF = session.createDataFrame(rdd, schema)
    newDF.where(col("count") === 100L)
      .union(df)
      .show(5)
  }

  private def dfSort(df: DataFrame): Unit = {
    logger.info("Method dfSort() ... ")
    df.sort("count", "DEST_COUNTRY_NAME").show(2)
  }

  private def dfSortDesc(df: DataFrame): Unit = {
    logger.info("Method dfSortDesc() ... ")
    df.sort(desc("count")).show(2)
  }

  private def dfOrderBy(df: DataFrame): Unit = {
    logger.info("Method dfOrderBy() ... ")
    df.orderBy("count", "DEST_COUNTRY_NAME").show(2)
  }

  private def dfOrderByMultiple(df: DataFrame): Unit = {
    logger.info("Method dfOrderByMultiple() ... ")
    df.orderBy(desc("count"),
               asc("DEST_COUNTRY_NAME"))
      .show(2)
  }

  private def dfLimit(df: DataFrame): Unit = {
    logger.info("Method dfLimit() ... ")
    df.orderBy("count", "DEST_COUNTRY_NAME").limit(2).show()
  }

  private def dfPartitionAndCoalesce(df: DataFrame): Unit = {
    logger.info("Method dfPartitionAndCoalesce() ... ")
    println(s"Original partition number is ${df.rdd.getNumPartitions}")

    val newDF1 = df.repartition(10, col("DEST_COUNTRY_NAME"))
    newDF1.show(2)
    println(s"Parition number after repartition is ${newDF1.rdd.getNumPartitions}")

    val newDF2 = newDF1.coalesce(2)
    newDF2.show(2)
    println(s"Partition number after coalesce is ${newDF2.rdd.getNumPartitions}")
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
