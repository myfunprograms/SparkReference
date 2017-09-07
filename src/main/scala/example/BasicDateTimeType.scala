package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object BasicDateTimeType extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()

    runQuery(sparkSession)
  }

  private def runQuery(session: SparkSession) = {
    current(session)
    dateAddSub(session)
    dateDiff(session)
    dateMonthsBetween(session)
    dateConversion(session)
    dateFilter(session)
  }

  private def current(session: SparkSession) = {
    logger.info("Method current ...")
    val df = createDF(session)
    df.createOrReplaceTempView("dateTable")
    df.printSchema()
    df.show(2)
  }

  private def dateAddSub(session: SparkSession) = {
    logger.info("Method dateAddSub ...")
    val df = createDF(session)
    df.select(date_sub(col("today"), 5).alias("Past"),
              date_add(col("today"), 5).alias("Future"))
      .show(1)
  }

  // SELECT date_sub(today, 5), date_add(today, 5) FROM table
  private def dateDiff(session: SparkSession) = {
    logger.info("Method dateDiff ...")
    val df = createDF(session)
    df.withColumn("week_ago", date_sub(col("today"), 7))
      .select(datediff(col("week_ago"), col("today")))
      .show(1)
  }

  // SELECT to_date('2016-01-01') as start,
  //        to_date('2017-05-22') as end,
  //        months_between(start, end)
  // FROM table
  private def dateMonthsBetween(session: SparkSession) = {
    logger.info("Method dateMonthsBetween ...")
    val df = createDF(session)
    df.select(to_date(lit("2016-01-01")).alias("start"),
              to_date(lit("2017-05-22")).alias("end"))
      .select(months_between(col("start"), col("end")))
      .show(1)
  }

  // SELECT
  //   to_date(cast(unix_timestamp('2017-12-11', 'yyyy-dd-MM') as timestamp)) as date1,
  //   to_date(cast(unix_timestamp('2017-20-12', 'yyyy-dd-MM') as timestamp)) as date2)
  // FROM table
  private def dateConversion(session: SparkSession) = {
    logger.info("Method dateConversion ...")
    val dateFormat = "yyyy-dd-MM"
    session.range(1)
      .select(timestampCast(dateFormat, "2017-12-11", "date1"),
              timestampCast(dateFormat, "2017-20-12", "date2"))
      .show(1)
  }

  private def dateFilter(session: SparkSession) = {
    logger.info("Method dateFilter ...")
    val schema = new StructType(Array(
      new StructField("ID", IntegerType, true),
      new StructField("DATE_STRING", StringType, true)))
    val data = Seq(Row(1, "2017-01-01"),
                   Row(2, "2017-02-02"),
                   Row(3, "2017-03-03"))
    val rdd = session.sparkContext.parallelize(data)
    val df = session.createDataFrame(rdd, schema)
    df.withColumn("DATE", to_date(col("DATE_STRING")))
      .filter(col("DATE") > lit("2017-02-01"))
      .show()
  }

  private def createDF(session: SparkSession) = {
    session.range(10)
      .withColumn("today", current_date())
      .withColumn("now", current_timestamp())
  }

  private def timestampCast(dateFormat: String,
                            date: String,
                            name: String) = {
    to_date(unix_timestamp(lit(date), dateFormat).cast("timestamp"))
      .alias(name)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicDateTimeType")
      .master("local")
      .getOrCreate()
  }
}
