package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicUDFType extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = sparkSession.range(5).toDF("num")

    runQuery(sparkSession, df)
  }

  private def runQuery(session: SparkSession,
                       df: DataFrame) = {
    power3DF(session, df)
    power3DFFunc(session, df)
    power3SQL(session, df)
  }

  private def power3UDFFn() = {
    udf {(value: Double) =>
      Math.pow(value, 3)
    }
  }

  private def power3DF(session: SparkSession, df: DataFrame) = {
    logger.info("Method power3DF ...")

    def power3(number: Double): Double = Math.pow(number, 3)
    df.select(power3UDFFn()(col("num"))).show()
  }

  private def power3DFFunc(session: SparkSession, df: DataFrame) = {
    logger.info("Method power3DFFunc ...")
    df.select(power3UDFFn()(col("num"))).show()
  }

  private def power3SQL(spark: SparkSession, df: DataFrame) = {
    logger.info("Method power3SQL ...")

    def power3(number: Double): Double = Math.pow(number, 3)
    spark.udf.register("power3", power3(_:Double):Double)
    df.createOrReplaceTempView("temp")
    val sql = spark.sql(
      """
        SELECT power3(num) FROM temp
      """.stripMargin)
    sql.show()
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicUDFType")
      .master("local")
      .getOrCreate()
  }
}
