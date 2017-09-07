package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BasicNumberStatType extends LazyLogging {
  private val FilePath = "src/main/data/retail-data/by-day/2010-12-01.csv"

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSession()
    val df = readData(sparkSession)
    df.printSchema()

    runQuery(df)
  }

  private def runQuery(df: DataFrame) = {
    desc(df)
    statCorr(df)
    statApproxQuantile(df)
    statFreqItems(df)
  }

  private def desc(df: DataFrame) = {
    logger.info("Method desc ...")
    df.describe().show()
  }

  // SELECT corr(quantity, UnitPrice) FROM table
  private def statCorr(df: DataFrame) = {
    logger.info("Method statCorr ...")
    df.select(corr("Quantity", "UnitPrice"))
      .show()
  }


  // SELECT corr(quantity, UnitPrice) FROM table
  private def statApproxQuantile(df: DataFrame) = {
    logger.info("Method statApproxQuantile ...")
    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    val result = df.stat.approxQuantile(colName, quantileProbs, relError)
    println(result.mkString(","))
  }

  private def statFreqItems(df: DataFrame) = {
    logger.info("MEthod statFreqItem ...")
    df.stat.freqItems(Seq("StockCode", "Quantity")).show()
  }

  private def readData(session: SparkSession): DataFrame = {
    session.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(FilePath)
  }

  private def getOrCreateSparkSession() = {
    SparkSession.builder()
      .appName("BasicNumberStatType")
      .master("local")
      .getOrCreate()
  }
}
