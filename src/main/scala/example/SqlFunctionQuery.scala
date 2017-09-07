package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

// View are equivalent to DataFrame of DataFrame
object SqlFunctionQuery extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()

    showFunction(spark)
    showSystemFunction(spark)
    showUserFunction(spark)
    findFunctions(spark)
    userDefinedFunction(spark)
  }

  private def showFunction(spark: SparkSession) = {
    logger.info("Method showFunction ...")
    spark.sql(
      s"""
         |SHOW FUNCTIONS
      """.stripMargin
    ).show()
  }

  private def showSystemFunction(spark: SparkSession) = {
    logger.info("Method showSystemFunction ...")
    spark.sql(
      s"""
         |SHOW SYSTEM FUNCTIONS
      """.stripMargin
    ).show()
  }

  private def showUserFunction(spark: SparkSession) = {
    logger.info("Method showUserFunction ...")
    spark.sql(
      s"""
         |SHOW USER FUNCTIONS
      """.stripMargin
    ).show()
  }

  private def findFunctions(spark: SparkSession) = {
    logger.info("Method findFunctions ...")
    spark.sql(
      s"""
         |SHOW FUNCTIONS 's*'
      """.stripMargin
    ).show()

    spark.sql(
      s"""
         |SHOW FUNCTIONS LIKE 'collect*'
      """.stripMargin
    ).show()
  }

  private def userDefinedFunction(spark: SparkSession) = {
    def power3(number: Double) = Math.pow(number, 3)

    spark.udf.register("power3", power3(_:Double): Double)

    spark.sql(
      s"""
        |SELECT 5, power3(5) AS cube
      """.stripMargin)
      .show()
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("SqlFunctionQuery")
      .master("local")
      .getOrCreate()
  }
}
