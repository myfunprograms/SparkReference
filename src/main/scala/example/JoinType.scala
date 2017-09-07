package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object JoinType extends LazyLogging {
  val spark = createSparkSession()
  val personDF = createPersonDF(spark)
  val graduateDF = createGraduateProgramDF(spark)
  val statusDF = createStatusDF(spark)

  val joinPersonGraduate =
    personDF("graduate_program") === graduateDF("id")

  def main(args: Array[String]) = {
    runQuery()
  }

  private def runQuery() = {
    innerJoin()
    outerJoin()
    leftOuterJoin()
    rightOuterJoin()
    leftSemiJoin()
    leftAntiJoin()
    crossJoin()
    complexTypeJoin()
    joinWithDupColumnName()
    joinWithBroadcastSpecified()
  }

  // SELECT *
  // FROM person
  // INNER JOIN graduateProgram
  //    ON person.graduate_program = graduateProgram.id
  private def innerJoin() = {
    logger.info("Method innerJoin ...")
    val joinType = "inner"
    personDF.join(graduateDF, joinPersonGraduate, joinType)
      .show()
  }

  // SELECT *
  // FROM person
  // OUTER JOIN graduateProgram
  //    ON person.graduate_program = graduateProgram.id
  private def outerJoin() = {
    logger.info("Method outerJoin ...")
    val joinType = "outer"
    personDF.join(graduateDF, joinPersonGraduate, joinType)
      .show()
  }

  // SELECT *
  // FROM person
  // JOIN graduateProgram
  //    ON person.graduate_program = graduateProgram.id
  private def leftOuterJoin() = {
    logger.info("Method leftOuterJoin ...")
    val joinType = "left_outer"
    personDF.join(graduateDF, joinPersonGraduate, joinType)
      .show()
  }

  // SELECT *
  // FROM person
  // RIGHT OUTER JOIN graduateProgram
  //    ON person.graduate_program = graduateProgram.id
  private def rightOuterJoin() = {
    logger.info("Method rightOuterJoin ...")
    val joinType = "right_outer"
    personDF.join(graduateDF, joinPersonGraduate, joinType)
      .show()
  }

  // SELECT *
  // FROM graduateProgram
  // LEFT SEMI JOIN person
  //    ON person.graduate_program = graduateProgram.id
  private def leftSemiJoin() = {
    logger.info("Method leftSemiJoin ...")
    val joinType = "left_semi"
    graduateDF.join(personDF, joinPersonGraduate, joinType)
      .show()
  }

  // SELECT *
  // FROM graduateProgram
  // LEFT ANTI JOIN person
  //    ON person.graduate_program = graduateProgram.id
  private def leftAntiJoin() = {
    logger.info("Method leftAntiJoin ...")
    val joinType = "left_anti"
    graduateDF.join(personDF, joinPersonGraduate, joinType)
      .show()
  }

  // SELECT *
  // FROM person
  // CROSS JOIN graduateProgram
  //    ON person.graduate_program = graduateProgram.id
  private def crossJoin() = {
    logger.info("Method crossJoin ...")
    personDF.crossJoin(graduateDF).show()
  }

  // SELECT *
  // FROM
  //   (SELECT id as personId, name, graduate_program, spark_status
  //    FROM person
  //    INNER JOIN status
  //      ON array_contains(spark_status, id)
  private def complexTypeJoin() = {
    logger.info("Method complexTypeJoin ...")
    personDF.withColumnRenamed("id", "personId")
      .join(statusDF, expr("array_contains(spark_status, id)"))
      .show()
  }

  // Handle duplicated columns
  // 1) Join condition use the common column name
  // 2) Drop the column after join
  // 3) Rename bofore the join
  private def joinWithDupColumnName() = {
    logger.info("Method joinWithDupColumnName ...")
    val graduateDupDF = graduateDF.withColumnRenamed("id", "graduate_program")
    val joinExp =
      personDF("graduate_program") === graduateDupDF("graduate_program")

    logger.info("Method joinWithDupColumnName with dup columns...")
    personDF.join(graduateDupDF, joinExp, "inner")
      .show()

    logger.info("Method joinWithDupColumnName with dup columns: approach 1")
    personDF.join(graduateDupDF, "graduate_program")
      .show()

    logger.info("Method joinWithDupColumnName with dup columns: approach 2...")
    personDF.join(graduateDupDF, "graduate_program")
      .drop(graduateDupDF("graduate_program"))
      .show()
  }

  private def joinWithBroadcastSpecified() = {
    logger.info("Metho joinWithBroadcastSpecified...")
    broadcast(personDF).join(broadcast(graduateDF), joinPersonGraduate).explain()
  }

  private def createPersonDF(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      (0, "Bill Chambers", 0, Seq(100)),
      (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
      (2, "Michael Armbrust", 1, Seq(250, 100))
    ).toDF("id", "name", "graduate_program", "spark_status")
    df.show()
    df
  }

  private def createGraduateProgramDF(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      (0, "Masters", "School of Information", "UC Berkeley"),
      (2, "Masters", "EECS", "UC Berkeley"),
      (1, "Ph.D.", "EECS", "UC Berkeley"))
      .toDF("id", "degree", "department", "school")
    df.show()
    df
  }

  private def createStatusDF(spark: SparkSession) = {
    import spark.implicits._
    val df = Seq(
      (500, "Vice President"),
      (250, "PMC Member"),
      (100, "Contributor"))
      .toDF("id", "status")
    df.show()
    df
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("JoinType")
      .master("local")
      .getOrCreate()
  }
}
