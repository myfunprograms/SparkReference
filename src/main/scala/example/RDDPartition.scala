package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{HashPartitioner, Partitioner, SparkContext}

class DomainPartitioner extends Partitioner {
  def numPartitions = 20
  def getPartition(key: Any): Int = {
    (key.asInstanceOf[Double] / 1000).toInt
  }
}

object RDDPartition extends LazyLogging {
  private val JsonPath = "src/main/data/flight-data/json/2015-summary.json"

  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    val jsonData = readJsonData(spark)
    val words = createWords(spark.sparkContext)

    runColalesce(words)
    runRepartition(words)
    runRepartitionCustomer(jsonData)
    runRepartitionAndSortWithinPartitions(jsonData)
  }

  // No shuffle for the partition on the same driver
  private def runColalesce(words: RDD[String]) = {
    logger.info("Method runColalesce ...")
    words.coalesce(1)
  }

  // Shuffle
  private def runRepartition(words: RDD[String]) = {
    logger.info("Method runRepartition ...")
    words.repartition(10)
  }

  // Spark has two partitioners
  // HashPartitioner: for discrete values
  // RangePartitioner: for continuous values
  private def runRepartitionCustomer(jsonData: DataFrame) = {
    logger.info("Method runRepartitionCustomer ...")
    val rdds = jsonData.repartition(10).rdd
    logger.info(s"Number of partitions is ${rdds.getNumPartitions}")
    rdds.map(r => r(2))
      .take(5)
      .foreach(println)

    val keyedRDD = rdds.keyBy(row => row(2).asInstanceOf[Long])
    val result = keyedRDD.partitionBy(new HashPartitioner(10))

    result.glom()
      .collect()
      .map(arr => {
        if (arr.length > 0)
          arr.map(_._2(2)).toSet.toSeq.length
      })
      .foreach(println)
  }

  private def runRepartitionAndSortWithinPartitions(jsonData: DataFrame) = {
    logger.info("Method runRepartitionAndSortWithinPartitions ...")
    val rdds = jsonData.repartition(10).rdd
    logger.info(s"Number of partitions is ${rdds.getNumPartitions}")
    rdds.map(r => r(2))
      .take(5)
      .foreach(println)

    val keyedRDD = rdds.keyBy(row => row(2).asInstanceOf[Long])
    val resultRDDs = keyedRDD.repartitionAndSortWithinPartitions(new DomainPartitioner())
    logger.info(s"Number of partitions ${resultRDDs.getNumPartitions}")
  }

  private def readJsonData(spark: SparkSession): DataFrame = {
    spark.read.json(JsonPath)
  }

  private def createWords(sc: SparkContext) = {
    val collection = Array("This", "is", "a", "example", "example")
    sc.parallelize(collection, 2)
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("RDDPartition")
      .master("local")
      .getOrCreate()
  }
}
