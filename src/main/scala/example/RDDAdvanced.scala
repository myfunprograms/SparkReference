package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// TODO: split into smaller classes
object RDDAdvanced extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("My Application")
      .setMaster("local")
    val sc = new SparkContext(conf)

    runSinglePartition(sc)
    runKeyValueRDDs(sc)
  }

  private def runSinglePartition(sc: SparkContext) = {
    val words = createWords(sc)

    pipeRDD(sc)
    mapPartitions(words)
    mapPartitionsWithIndex(words)
    foreachPartition(words)
    glom(words)
  }

  private def runKeyValueRDDs(sc: SparkContext) = {
    val words = createWords(sc)

    runKeyBy(words)
    runMapValues(words)
    runFlatMapValues(words)
    runExtractKeys(words)
    runExtractValues(words)
    runLookup(words)
  }

  private def pipeRDD(sc: SparkContext) = {
    logger.info("Method pipeRDD ...")
    val collection = "This is a Spark example".split(" ")
    sc.parallelize(collection, 2)
      .foreach(println)
  }

  // Operate on an individual partition, with a return value
  private def mapPartitions(words: RDD[String]) = {
    logger.info("Method mapPartitions ...")
    val numPartitions = words.mapPartitions(part => Iterator[Int](1)).sum()
    logger.info(s"Number of partitions = $numPartitions")
  }

  private def mapPartitionsWithIndex(words: RDD[String]) = {
    logger.info("Method mapPartitionsWithIndex ...")
    val result = words.mapPartitionsWithIndex[String](
      (partitionIndex: Int, withinPartIterator: Iterator[String]) =>
        withinPartIterator.toList.map(value =>
          s"Partition: $partitionIndex => $value")
        .iterator)
    result.foreach(println)
  }

  // Operate on an individual partition, without return value
  // Need to create the folder first.
  private def foreachPartition(words: RDD[String]) = {
    logger.info("Method foreachPartitions ...")
    words.foreachPartition { iter =>
      while (iter.hasNext)
        println(iter.next())
    }
  }

  // Takes every partition in the dataset and turns it into an array
  // of the values in that partition.
  private def glom(words: RDD[String]) = {
    logger.info("Method glom ...")
    val result = words.glom()
      .collect()
    result.foreach(_.foreach(println))
  }

  // Create a key for the RDD
  private def runKeyBy(words: RDD[String]) = {
    logger.info("Method runKeyBy ...")
    words.keyBy(word => word.toLowerCase.toSeq(0))
      .collect()
      .foreach(println)
  }

  private def runMapValues(words: RDD[String]) = {
    logger.info("Method runMapValues ...")
    words.map(word => (word.toLowerCase().toSeq(0), word))
      .mapValues(word => word.toUpperCase())
      .collect()
      .foreach(println)
  }

  private def runFlatMapValues(words: RDD[String]) = {
    logger.info("Method runFlatMapValues ...")
    words.map(word => (word.toLowerCase().toSeq(0), word))
      .flatMapValues(word => word.toUpperCase())
      .collect()
      .foreach(println)
  }

  private def runExtractKeys(words: RDD[String]) = {
    logger.info("Method runExtractKeys ...")
    words.map(word => (word.toLowerCase().toSeq(0), word))
      .keys
      .collect()
      .foreach(println)
  }

  private def runExtractValues(words: RDD[String]) = {
    logger.info("Method runExtractValues ...")
    words.map(word => (word.toLowerCase().toSeq(0), word))
      .values
      .collect()
      .foreach(println)
  }

  private def runLookup(words: RDD[String]) = {
    logger.info("Method runLookup ...")
    words.map(word => (word.toLowerCase(), 1))
      .lookup("example")
      .foreach(println)
  }

  private def createWords(sc: SparkContext) = {
    val collection = Array("This", "is", "a", "example", "example")
    sc.parallelize(collection, 2)
  }
}