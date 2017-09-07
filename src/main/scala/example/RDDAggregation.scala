package example

import java.util.Random

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDDAggregation extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("My Application")
      .setMaster("local")
    val sc = new SparkContext(conf)

    runAggregation(sc)
  }

  private def runAggregation(sc: SparkContext) = {
    val words = createWords(sc)
    val wordsPair = words.map(word => (word.toLowerCase(), 1))
    val numbers = sc.parallelize(1 to 10, 5)

    runCountByKey(wordsPair)
    runGroupByKey(wordsPair)
    runReduceByKey(wordsPair)
    runAggregate(numbers)
    runTreeAggregate(numbers)
    runAggregateByKey(wordsPair)
    runCombineByKey(numbers)
    runFoldByKey(numbers)
//    runSampleByKey(numbers)
    runCoGroup(sc)
  }

  private def runCountByKey(wordsPair: RDD[(String, Int)]) = {
    logger.info("Method runCountByKey ...")
    wordsPair.countByKey()
      .foreach(println)
  }

  // Bad use of memory for duplicated entries, use reduceByKey() most of the time
  private def runGroupByKey(wordsPair: RDD[(String, Int)]) = {
    logger.info("Method runGroupByKey ...")
    wordsPair.groupByKey()
      .map(row => (row._1, row._2.reduce(addFunc)))
      .collect()
      .foreach(println)
  }

  private def runReduceByKey(wordsPair: RDD[(String, Int)]) = {
    logger.info("Method runReduceByKey ...")
    wordsPair.reduceByKey(_+_)
      .collect()
      .foreach(println)
  }

  // Aggregate by partition
  // Two args: 1 - within partition, 2 - across partition
  private def runAggregate(numPair: RDD[Int]) = {
    logger.info("Method runAggregate ...")
    val result = numPair.aggregate[Int](0)(maxFunc, addFunc)
    println(s"Aggregated result = $result")
  }

  private def runTreeAggregate(numPair: RDD[Int]) = {
    logger.info("Method runTreeAggregate ...")
    val result = numPair.treeAggregate(0)(maxFunc, addFunc)
    println(s"treeAggregated result = $result")
  }

  // TODO: Get a better example
  private def runAggregateByKey(wordsPair: RDD[(String, Int)]) = {
    logger.info("Method runAggregateByKey ...")
    wordsPair.aggregateByKey(0)(maxFunc, addFunc)
      .collect()
      .foreach(println)
  }

  private def runCombineByKey(numbers: RDD[Int]) = {
    logger.info("Method runCombineByKey ...")
    val valToCombiner = (value:Int) => List(value)
    val mergeValuesFunc =
      (vals:List[Int], valToAppend:Int) => valToAppend :: vals
    val mergeCombinerFunc =
      (vals1:List[Int], vals2:List[Int]) => vals1 ::: vals2
    val outputPartitions = 6

    val numPair = numbers.map(num => (num, 1))
    numPair
      .combineByKey(
        valToCombiner,
        mergeValuesFunc,
        mergeCombinerFunc,
        outputPartitions)
      .collect()
      .foreach(println)
  }

  private def runFoldByKey(numbers: RDD[Int]) = {
    logger.info("Method runFoldByKey ...")
    val numPair = numbers.map(num => (num, 1))
    numPair.foldByKey(0)(addFunc)
      .collect()
      .foreach(println)
  }

//  private def runSampleByKey(wordsPair: RDD[(Int, Int)]) = {
//    logger.info("Method runSampleByKey ...")
//  }

    private def runCoGroup(sc: SparkContext) = {
      logger.info("Method runCoGroup ...")
      val words =sc.parallelize("This is a Spark example example".split(" "))
      val distinctChars = words.flatMap(word =>
        word.toLowerCase().toSeq)
        .distinct

      val charRDD = distinctChars.map(c =>
        (c, new Random().nextDouble()))
      val charRDD2 = distinctChars.map(c =>
        (c, new Random().nextDouble()))
      val charRDD3 = distinctChars.map(c =>
        (c, new Random().nextDouble()))
      charRDD.cogroup(charRDD2, charRDD3)
        .take(5)
        .foreach(println)
    }


  private def maxFunc(num1: Int, num2: Int) =
    Math.max(num1, num2)

  private def addFunc(num1: Int, num2: Int) =
    num1 + num2

  private def createWords(sc: SparkContext) = {
    val collection = Array("This", "is", "a", "example", "example")
    sc.parallelize(collection, 2)
  }
}