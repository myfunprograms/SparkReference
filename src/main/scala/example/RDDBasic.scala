package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object RDDBasic extends LazyLogging {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("My Application")
      .setMaster("local")
    val sc = new SparkContext(conf)

    runCreateQuery(sc)
    runTransformation(sc)
    runAction(sc)
    save(sc)
    cache(sc)
  }

  private def runCreateQuery(sc: SparkContext) = {
    createFromCollection(sc)
    createFromDataSource(sc)
  }

  private def runTransformation(sc: SparkContext) = {
    val words = createWords(sc)

    runDistinct(sc, words)
    runFilter(words)
    runMap(words)
    runFlatMap(words)
    runSortBy(words)
    runRandomSplit(words)
  }

  private def runAction(sc: SparkContext) = {
    val words = createWords(sc)
    val numbers = sc.parallelize(1 to 20)

    runReduce(numbers)
    runReduceLongestWord(words)
    runCountByValue(words)
    runMinMax(numbers)

    runTake(numbers)
    runTakeSample(numbers)
  }

  private def save(sc: SparkContext) = {
    val words = createWords(sc)
    saveToFile(words)
    saveToFileCompressed(words)
  }

  private def cache(sc: SparkContext) = {
    val words = createWords(sc)
    cacheRDD(words)
  }

  private def createFromCollection(sc: SparkContext) = {
    logger.info("Method createFromCollection ...")
    val collection = Array("This", "is", "a", "example", "example")
    val words = sc.parallelize(collection, 2)
    words.setName("myWords")
    println(words.name)
  }

  private def createFromDataSource(sc: SparkContext) = {
    logger.info("Method createFromDataSource ...")
    val path = "src/main/data/flight-data/csv/2015-summary.csv"
    val rdd = sc.textFile(path)
    println(rdd.count())
  }

  private def runDistinct(sc: SparkContext, words: RDD[String]) = {
    logger.info("Method runDistinct ...")
    val countDistinct = words.distinct().count()
    println(countDistinct)
  }

  private def runFilter(words: RDD[String]) = {
    logger.info("Method runFilter ...")
    val filtered = words.filter(word => word.startsWith("e"))
    filtered.foreach(println)
  }

  private def runMap(words: RDD[String]) = {
    logger.info("Method runMap ...")
    words.map(word => (word, word(0), word.startsWith("e")))
      .foreach(println)
  }

  private def runFlatMap(words: RDD[String]) = {
    logger.info("Method runFlatMap ...")
    words.flatMap(word => Seq(word))
      .foreach(println)
  }

  private def runSortBy(words: RDD[String]) = {
    logger.info("Method runSortBy ...")
    words.sortBy(word => word.length() * -1)
      .foreach(println)
  }

  private def runRandomSplit(words: RDD[String]) = {
    logger.info("Method runRandomSplit ...")
    val splitted = words.randomSplit(Array[Double](0.5, 0.5))
    splitted(0).foreach(println)
    splitted(1).foreach(println)
  }

  private def runReduce(numbers: RDD[Int]) = {
    logger.info("Method runReduce ...")
    val sum = numbers.reduce(_+_)
    println(sum)
  }

  private def runReduceLongestWord(words: RDD[String]) = {
    logger.info("Method runReduceLongestWord ...")
    val longest = words.reduce((word1, word2) => {
      val diff = word1.length - word2.length
      if (diff > 0) word1 else word2
    })
    println(longest)
  }

  // Warning: use countByValue() on small dataset because it loads all to memory
  private def runCountByValue(words: RDD[String]) = {
    logger.info("Method runCountByValue ...")
    val value = words.countByValue()
    println(value)
  }

  private def runMinMax(numbers: RDD[Int]) = {
    logger.info("Method runMinMax ...")
    println(numbers.min())
    println(numbers.max())
  }

  private def runTake(numbers: RDD[Int]) = {
    logger.info("Method runTake ...")

    val taken = numbers.take(5)
    logger.info(s"take(): ${taken.mkString(", ")}")

    val takenOrdered = numbers.takeOrdered(5)
    logger.info(s"takeOrdered(): ${takenOrdered.mkString(", ")}")

    val topped = numbers.top(5)
    logger.info(s"top(): ${topped.mkString(", ")}")
  }

  private def runTakeSample(numbers: RDD[Int]) = {
    logger.info("Method runTakeSample ...")
    val withReplacement = true
    val numberToTake = 6
    val seed = 1L

    val sampled = numbers.takeSample(withReplacement, numberToTake, seed)
    logger.info(s"sample(): ${sampled.mkString(", ")}")
  }

  private def saveToFile(words: RDD[String]) = {
    logger.info("Method saveToFile ...")
    words.saveAsTextFile("temp/text/raw")
  }

  private def saveToFileCompressed(words: RDD[String]) = {
    logger.info("Method saveToFileCompressed ...")
    words.saveAsTextFile("temp/text/compress", classOf[BZip2Codec])
  }

  // By default, cache() and persist() in memory
  private def cacheRDD(words: RDD[String]) = {
    logger.info("Method cacheRDD ...")
    words.cache()
    words.persist(StorageLevel.MEMORY_ONLY_SER)
  }

  private def createWords(sc: SparkContext) = {
    val collection = Array("This", "is", "a", "example", "example")
    sc.parallelize(collection, 2)
  }
}