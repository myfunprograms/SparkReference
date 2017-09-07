package example

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

final case class Flight(DEST_COUNTRY_NAME: String,
                        ORIGIN_COUNTRY_NAME: String,
                        count: BigInt)

object RDDBroadcastAndAccumulator extends LazyLogging {
  def main(args: Array[String]): Unit = {
    val session = createSparkSession()

    runBroadcast(session)
    runAccumulator(session)
  }

  private def runBroadcast(session: SparkSession) = {
    val sc = session.sparkContext
    val wordsRDD = createWordsRDD(sc)
    val supplymentData = createSupplymentData()

    val suppBroadcast = sc.broadcast(supplymentData)
    wordsRDD.map(word =>
      suppBroadcast.value.getOrElse(word, 0)
    ).foreach(println)
  }

  private def runAccumulator(session: SparkSession) = {
    import session.implicits._
    val flights = session.read.parquet("temp/parquet")
      .as[Flight]

    val sc = session.sparkContext
    val accUS = sc.longAccumulator("United States")

    def accUSFunc(flightRow: Flight) = {
      val dest = flightRow.DEST_COUNTRY_NAME
      val origin = flightRow.ORIGIN_COUNTRY_NAME
      if (dest == "United States") {
        accUS.add(flightRow.count.toLong)
      }
      if (origin == "United States") {
        accUS.add(flightRow.count.toLong)
      }
    }

    flights.foreach(flightRow => accUSFunc(flightRow))
    println(accUS.value)
  }

  private def createWordsRDD(sc: SparkContext) = {
    val wordsData = Array[String]("This", "is", "a", "Spark", "example")
    sc.parallelize(wordsData)
  }

  private def createSupplymentData() = {
    Map[String, Int](
      "This" -> 100,
      "is" -> 200,
      "a" -> 300,
      "Spark" -> 400,
      "example" -> 500)
  }

  private def createSparkSession() = {
    SparkSession.builder()
      .appName("RDDBroadcastAndAccumulator")
      .master("local")
      .getOrCreate()
  }
}
