import org.apache.arrow.flatbuf.Date
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/*
* Un job che sia in grado di generare, per ciascun anno,
* le dieci parole che sono state più usate nelle recensioni (campo summary) in ordine di frequenza,
* indicando, per ogni parola, la sua frequenza, ovvero il numero di occorrenze
* della parola nelle recensioni di quell’anno.
* */
object WordOccurrences {

  case class Review(Id: BigInt, ProductId: String, UserId: String, ProfileName: String, HelpfulnessNumerator: String, HelpfulnessDenominator: String, Score: String, Time: String, Summary: String, Text: String)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("WordOccurrences")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Reviews.csv")
      .as[Review]

    val convertedDs = ds.select(year(to_date(from_unixtime($"time"))).as("Time"),split($"Summary", "\\s+").as("Summary"))
    val filteredDs = convertedDs.filter($"Time" === 2003)

    filteredDs.select(explode($"Summary").as("Word"))
      .groupBy(lower($"Word").as("Word"))
      .count().as("Occurrences")
      .sort($"count".desc)
      .show(10)

    spark.stop()
  }

}
