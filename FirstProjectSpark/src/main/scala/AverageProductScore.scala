
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
/*
* Un job che sia in grado di generare, per ciascun prodotto,
* lo score medio ottenuto tra il 2003 e il 2012,
* indicando ProductId seguito da tutti gli score medi ottenuti negli anni dell’intervallo.
* Il risultato deve essere ordinato in base al ProductId.
* */
object AverageProductScore {
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

    val filteredDs = ds.filter("year(FROM_UNIXTIME(Time)) >= 2003 and year(FROM_UNIXTIME(time)) <= 2013")

      filteredDs.withColumn("Score", col("Score").cast(IntegerType)).groupBy("ProductId")
      .agg(round(avg("Score"), 2).as("Average Score") , count("ProductId").as("N Review"))
      .sort("ProductId")
      .show(20)

    spark.stop()
  }

}
