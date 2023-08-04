import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/*
* Un job in grado di generare coppie di prodotti che hanno almeno un utente in comune,
* ovvero che sono stati recensiti da uno stesso utente, indicando, per ciascuna coppia, il numero di utenti in comune.
* Il risultato deve essere ordinato in base allo ProductId del primo elemento della coppia e,
* possibilmente, non deve presentare duplicati.
* */
object ProductPairs {
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

    val filteredDs = ds.select("UserId", "ProductId")

    filteredDs.as("ds1")
      .join(filteredDs.as("ds2"), ($"ds1.UserId" === $"ds2.UserId" && $"ds1.ProductId" < $"ds2.ProductId"))
      .select("ds1.UserId", "ds1.ProductId", "ds2.ProductID")
//      .where(" ds1.ProductId == 'B005CUU25G' AND ds2.ProductID == 'B008O3G2K2' ").count() QUERY USED TO CHECK THE RESULT
      .groupBy("ds1.ProductId", "ds2.ProductID")
      .count()
      .sort("ds1.ProductId")
      .show()
  }
}
