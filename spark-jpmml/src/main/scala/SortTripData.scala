import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SortTripData {
  // Note: double precision of latitude and longitude is lost through this process,
  // though the original data seem to have more digits than significant figures.
  def main(args: Array[String]): Unit = {
    //val inputFile = "yellow_tripdata_2014-05-small.csv"
    val inputFile = "yellow_tripdata_2014-05.csv"
    //val outputFile = "yellow_tripdata_2014-05-small-sorted.csv"
    val outputFile = "yellow_tripdata_2014-05-sorted.csv"
    val spark = SparkSession.builder().appName("Sort raw ride data").master("local").getOrCreate()
    val ridesPath = getClass.getResource(inputFile).getPath
    val rides = Preprocessing.readCSV(ridesPath)
    val sorted = rides.orderBy(asc(" pickup_datetime"))
    sorted.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
      .format("csv").save(outputFile)
  }
}
