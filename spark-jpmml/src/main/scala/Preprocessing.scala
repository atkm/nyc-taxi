import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType}
import xyz.akumano.scala.beam.util.GeoUtils

/**
  * TODO: which part of the preprocessing done here should be included in the ML pipeline?
  *   Bucketing? => ideal for predicting, but training requires bucketed data.
  *   Does PMML support preprocessing? => yes
  * Input: the raw ride data and raw metar data
  * Output: weekday in int 1 <= w <= 7, hour in int 0 <= h <= 23, bucketed temperature, bucketed precipitation, and count
  */
object Preprocessing {
  val spark = SparkSession.builder
    .appName("Ride data pre-processing")
    .master("local")
    // TODO: what to do with the local.dir option in the cloud?
    .config("spark.local.dir", "/home/atkm/nycTaxi/tmp")
    .getOrCreate
  import spark.implicits._

  def readCSV(path: String) = {
    val data = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    data
  }

  def loadRides(ridesPath: String) = {
    val rides = readCSV(ridesPath)
    // the raw data has a white space prefix in column names
    val colNames = rides.columns.map(_ stripPrefix " ")
    val ridesColumnNamedStripped = rides.toDF(colNames: _*)
    ridesColumnNamedStripped.select("pickup_datetime","pickup_latitude", "pickup_longitude")
      .toDF(Seq("date", "lat", "lon"): _*)
  }

  val isInNYC: (Double, Double) => Boolean =
    (lat, lon) => GeoUtils.isInNYC(GeoUtils.Coord(lat, lon))
  val isInNYCudf = udf(isInNYC)
  val cellIdEnrichment: (Double, Double) => Int =
    (lat, lon) => GeoUtils.toGridCell(GeoUtils.Coord(lat, lon))
  val cellIdEnrichmentUDF = udf(cellIdEnrichment)
  val cellIndexExtraction: Int => (Int, Int) =
    id => GeoUtils.getIndex(id)
  val cellIndexUDF = udf(cellIndexExtraction)

  // computes cell ID from (lat, lon)
  def enrichRides(ridesRaw: DataFrame) = {
    val filtered = ridesRaw.filter(isInNYCudf(col("lat"), col("lon")))
    filtered.withColumn("cell_id",
      cellIdEnrichmentUDF(col("lat"), col("lon")))
    // cast it to String so the column is treated as categorical.
    //cellIdEnrichmentUDF(col("lat"), col("lon")).cast(StringType))

  }

  // weekday format: 1 = Mon, ... , 7 = Sun
  def cleanRides(ridesEnriched: DataFrame) = {
    ridesEnriched
      .withColumn("weekday", date_format(col("date"), "u").cast(IntegerType))
      .withColumn("hour", date_format(col("date"), "H").cast(IntegerType))
      .withColumn("date", date_format(col("date"), "yyyy-MM-dd-HH"))
      .drop("lat")
      .drop("lon")
  }

  //def loadPrecip = {
  //  val precipPath = getClass.getResource("/nyc-2014-05-precip-hourly.csv").getPath
  //  val precip = readCSV(precipPath)
  //  precip
  //}

 def loadMetar(metarPath: String) = {
    val metar = readCSV(metarPath)
    val colNames = metar.columns.map(_.replaceAll(" ", ""))
    metar.toDF(colNames: _*)
  }

  val bucketTmpf: Double => Int  = (deg: Double) => deg match {
    case _ if deg < 40 => 0
    case _ if deg < 55 => 1
    case _ if deg < 70 => 2
    case _ if deg < 85 => 3
    case _ => 4
  }
  val bucketTmpfUDF = udf(bucketTmpf)

  val bucketPrecip: Double => Int  = (inch: Double) => inch match {
    case _ if inch < 0.1 => 0
    case _ if inch < 0.3 => 1
    case _ => 2
  }
  val bucketPrecipUDF = udf(bucketPrecip)

  def cleanMetar(metar: DataFrame) = {
    metar
      .select("valid", "tmpf", "p01i")
      .withColumn("minute", date_format(col("valid"), "mm"))
      .filter($"minute" === "51")
      .drop("minute")
      .withColumn("valid", date_format(col("valid"), "yyyy-MM-dd-HH"))
      .withColumnRenamed("valid", "date")
      //.withColumnRenamed("tmpf", "fahrenheit")
      .withColumn("temperature", bucketTmpfUDF(col("tmpf")))
      .drop("tmpf")
      //.withColumnRenamed("p01i", "precip_in")
      .withColumn("precipitation", bucketPrecipUDF(col("p01i")))
      .drop("p01i")

  }


  def joinedData(ridesPath: String, metarPath: String) = {
    val ridesRaw = loadRides(ridesPath)
    val ridesEnriched = enrichRides(ridesRaw)
    val ridesClean = cleanRides(ridesEnriched)

    val metarRaw = loadMetar(metarPath)
    //val distinctDates: Array[String] = metarRaw
    //  .select("valid")
    //  .withColumn("valid", date_format(col("valid"), "yyyy-MM-dd-HH"))
    //  .distinct()
    //  .as[String]
    //  .collect()
    //println(distinctDates.count()) // => 744
    val metarClean = cleanMetar(metarRaw)
    // println(metarClean.select("date").distinct().count()) // => 744. No missing hour.

    val joined = ridesClean.join(metarClean, Seq("date"), "inner").drop("date")
    joined
  }

  def getCounts(joined: DataFrame) = {
    joined.groupBy("cell_id", "weekday", "hour", "temperature", "precipitation").count()
    //joined.groupBy("cell_id", "weekday", "hour", "fahrenheit", "precip_in").count()
  }
  // unpack cell_id
  def cleanCounts(ridesPath: String, metarPath: String) = {
    val counts = getCounts(joinedData(ridesPath, metarPath))
    counts.withColumn("cell_index", cellIndexUDF(col("cell_id")))
      .withColumn("cell_index_x", col("cell_index._1"))
      .withColumn("cell_index_y", col("cell_index._2"))
      .drop("cell_index")
      .drop("cell_id")
  }
  def counts(ridesPath: String, metarPath: String) =
    cleanCounts(ridesPath, metarPath)

  def main(args: Array[String]): Unit = {
    val ridesPath = getClass.getResource("yellow_tripdata_2014-05-small.csv").getPath
    val metarPath = getClass.getResource("/lga-2014-05-metar.csv").getPath
    val countsDf = counts(ridesPath, metarPath)
    countsDf.orderBy(desc("count")).show
    //println(counts.select("count").as[Long].collect().sum) // => 10000
    countsDf.printSchema()

    countsDf.schema
  }

}
