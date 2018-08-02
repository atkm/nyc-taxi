import org.scalatest.FlatSpec
import org.scalatest.Matchers._

import java.util
import scala.collection.JavaConversions._

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/* Tests:
- the trained model and imported model should yield the same prediction on the training set
 */

class RandomForestTest extends FlatSpec {

  println("Warning: RandomForestTest is disabled")

  //val predictions = {
  //  val ridesPath = "/home/atkm/code/nycTaxi/scikit/yellow_counts_201405.csv"
  //  val spark = SparkSession.builder().appName("Random Forest Example").master("local").getOrCreate()
  //  val rideCounts: DataFrame = spark.read.format("csv")
  //    .option("header", "true")
  //    .option("inferSchema", "true")
  //    .load(ridesPath)

  //  val (_, rfModel) = RandomForestPMML.trainRF(rideCounts)
  //  val predictions: List[Double] = rfModel.transform(rideCounts)
  //    .select("prediction")
  //    .collectAsList
  //    .map(_.getDouble(0))
  //    .toList
  //  predictions
  //}
  //val ridesPath = getClass.getResource("yellow_tripdata_2014-05-small.csv").getPath
  //val metarPath = getClass.getResource("/lga-2014-05-metar.csv").getPath

  //"A PMML model" should "give the same predictions as the Spark model" in {

  //  val pmml = RandomForestPMML.trainedRFPMML(ridesPath, metarPath)
  //  val pmmlPredictions = PMMLImportExample.evaluatePMML(pmml, ridesPath)

  //  assert(predictions.size == pmmlPredictions.size)
  //  val tolerance = 0.001 // we are comparing counts, so tolerance need not be small.
  //  for ( (expected, actual) <- predictions zip pmmlPredictions) {
  //    assert(actual === expected +- tolerance)
  //  }
  //}

  //"A PMML model exported then imported" should "give the same predictions as the Spark model" in {

  //  // TODO: create a model with trainedRFPMML, then export.
  //  val pmml = PMMLImportExample.importPMML("/taxirf.pmml")
  //  val pmmlPredictions = PMMLImportExample.evaluatePMML(pmml, ridesPath)

  //  assert(predictions.size == pmmlPredictions.size)
  //  val tolerance = 0.001 // we are comparing counts, so tolerance need not be small.
  //  for ( (expected, actual) <- predictions zip pmmlPredictions) {
  //    assert(actual === expected +- tolerance)
  //  }
  //}

}
