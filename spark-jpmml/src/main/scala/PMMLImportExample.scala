import java.io.{File, InputStream}

import scala.collection.JavaConverters._
import org.dmg.pmml.{FieldName, PMML}
import org.jpmml.evaluator._
import org.jpmml.model.PMMLUtil

// unmarshals a random forest PMML model, and evaluates it on the data it was trained on.
object PMMLImportExample {
  def importPMML(pmmlPath: String) = {
    val modelStream: InputStream = getClass.getResourceAsStream(pmmlPath)
    val pmml: PMML = PMMLUtil.unmarshal(modelStream)
    pmml
  }

  def getRides(ridesPath: String) = {
    val ridesStream = getClass.getResourceAsStream(ridesPath)
    val rides = scala.io.Source.fromInputStream(ridesStream).getLines
    rides
  }

  def prepArguments(modelEvaluator: Evaluator, ridesPath: String) = {
    val rides = getRides(ridesPath)
    val rideSchema = rides.take(1).toList.apply(0).split(",").toList
    // Note: an iterator is mutable. Taking 1 in the preceding line moves the cursor.
    val rideData: List[Map[String, String]] = rides.toList.map {rideSchema zip _.split(",").toList toMap}
    // val oneRide: Map[String, String] = rideData(0)

    // prepare features
    val inputFields: Seq[InputField] = modelEvaluator.getInputFields.asScala
    val arguments: List[Map[FieldName, FieldValue]] = rideData map {prepOneArgument(_, inputFields)}
    //arguments.foreach {println _}
    arguments
  }

  def prepOneArgument(dataPoint: Map[String, String],
                      inputFields: Seq[InputField]): Map[FieldName, FieldValue] = {
    val arg = inputFields.map(field => {
      val name = field.getName
      val v = field.prepare(dataPoint(name.getValue))
      (name, v)
    }).toMap
    arg
  }

  def evaluatePMML(pmml: PMML, ridesPath: String) = {


    val modelEvaluator: Evaluator = ModelEvaluatorFactory.newInstance
      .newModelEvaluator(pmml).asInstanceOf[Evaluator]

    val arguments: List[Map[FieldName, FieldValue]] = prepArguments(modelEvaluator, ridesPath)
    val results = arguments.map(arg => modelEvaluator.evaluate(arg.asJava))

    // target and output fields are the same thing in this model?
    //modelEvaluator.getTargetFields.asScala map {targetField =>
    //  val name = targetField.getName
    //  println(name) // => "count"
    //  println(results.get(name))
    //}

    //modelEvaluator.getOutputFields.asScala map {outputField =>
    //  val name = outputField.getName
    //  println(name) // => "count"
    //  println(results.get(name)) // Some[Double]
    //}

    val predictions = results.map(res => res.get(new FieldName("count")).asInstanceOf[Double])
    predictions
  }

  def main(args: Array[String]): Unit = {
    val modelPath = "/rfModel-201405.pmml"
    //val modelPath = "/taxirf.pmml"
    // TODO: data model is obsolete. yellow_counts_201405.csv doesn't have cell_index but zipcode instead.
    val ridesPath = "/yellow_counts_201405.csv"
    //val modelPath = "/home/atkm/code/nycTaxi/spark-jpmml/taxirf.pmml"

    val pmml = importPMML(modelPath)
    val predictions = evaluatePMML(pmml, ridesPath)
    println(predictions.size)
    //predictions.take(100).foreach(println _)

  }


}
