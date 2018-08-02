import java.io.File

import javax.xml.transform.stream.StreamResult
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.dmg.pmml.PMML
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.ConverterUtil

// TODO: the cell_id field has too many categories. Perhaps this is where neural network shines.
// Use it as a continuous variable for now. A small improvement would be to use the 2d-indices of cells.
// Input: a DataFrame in the format of the output of Preprocessing. Temperature and precipitation fields are bucketed.
// Output: the PMML of a trained random forest model. Features that are not used for splitting
//    do not show in the input fields of the model.
object RandomForestPMML {

  def trainRF(counts: DataFrame): (StructType, PipelineModel) = {
    // hour, weekday, fahrenheit, precipitation: continuous variables that take on discrete values

    // cell ID: index it, then run through OneHotEncoderEstimator
    //val idIndexer = new StringIndexer()
    //  .setInputCol("cell_id")
    //  .setOutputCol("cell_id_indexed")
    //  .fit(counts)
    // Warning: OneHotEncoderEstimator is new in 2.3. Must use OneHotEncoder in 2.2.
    //val idOneHotEncoder = new OneHotEncoderEstimator()
    //  .setInputCols(Array(idIndexer.getOutputCol))
    //  .setOutputCols(Array("cell_id_categorical"))

    val assembler = new VectorAssembler()
      // when treating cell_id as categorical
      //.setInputCols(Array("hour", "weekday", "temperature", "precipitation", idOneHotEncoder.getOutputCols(0)))
      // without unpacking cell_id
      //.setInputCols(Array("hour", "weekday", "temperature", "precipitation", "cell_id"))
      .setInputCols(Array("hour", "weekday", "temperature", "precipitation", "cell_index_x", "cell_index_y"))
      .setOutputCol("features")

    // the complexity of trees is kept small for prototyping. TODO: optimize parameters
    val rf = new RandomForestRegressor()
      .setFeaturesCol(assembler.getOutputCol)
      .setLabelCol("count")
      .setNumTrees(50) // Default = 20
      .setMaxDepth(10) // Default = 5

    val pipeline = new Pipeline()
      .setStages(Array(assembler, rf))
      // when treating cell_id as categorical
      //.setStages(Array(idIndexer, idOneHotEncoder, assembler, rf))

    val model = pipeline.fit(counts)

    (counts.schema, model)
  }

  def trainedRFPMML(ridesPath: String, metarPath: String): PMML = {

    val counts = Preprocessing.counts(ridesPath, metarPath)
    val (dataSchema, model) = trainRF(counts)
    //println(s"Learned regression forest model:\n ${model.stages(3).asInstanceOf[RandomForestRegressionModel].toDebugString}")

    val pmml: PMML = ConverterUtil.toPMML(dataSchema, model)
    //JAXBUtil.marshalPMML(pmml, new StreamResult(System.out))
    pmml
  }

  def trainAndSave(ridesPath: String, metarPath: String, writePath: String) = {

    val counts = Preprocessing.counts(ridesPath, metarPath)
    val (dataSchema, model) = trainRF(counts)

    model.write.overwrite().save(writePath)
  }


  /**
    *
    * @param args ridesPath, metarPath: if the string starts with a forward slash, then the method looks for a file with
    *             the given name in src/main/resources. If the string starts with "gs://" the method looks for a file
    *             with the given name in Google Cloud Storage.
    *             writePath: the location where the resulting PMML file is written to.
    */
  def main(args: Array[String]): Unit = {
    // https://stackoverflow.com/questions/2315912/best-way-to-parse-command-line-parameters
    val usage =
      """
        Required options: --ridesPath --metarPath --writePath
      """.stripMargin
    if (args.length == 0) println(usage)
    val argsList = args.toList
    type OptionMap = Map[Symbol, String]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--ridesPath" :: value :: tail =>
          nextOption(map ++ Map('ridesPath -> value), tail)
        case "--metarPath" :: value :: tail =>
          nextOption(map ++ Map('metarPath -> value), tail)
        case "--writePath" :: value :: tail =>
          nextOption(map ++ Map('writePath -> value), tail)
        case option :: tail => {
          println("Unknown option " + option)
          sys.exit(1)
        }
      }
    }
    val options = nextOption(Map(), argsList)
    println(options)

    trainAndSave(options('ridesPath), options('metarPath), options('writePath))
    sys.exit()

    //val ridesPath = getClass.getResource("/yellow_tripdata_2014-05-small.csv").getPath
    //val ridesPath = getClass.getResource("/yellow_tripdata_2014-05-tiny.csv").getPath
    //val ridesPath = getClass.getResource(options('ridesPath)).getPath

    //val metarPath = getClass.getResource("/lga-2014-05-metar.csv").getPath
    //val metarPath = getClass.getResource(options('metarPath)).getPath

    val startTime = System.nanoTime
    val pmml = trainedRFPMML(options('ridesPath), options('metarPath))
    val endTime = System.nanoTime
    println("\n\nTraining took " + (endTime - startTime)/1e9d + " seconds.\n\n")
    // => nTrees = maxDepth = 1 on 2014-05-small => 34 secs
    // => nTrees = 2, maxDepth = 3 on 2014-05-small => 46 secs
    // => nTrees = 10, maxDepth = 5 on 2014-05-small => 110 secs
    // => nTrees = 30, maxDepth = 10 on 2014-05-small => 135 secs
    // On DataProc:
    // nTrees = 50, maxDepth = 10 on 2014-05 (full) master=2cores + 2 workers=1core => 16 mins

    //val writePath = "/home/atkm/code/nycTaxi/data/taxirf.pmml"
    val writePath = options('writePath)
    JAXBUtil.marshalPMML(pmml, new StreamResult(new File(writePath)))

  }
}
