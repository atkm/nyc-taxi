object RandomForestPMMLExample {
  def trainRF(ridesPath: String): (StructType, PipelineModel) = {
    val spark = SparkSession.builder().appName("Random Forest Example").master("local").getOrCreate()
    val rideCounts: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(ridesPath)

    val dataSchema = rideCounts.schema
    rideCounts.printSchema()
    // note: reading in libsvm format would return a DataSet[LabeledPoint]



    // hour & weekday: continuous variable
    // temperature & precipitation: continuous variable

    // zip code: StringIndexer -> OneHotEncoderEstimator
    val zipIndexer = new StringIndexer()
      .setInputCol("zip")
      .setOutputCol("indexedZip")
      .fit(rideCounts)
    val zipOneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array("indexedZip"))
      .setOutputCols(Array("categoricalZip"))
    // val encodedData = encoderModel.transform(rideCounts)


    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "weekday", "categoricalZip"))
      .setOutputCol("features")

    //trainData.select("features", "count").show(true)

    //rideCounts.cache()

    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("count")
      .setNumTrees(2) // keep the number of trees small for prototyping.
      .setMaxDepth(3)

    val pipeline = new Pipeline()
      .setStages(Array(zipIndexer, zipOneHotEncoder, assembler, rf))

    val model = pipeline.fit(rideCounts)

    (dataSchema, model)
  }

  def trainedRFPMML(ridesPath: String): PMML = {

    val (dataSchema, model) = trainRF(ridesPath)

    //println(s"Learned regression forest model:\n ${model.stages(3).asInstanceOf[RandomForestRegressionModel].toDebugString}")

    val pmml: PMML = ConverterUtil.toPMML(dataSchema, model)
    //JAXBUtil.marshalPMML(pmml, new StreamResult(System.out))
    pmml
  }

  def main(args: Array[String]): Unit = {

    /* schema ---
    taxi:
    hour | weekday |  zip  | count
    9       2        10016   6

    weather:
    fahrenheit | precip in inch
    48.56         0.33

    --- */

    val ridesPath = "/home/atkm/code/nycTaxi/scikit/yellow_counts_201405.csv"

    val pmml = trainedRFPMML(ridesPath)

    val basePath = "/home/atkm/code/nycTaxi/spark-jpmml"
    JAXBUtil.marshalPMML(pmml, new StreamResult(new File(basePath + "/src/main/resources/taxirf.pmml")))
  }

}
