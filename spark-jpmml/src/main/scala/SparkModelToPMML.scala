import java.io.File

import javax.xml.transform.stream.StreamResult
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.types.StructType
import org.dmg.pmml.PMML
import org.jpmml.model.JAXBUtil
import org.jpmml.sparkml.ConverterUtil

object SparkModelToPMML {

  def main(args: Array[String]): Unit = {
    // load DataFrame just to compute its schema
    val ridesPath = getClass.getResource("/yellow_tripdata_2014-05-tiny.csv").getPath
    val metarPath = getClass.getResource("/lga-2014-05-metar.csv").getPath
    val schema: StructType = Preprocessing.counts(ridesPath, metarPath).schema

    //val writePath = args(0)
    val modelPath = "/home/atkm/code/nycTaxi/deploy/rfModel-201405.spark"
    val model: PipelineModel = PipelineModel.load(modelPath)
    val pmml: PMML = ConverterUtil.toPMML(schema, model)
    val writePath = "/home/atkm/code/nycTaxi/deploy/rfModel-201405.pmml"
    JAXBUtil.marshalPMML(pmml, new StreamResult(new File(writePath)))
  }

}
