- where and how to write the resulting pmml (try file://home/at\_kumano/spark-rf.pmml)
    * if using a spark function, writing to gs://bucket-id-blah/rfModel.spark works.

To deploy:
- mark both spark-sql and spark-mllib as provided in build.sbt. Use Spark 2.2.1, which is the version that DataProc 1.2 comes with. Exclude an old jpmml transitive dependency from spark-mllib. Use sbt-assembly to create a jar.
- mark cloud storage connector as provided. DataProc 1.2 comes with 1.6.6-hadoop2.
- Create a DataProc cluster
- Place yellow\_tripdata\_YYYY-MM.csv, lga-YYYY-MM-metar.csv, and spark-random-forest-assembly.jar (in spark-jpmml/target/scala-2.11/) in a gcs bucket.
- Submit a job with the following options:
    * Main class or jar =	RandomForestPMML
    * Job type = Spark
    * Jar files = gs://bucket-id-blah/spark-random-forest-assembly-1.0.jar
    * Arguments	= --ridesPath
          gs://bucket-id-blah/yellow_tripdata_2014-05-small.csv
          --metarPath
          gs://bucket-id-blah/lga-2014-05-metar.csv
          --writePath
          ???://spark-rf.pmml 
