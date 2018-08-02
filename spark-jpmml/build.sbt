name := "spark-jpmml"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.1" // => 2.2.1 is the version supported by DataProc 1.2
//val sparkVersion = "2.3.0"
val jpmmlVersion = "1.4.1"

// single % to denote non-dependence on scala version.
// consider excluding pmml-model inside spark per docs recommendation
libraryDependencies ++= Seq(
  //"org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  //("org.apache.spark" %% "spark-mllib" % sparkVersion).exclude("org.jpmml", "pmml-model"),
  ("org.apache.spark" %% "spark-mllib" % sparkVersion % "provided").exclude("org.jpmml", "pmml-model"),
  //"org.apache.hadoop" % "hadoop-client" % "2.7.2", // to run SparkModelToPMML; there is a guava version conflict.
  "org.jpmml" % "jpmml-sparkml" % "1.3.7",
  //"org.jpmml" % "jpmml-evaluator-spark" % "1.1.0"
  "org.jpmml" % "pmml-evaluator" % jpmmlVersion,
  "org.jpmml" % "pmml-evaluator-extension" % jpmmlVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
  // NominatimClient dependencies. Conflict with spark logger.
  //"ch.qos.logback" %  "logback-classic" % "1.2.3",
  //"org.dispatchhttp" %% "dispatch-core"   % "0.14.0",
  //"com.typesafe.play" %% "play-json" % "2.6.8",
)

// ref: http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin#spark-2-deduplicate-different-file-contents-found-in-the-following
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
