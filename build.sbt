import AssemblyKeys._  

assemblySettings

name := "ictgraph"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
        "org.apache.spark" % "spark-core_2.10" % "1.3.0" % "provided",
        "org.apache.spark" %% "spark-mllib" % "1.3.0" % "provided",
		"jaxen" % "jaxen" % "1.1.6",
		"dom4j" % "dom4j" % "1.6.1",
		"org.apache.spark" % "spark-graphx_2.10" % "1.3.0" % "provided",
        "org.apache.hive.hcatalog" % "hive-hcatalog-core" % "0.13.1" % "provided",
		"org.apache.hadoop" % "hadoop-common" % "2.5.0" % "provided",
		"org.apache.hadoop" % "hadoop-hdfs" % "2.5.0" % "provided",
		"org.apache.hive" % "hive" % "0.13.1" % "provided",
		"org.apache.hive" % "hive-serde" % "0.13.1" % "provided",
		"org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.5.0" % "provided"		
)

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

 
libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.scalatest" %% "scalatest" % "2.2.3-SNAP2" % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.3.0" % "provided", 
  "org.apache.hbase" % "hbase" % "0.98.1-cdh5.1.3" % "provided",
  "org.apache.hbase" % "hbase-client" % "0.98.1-cdh5.1.3" % "provided",
  "org.apache.hbase" % "hbase-common" % "0.98.1-cdh5.1.3" % "provided",
  "org.apache.hbase" % "hbase-server" % "0.98.1-cdh5.1.3" % "provided"
)

