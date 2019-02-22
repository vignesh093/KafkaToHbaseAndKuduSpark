name := "KafkaToHbaseAndKuduSpark"

version := "0.1"
import sbtassembly.MergeStrategy

scalaVersion := "2.11.10"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.0" 

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0" 

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.0"

libraryDependencies += "org.apache.kudu" % "kudu-client" % "1.7.0-cdh5.16.1"

libraryDependencies += "org.apache.kudu" % "kudu-spark2_2.11" % "1.7.0-cdh5.16.1"

// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"

resolvers += "Cloudera Repositories" at "https://repository.cloudera.com/artifactory/cloudera-repos"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}