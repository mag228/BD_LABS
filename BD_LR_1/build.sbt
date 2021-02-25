name := "lab1"
version := "0.1"
scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "2.4.7" ,
 "org.apache.spark" %% "spark-sql" % "2.4.7"
).map(_.exclude("org.slf4j", "*"))
libraryDependencies ++= Seq(
 "ch.qos.logback" % "logback-classic" % "1.1.3"
)
libraryDependencies += "com.thoughtworks.paranamer" % "paranamer" % "2.8"