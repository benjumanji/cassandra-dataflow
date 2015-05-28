lazy val commonSettings = Seq(
  javacOptions ++= Seq(
    "-Xlint:deprecation",
    "-Xlint:unchecked"
  ),
  exportJars := true,
  organization := "com.sphonic",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.10.5",
  resolvers ++= Seq(
    "Sonatype releases" at "https://oss.sonatype.org/content/repositories/releases",
    "Typesafe releases" at "http://repo.typesafe.com/typesafe/releases/"
  )
)

lazy val root = project.in(file(".")).aggregate(cassandraDataflow)

lazy val cassandraDataflow = project.in(file("cassandra-dataflow"))
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % "2.1.3",
      "com.google.apis" % "google-api-services-storage" % "v1-rev25-1.19.1" exclude("com.google.guava", "guava-jdk5") ,
      "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "0.4.150414" exclude("org.slf4j", "slf4j-jdk14"),
      "com.google.http-client" % "google-http-client-jackson2" % "1.19.0",
      "joda-time" % "joda-time" % "2.7",
      "org.joda" %  "joda-convert" % "1.7"
    )
  )
