name := "scala-actors-example"

version := "0.1"

scalaVersion := "2.13.3"


// S'ha d'afegir la versió "-typed" a l'akka-actor
//libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % "2.6.9")
libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor-typed" % "2.6.9")
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"

enablePlugins(JavaAppPackaging)

/*libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)
*/