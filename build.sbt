name := "MapReduceAkkaActors"

version := "0.1"

scalaVersion := "2.13.3"


// S'ha d'afegir la versió "-typed" a l'akka-actor
//libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % "2.6.9")
libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor-typed" % "2.6.9")
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"

enablePlugins(JavaAppPackaging)

