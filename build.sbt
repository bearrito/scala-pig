import AssemblyKeys._

name := "Scala-Pig"
     
version := "1.0"
      
scalaVersion := "2.10.0"

assemblySettings

resolvers += "JBoss" at "https://repository.jboss.org/nexus/content/groups/public/"

resolvers += "Apache Maven" at "http://mvnrepository.com/artifact"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Simile" at "http://simile.mit.edu/maven/"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"

libraryDependencies += "org.apache.pig" % "pig" % "0.8.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.0.2-alpha" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.1.1" % "provided"

libraryDependencies += "joda-time" % "joda-time" % "2.1"

libraryDependencies += "org.scalaj" % "scalaj-time_2.10.0-M7" % "0.6"
