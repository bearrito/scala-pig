scala-pig
=========

Wanted to create PIG udfs in scala.

I had not previously seen a working example of a PIG udf that was written in pure scala.

This repo currently contains two working examples. They are domain specific for me but the syntax might nevertheless be useful.

Because of the dependencies involved the project uses the sbt-assembly plugin to package up the needed .jars


Building 
========
You should simply be able to do
  sbt reload
  sbt update
  sbt compile
  sbt test
  sbt assembly

