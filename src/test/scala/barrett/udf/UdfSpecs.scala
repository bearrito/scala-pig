package barrett.udf

/**
 * Created with IntelliJ IDEA.
 * User: me
 * Date: 1/28/13
 * Time: 8:30 PM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.fs.Path
import org.apache.pig.pigunit.{PigTest,Cluster}
import org.apache.pig.pigunit.pig.{PigServer,GruntParser}
import org.junit._
import org.scalatest.junit._
import org.apache.commons.math.stat.clustering.Cluster


class UdfSpecs extends JUnitSuite {

    private  var  test : PigTest = _
    private  var cluster  : org.apache.pig.pigunit.Cluster = _
    private  val PIG_SCRIPT : String = "/home/bearrito/Git/scala-pig/src/main/pig/top_queries.pig"

    @Before def init()  = {
    cluster = PigTest.getCluster()
      cluster.update(
        new Path("/home/bearrito/Git/scala-pig/src/main/pig/top_queries_input_data.txt"),
        new Path("top_queries_input_data.txt"))

 	  }



  @Test
  def VerifySetupIsCorrectWithKnownScript {
        val args = List("n=3","reducers=1","input=top_queries_input_data.txt","output=top_3_queries").toArray
        test = new PigTest(PIG_SCRIPT, args)

  val inputA = List(
                "yahoo\t10",
                "twitter\t7",
                "facebook\t10",
                "yahoo\t15",
                "facebook\t5",
                "a\t1",
                "b\t2",
                "c\t3",
                "d\t4",
                "e\t5"
        ).toArray

        val outputA = List(
                "(yahoo,25)",
                "(facebook,15)",
                "(twitter,7)"
        ).toArray

        test.assertOutput("data", inputA, "queries_limit", outputA)
  }

  @Test
  @Ignore
  def VerifyWeatherRecordsAreParsed {

    val args = List("n=3","reducers=1","input=top_queries_input_data.txt","output=top_3_queries").toArray
    test = new PigTest("/home/bearrito/Git/scala-pig/src/main/pig/WeatherRecordParse.pig", args)

    val input = List(
      "722057 12854  20100101    40.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000",
      "722058 12854  20100101    40.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000",
      "722059 12854  20100101    40.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000"
    ).toArray

    val output = List(
      "(1262322000000,722057,40.9)",
      "(1262322000000,722058,40.9)",
      "(1262322000000,722059,40.9)"
    ).toArray

    test.assertOutput("rawWeather", input, "cleanWeather", output);
  }
  @Test
  @Ignore
  def VerifyWeatherRecordsAreParsedFromLocal {


    val args = List("n=3","reducers=1","input=top_queries_input_data.txt","output=top_3_queries").toArray
    val script = List("rawWeather = LOAD '$input'  as (s : CHARARRAY);",
                      "cleanWeather = FOREACH rawWeather GENERATE FLATTEN(barrett.udf.SimpleWeatherParser(s));").toArray
    test = new PigTest(script, args)

    val input = List(
      "722057 12854  20100101    40.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000",
      "722058 12854  20100101    40.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000",
      "722059 12854  20100101    40.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000"
    ).toArray

    val output = List(
      "(1262322000000,722057,40.9)",
      "(1262322000000,722058,40.9)",
      "(1262322000000,722059,40.9)"
    ).toArray
    test.assertOutput("rawWeather", input, "cleanWeather", output);
  }
  @Test
  @Ignore
  def VerifySensorAverages{

    val args = List("n=3","reducers=1","input=/home/bearrito/Git/EMR-DEMO/code/resources/mapper_input","output=top_3_queries").toArray
    test = new PigTest("/home/bearrito/Git/scala-pig/src/main/pig/ComputeSensorAverage.pig", args)

    val output = List(
    "( Machine-1,-0.0064942719924217076,0.32952057587479033)",
    "( Machine-2,1.0070450038667758,0.3297018000232449)" ,
    "( Machine-3,2.0017185716988575,0.3432592560207814)"
    ).toArray

    test.assertOutput("grouped_channel_averages", output);


  }
  @Test
  @Ignore
  def VerifyDateToMillisParser{

    val args = List("n=3","reducers=1","input=top_queries_input_data.txt","output=top_3_queries").toArray


    val script = List("datesAsYYYYMMDD =  LOAD '$input'  as ( d : CHARARRAY , stub : CHARARRAY);",
                      "datesAsMillis =    FOREACH datesAsYYYYMMDD  GENERATE FLATTEN(barrett.udf.DateToMillis(d));"
    ).toArray;

    val input = List(
      "20100101\ttest",
      "20121101\ttest"
    ).toArray

    val output = List(
      "(1262322000000)",
      "(1351742400000)"
    ).toArray

    test = new PigTest(script,args);
    test.assertOutput("datesAsYYYYMMDD", input, "datesAsMillis", output);



  }



}
