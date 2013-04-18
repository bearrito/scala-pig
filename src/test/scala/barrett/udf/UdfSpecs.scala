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
import scala.io.Source
import org.apache.commons.math.stat.clustering.Cluster


class UdfSpecs extends JUnitSuite {

    private  var  test : PigTest = _
    private  var cluster  : org.apache.pig.pigunit.Cluster = _
    private  val PIG_SCRIPT : String = getClass().getResource("/top_queries.pig").getFile()

    @Before def init()  = {
    cluster = PigTest.getCluster()
      cluster.update(
        new Path(getClass().getResource("/top_queries_input_data.txt").getFile()),
        new Path("top_queries_input_data.txt"))



      cluster.update(new Path( getClass().getResource("/mapper_input").getFile()),new Path("mapper_input"))

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
  def VerifyWeatherRecordsAreParsed {



    val args = List("n=3","reducers=1","input=top_queries_input_data.txt","output=top_3_queries").toArray
    test = new PigTest(getClass().getResource("/WeatherRecordParse.pig").getFile(), args)

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
  def VerifySensorAverages{

    val args = List("n=3","reducers=1","input=mapper_input","output=sensor_output").toArray
    test = new PigTest(getClass().getResource("/ComputeSensorAverage.pig").getFile(), args)



    val output = List(
    "(Machine-1,0.011345351009114579,0.3370030595954243)",
    "(Machine-2,1.0195438951198499,0.3416909992621829)",
    "(Machine-3,1.9994550564065363,0.328697570097795)"
    ).toArray

    test.assertOutput("grouped_channel_averages", output);


  }
  @Test
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
    @Test
    def VerifyHarmonic{

      val args = List("n=3","reducers=1","input=top_queries_input_data.txt","output=top_3_queries").toArray


      val script = List("records = LOAD '$input'  as (id : INT, d : Double , stub : CHARARRAY);",
                        "groups = GROUP records  BY id;" ,
                        "result = FOREACH groups GENERATE group, barrett.udf.GeometricMean(records.d);"
      ).toArray;

      val input = List(
        "1\t8.0\ttest",
        "1\t8.0\ttest"
      ).toArray

      val output = List(
        "(1,7.999999999999998)"
      ).toArray

      test = new PigTest(script,args);
      test.assertOutput("records", input, "result", output);

  }

}


