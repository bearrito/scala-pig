package barrett.udf

/**
 * Created with IntelliJ IDEA.
 * User: me
 * Date: 1/26/13
 * Time: 12:55 AM
 * To change this template use File | Settings | File Templates.
 */

import org.scalatest.FunSuite
import org.scala_tools.time.Imports._
import org.apache.pig.data.{DefaultTuple,TupleFactory,DefaultTupleFactory,DefaultDataBag}
import scala.collection.JavaConversions._

class Specs extends FunSuite {


  test("doesn't validate bad date") {
    val expectFalse = ValidateSensorLogRecord.dateStringToMillis("sdds")
    assert(expectFalse == false)
  }
  test("doesn't validate bad date that looks right") {
    val expectFalse = ValidateSensorLogRecord.dateStringToMillis("XXX 12,2004")
    assert(expectFalse == false)
  }
  test("doesn't validate bad date that looks right again") {
    val expectFalse = ValidateSensorLogRecord.dateStringToMillis("XXX 12, 2004")
    assert(expectFalse == false)
  }
  test("doesn't validate almost good date") {
    val expectTrue = ValidateSensorLogRecord.dateStringToMillis("Aug 12,2004")
    assert(expectTrue == false)
  }
  test("validates good date") {
    val expectTrue = ValidateSensorLogRecord.dateStringToMillis("Aug 12, 2004")
    assert(expectTrue == true)
  }
  test("parses good date"){

    val expectSome = DateToMillis.date2Millis("20100112")
    val isSome = expectSome match {
      case Some(d) => true
      case None => false


    }
    assert(isSome == true)

  }
  test("converts a good date to millis"){

   val goodDate = DateToMillis.date2Millis("Jan 5, 2013")

    val isSome = goodDate  match {
      case Some(d) => true
      case None => false


    }
    assert(isSome == true)

   val lower = new DateTime(2013,1,4,12,1).millis
   val upper = new DateTime(2013,1,6,12,1).millis
    // I'm not trying to test joda or the wrapper. So I just make sure  it gave me some sort of long back
   assert(goodDate.get >  lower)
   assert(goodDate.get <  upper)

  }
  test("dateToMilli udf parses date correctly"){

      val d2m = new DateToMillis()
      val factory = new DefaultTupleFactory()
      val tuple = factory.newTuple(1)
      tuple.set(0,"Aug 12, 2004")
      val result = d2m.exec(tuple)
      assert(result == 1092283200000L)



  }
  test("dateToMilli udf doesn't parse a bad date"){
    val d2m = new DateToMillis()
    val factory = new DefaultTupleFactory()
    val tuple = factory.newTuple(1)
    tuple.set(0,"Aug 1,2004")
    val result = d2m.exec(tuple)
    assert(result == null)
  }

  test("weather parser parses good record"){
    val input = "722057 12854  20100101    60.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000"
    val expected = SimpleWeatherParser.WeatherRecord(1262322000000L,722057, 60.9)
    val actual = SimpleWeatherParser.string2WeatherRecord(input)
    val isSome = actual match{
      case Some(d) => true
      case _ =>  false }

    assert(isSome == true)
    val actualGot = actual.get
    assert(actualGot == expected)
  }
  test("weather parser does not parse bad record"){
    val input = "722057 12854  20100101      60.9 24    58.0 24  1017.0 15  1015.3 12    7.1 24    5.0 24   20.0  999.9    68.0    48.9   0.26G 999.9  000000"
    val expected = SimpleWeatherParser.WeatherRecord(20100101,722057, 60.9)
    val actual = SimpleWeatherParser.string2WeatherRecord(input)
    val isSome = actual match{
      case Some(d) => true
      case None =>  false }

    assert(isSome == false)
  }

  test("Initial executes correctly"){

    import org.apache.pig.data.DataByteArray
    import java.nio._
    val d2m = new DateToMillis()
    val factory = new DefaultTupleFactory()
    val tuple = factory.newTuple(1)

    tuple.set(0, scala.math.exp(4.0))
    val tuples =    List(tuple)

    val dbag = new DefaultDataBag(tuples)
    val dtuple = factory.newTuple(1)
    dtuple.set(0,dbag)
    val stage = new Initial
    val r = stage.exec(dtuple)
    assert(r.get(0) == 4.0)
    assert(r.get(1) == 1)
  }

  test("Intermediate executes correctly"){

    import org.apache.pig.data.DataByteArray
    import java.nio._
    val d2m = new DateToMillis()
    val factory = new DefaultTupleFactory()
    val tuple = factory.newTuple(2)

    tuple.set(0,4.0)
    tuple.set(1,1L)
    val tuples =    List(tuple,tuple)

    val dArray = new DataByteArray()
    val dbag = new DefaultDataBag(tuples)
    val dtuple = factory.newTuple(1)
    dtuple.set(0,dbag)
    val stage = new Intermediate
    val r = stage.exec(dtuple)
    assert(r.get(0) == 8.0)
    assert(r.get(1) == 2)
  }

  test("Final executes correctly"){

    import org.apache.pig.data.DataByteArray
    import java.nio._
    val d2m = new DateToMillis()
    val factory = new DefaultTupleFactory()
    val tuple = factory.newTuple(2)

    tuple.set(0, 4.0)
    tuple.set(1,1L)
    val tuples =    List(tuple,tuple)

    val dArray = new DataByteArray()
    val dbag = new DefaultDataBag(tuples)
    val dtuple = factory.newTuple(1)
    dtuple.set(0,dbag)
    val stage = new Final()
    val r = stage.exec(dtuple)
    assert(r == scala.math.exp(8.0 /2))

  }

}
