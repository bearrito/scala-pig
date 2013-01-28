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
import org.scala_tools.time.RichDate

class Specs extends FunSuite {


  test("doesn't validate bad date") {
    val expectFalse = ValidateGasPriceRecord.dateStringToMillis("sdds")
    assert(expectFalse == false)
  }


  test("doesn't validate bad date that looks right") {
    val expectFalse = ValidateGasPriceRecord.dateStringToMillis("XXX 12,2004")
    assert(expectFalse == false)
  }

  test("doesn't validate bad date that looks right again") {
    val expectFalse = ValidateGasPriceRecord.dateStringToMillis("XXX 12, 2004")
    assert(expectFalse == false)
  }

  test("doesn't validate almost good date") {
    val expectTrue = ValidateGasPriceRecord.dateStringToMillis("Aug 12,2004")
    assert(expectTrue == false)
  }

  test("validates good date") {
    val expectTrue = ValidateGasPriceRecord.dateStringToMillis("Aug 12, 2004")
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


   //0137
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



}
