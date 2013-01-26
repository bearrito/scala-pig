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

  test("validates almost good date") {
    val expectFalse = ValidateGasPriceRecord.dateStringToMillis("Aug 12,2004")
    assert(expectFalse == false)
  }

  test("validates good date") {
    val expectFalse = ValidateGasPriceRecord.dateStringToMillis("Aug 12, 2004")
    assert(expectFalse == true)
  }


  test("converts a good date to millis"){

   val goodDate = DateToMillis.date2Millis("Jan 5, 2013")

   val lower = new DateTime(2013,1,4,12,1).millis
   val upper = new DateTime(2013,1,6,12,1).millis
    // I'm not trying to test joda or the wrapper. So I just make sure  it gave me some sort of long back
   assert(goodDate >  lower )
   assert(goodDate < upper)

  }



}
