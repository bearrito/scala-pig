package barrett.udf

/**
 * Created with IntelliJ IDEA.
 * User: me
 * Date: 1/24/13
 * Time: 4:52 PM
 * To change this template use File | Settings | File Templates.
 */


import org.scala_tools.time.Imports._

import org.apache.pig.EvalFunc
import org.apache.pig.data.Tuple
import org.apache.pig.impl.logicalLayer.schema.Schema
import org.apache.pig.data.DataType
import org.joda.time.format.DateTimeParser
import org.joda.time.format.DateTimeFormatterBuilder
import java.lang.{Long => jLong}



object DateToMillis
{
  private def makeParser(s : String) : DateTimeParser =   DateTimeFormat.forPattern(s).getParser()
  implicit  def date2Millis(input : String, allowedDateStringFormats : List[String] = List("MMM dd, yyyy","yyyyMMdd")) : Option[jLong] = {

    val parsers = allowedDateStringFormats.map(s => makeParser(s)).toArray
    var builder = new DateTimeFormatterBuilder()
    val formatter = builder.append( null, parsers ).toFormatter();
    val richFormatter = RichDateTimeFormatter(formatter)
    richFormatter.parseOption(input) match
    {
      case Some(d) =>  Some(d.millis)
      case None => None
    }

  }
}


class DateToMillis extends EvalFunc[jLong]{
   def exec(input : Tuple) : java.lang.Long = {
     DateToMillis.date2Millis(input.get(0).asInstanceOf[String]) match
     {
       case Some(l) =>  l
       case None => null

     }
   }



}

