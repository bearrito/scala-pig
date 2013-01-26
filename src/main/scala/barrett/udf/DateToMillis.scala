package barrett.udf

/**
 * Created with IntelliJ IDEA.
 * User: me
 * Date: 1/24/13
 * Time: 4:52 PM
 * To change this template use File | Settings | File Templates.
 */


import org.apache.pig.EvalFunc
import org.apache.pig.data.Tuple
import org.scala_tools.time.Imports._



object DateToMillis
{

  implicit  def date2Millis(input : String) : Long = {

    val formatter = DateTimeFormat.forPattern("MMM dd, yyyy")
    val richFormatter = RichDateTimeFormatter(formatter)
    val date = richFormatter.parseOption(input)
    date match
    {
      case Some(d) =>  d.millis
      case None => throw new IllegalArgumentException
    }

  }


}


class DateToMillis extends EvalFunc[Long]{
   def exec(input : Tuple) :Long = DateToMillis.date2Millis(input.get(0).asInstanceOf[String])
}
