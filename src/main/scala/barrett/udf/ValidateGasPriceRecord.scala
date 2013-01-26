package barrett.udf

import org.scala_tools.time.Imports._
import org.apache.pig.{EvalFunc, FilterFunc}
import org.apache.pig.data.Tuple





/**
 * Created with IntelliJ IDEA.
 * User: me
 * Date: 1/25/13
 * Time: 10:17 PM
 * To change this template use File | Settings | File Templates.
 */




class ValidateGasPriceRecord extends FilterFunc {


  def exec(input : Tuple)   = {
        ValidateGasPriceRecord.dateStringToMillis(input.get(0).asInstanceOf[String])
  }
}

object ValidateGasPriceRecord
{

  implicit  def dateStringToMillis(input : String) : Boolean = {

    val formatter = DateTimeFormat.forPattern("MMM dd, yyyy")
    val richFormatter = RichDateTimeFormatter(formatter)

    try{

      val date = richFormatter.parseOption(input)
      date match
      {
        case Some(d) =>  true


        case None => false
      }
    }
    catch{
      case e : Throwable => false
    }


  }


}
