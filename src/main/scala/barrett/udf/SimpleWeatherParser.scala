package barrett.udf

import barrett.udf.DateToMillis._
import org.apache.pig.EvalFunc
import org.apache.pig.data.{DataType, Tuple}
import org.apache.pig.impl.logicalLayer.schema.Schema
import org.apache.pig.data.TupleFactory

/**
 * Created with IntelliJ IDEA.
 * User: me
 * Date: 1/27/13
 * Time: 4:25 PM
 * To change this template use File | Settings | File Templates.
 */

object SimpleWeatherParser
{
  case class WeatherRecord(date : Long, stationId : Long, temp : Double)
  def string2WeatherRecord(input : String) : Option[WeatherRecord] = {

    try {
      val splits = input.split(' ')
      DateToMillis.date2Millis(splits(3)) match{
        case Some(d) => Some(WeatherRecord(d,splits(0).toLong,splits(7).toDouble))
        case None => None

      }

    }
    catch {

        case nfe : NumberFormatException => None
    }

  }
  def record2Tuple (record : WeatherRecord, tuple : Tuple) : Tuple = {

    tuple.set(0,record.date)
    tuple.set(1,record.stationId)
    tuple.set(2,record.temp)
    return tuple

  }


}

class SimpleWeatherParser extends  EvalFunc[Tuple]{
      def exec(input : Tuple) : Tuple = {

        var tuple = TupleFactory.getInstance().newTuple(3)
        val record = SimpleWeatherParser.string2WeatherRecord(input.get(0).asInstanceOf[String])
        record match {
          case Some(r) => SimpleWeatherParser.record2Tuple(r,tuple)
          case None => null


        }
      }

  override def outputSchema(input : Schema) : Schema = {
    val tuple = new Schema()

    val date = new Schema.FieldSchema("date",DataType.LONG)
    val stationId = new Schema.FieldSchema("stationId",DataType.LONG)
    val tempF =  new Schema.FieldSchema("temp",DataType.DOUBLE)
    tuple.add(date)
    tuple.add(stationId)
    tuple.add(tempF)
    val s = new Schema(new Schema.FieldSchema(getSchemaName(this.getClass().getName().toLowerCase(), input),tuple, DataType.TUPLE))
    s

  }


}
