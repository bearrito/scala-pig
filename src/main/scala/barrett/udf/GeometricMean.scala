package barrett.udf



import org.apache.pig.data.{Tuple => pigTuple, DataByteArray, TupleFactory, DataBag}
import org.apache.pig.{Algebraic,EvalFunc}
import java.lang.{Long => jLong,Double => jDouble,Integer => jInt}
import scala.collection.JavaConversions._
import java.lang


/**
 * Created with IntelliJ IDEA.
 * User: bearrito
 * Date: 4/13/13
 * Time: 2:58 PM
 * To change this template use File | Settings | File Templates.
 */


object GeometricMean{

  val mTupleFactory = TupleFactory.getInstance()

  abstract trait TupleParse
  case object FailedParse extends TupleParse
  case class LazyAverage(value : jDouble, count : jLong ) extends TupleParse
  object LazyAverage
  {

    def addLazyAverage(t1 : TupleParse, t2 : TupleParse) : LazyAverage = {

      t1 match {
        case FailedParse => {
          t2 match  {
            case FailedParse => LazyAverage(0.0,0L)
            case LazyAverage(z,y) => LazyAverage(z,y)
          }
        }
        case LazyAverage(r,c) => {
          t2 match  {
            case FailedParse => LazyAverage(r,c)
            case LazyAverage(z,y) => LazyAverage(z + r,y + c)
          }
        }


      }


    }

    def lazyAverageToTuple(tuple : TupleParse) : pigTuple = {
        tuple match
        {
          case FailedParse => null
          case LazyAverage(r,c) =>  {
            val output = mTupleFactory.newTuple(2);
            output.set(0,r )
            output.set(1,c)
            output
          }
        }
    }

  }

  def tupleToLazyAverage(t : pigTuple) : TupleParse = {

    try{LazyAverage(t.get(0).asInstanceOf[jDouble],t.get(1).asInstanceOf[jLong])}
    catch {case e: Exception => FailedParse  }
  }

  def combine(bag : DataBag) : pigTuple  = {
    val r = bag.iterator()
            .map(t => tupleToLazyAverage(t))
            .foldLeft(LazyAverage(0.0,0L))((acc,t) => LazyAverage.addLazyAverage(acc,t) )
    LazyAverage.lazyAverageToTuple(r)
  }


}


class GeometricMean extends  EvalFunc[jDouble] with Algebraic {
  override def exec(input : pigTuple)  : jDouble = {

    5.0


  }
  def getInitial() : String = {
    return new Initial().getClass.getCanonicalName
  }
  def getIntermed() : String = {
    return new Intermediate().getClass.getCanonicalName
  }
  def getFinal() : String = {
    return new Final().getClass().getCanonicalName
  }



}


class Intermediate extends  EvalFunc[pigTuple]  {
  override def exec(input : pigTuple) : pigTuple = {
    val b = input.get(0).asInstanceOf[DataBag]
    GeometricMean.combine(b)
  }


}
class Final extends EvalFunc[jDouble] {
  val mTupleFactory = TupleFactory.getInstance()

  override def exec(input : pigTuple) : lang.Double = {
    val b = input.get(0).asInstanceOf[DataBag]
    val combined = GeometricMean.combine(b)
    val sum = combined.get(0).asInstanceOf[jDouble]
    val count = combined.get(1).asInstanceOf[jLong]
    scala.math.exp(sum / count)





  }

}
class Initial extends EvalFunc[pigTuple] {
  override def exec(input : pigTuple) : pigTuple  = {

    val t = TupleFactory.getInstance().newTuple(2);
    val bg =  input.get(0).asInstanceOf[DataBag]
    bg.iterator().hasNext() match
    {
      case true =>  {
        val tp = bg.iterator().next()
        t.set(0, scala.math.log( tp.get(0).asInstanceOf[Double]))
        t.set(1, 1L)
        t

      }
      case false => {
        t.set(0, null)
        t.set(1, 0L)
        t
      }
    }

  }
}