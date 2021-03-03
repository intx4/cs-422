package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import util.control.Breaks._
import java.util.Comparator
import scala.collection.JavaConverters._
import scala.reflect.internal.util.Collections


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Sort]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Sort protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    collation: RelCollation,
    offset: Option[Int],
    fetch: Option[Int]
) extends skeleton.Sort[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, collation, offset, fetch)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var sorted = List[Tuple]()
  private var index : Int = 0
  private var direction : RelFieldCollation.Direction = RelFieldCollation.Direction.ASCENDING
  private var ind : Int = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    //consume all unsorted tuples
    var option = input.next()
    while (option != NilTuple) {
        sorted = sorted.:+(option.get)
        option = input.next()
    }
    val collationList = collation.getFieldCollations
    val iterator = collationList.iterator()
    while(iterator.hasNext){
      val param = iterator.next()
      direction = param.getDirection
      ind = param.getFieldIndex
      //comparator.direction = direction
      //comparator.id = id
      //sorted.asJava.sort(comparator)
      if (direction == RelFieldCollation.Direction.DESCENDING) {
        sorted.sortWith(customCompare(_,_) > 0)
      }
      else{
        sorted.sortWith(customCompare(_,_) < 0)
      }
    }
    if (offset.isDefined){
      sorted = sorted.drop(offset.get)
    }
    if (fetch.isDefined){
      sorted = sorted.take(fetch.get)
    }
  }
  /*
  private val comparator = new Comparator[RelOperator.Tuple] {
    var direction : RelFieldCollation.Direction = RelFieldCollation.Direction.ASCENDING
    var id : Int = 0
    override def compare(o1: Tuple, o2: Tuple): Int = {
      val result = o1(id).asInstanceOf[Comparable[RelOperator.Elem]].compareTo(o2(id).asInstanceOf[Comparable[RelOperator.Elem]])
      if (direction.isDescending){
        if ( result > 0){
          //meaning o1(id) > o2(id)
          1
        }
        else if (result < 0){
          -1
        }
        else{
          0
        }
      }
      else {
        if (result > 0) {
          -1
        }
        else if (result < 0){
          1
        }
        else{
          0
        }
      }
    }
  }
  */
  def customCompare(t1: Tuple, t2:Tuple): Int = {
    t1(ind).asInstanceOf[Comparable[RelOperator.Elem]].compareTo(t2(ind).asInstanceOf[Comparable[RelOperator.Elem]])
  }
  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (index == sorted.length){
      NilTuple
    }
    else{
      index = index + 1
      Some(sorted(index-1))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
