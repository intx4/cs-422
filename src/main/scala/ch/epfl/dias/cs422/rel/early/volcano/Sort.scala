package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}

import util.control.Breaks._
import java.util.Comparator
import scala.collection.JavaConverters._
import scala.math.Ordering.Implicits.seqOrdering
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
  private val collationList = collation.getFieldCollations

  object customOrdering extends Ordering[Tuple]{
    override def compare(t1: Tuple, t2: Tuple): Int = {
      //Remember: DESC = > 0, ASC < 0
      val collationIter = collationList.iterator()
      var param = collationIter.next()
      var id = param.getFieldIndex
      var dir = param.getDirection
      var result = t1(id).asInstanceOf[Comparable[RelOperator.Elem]].compareTo(t2(id).asInstanceOf[Comparable[RelOperator.Elem]])
      while (result == 0 && collationIter.hasNext){
        param = collationIter.next()
        id = param.getFieldIndex
        dir = param.getDirection
        result = t1(id).asInstanceOf[Comparable[RelOperator.Elem]].compareTo(t2(id).asInstanceOf[Comparable[RelOperator.Elem]])
      }
      if (dir == RelFieldCollation.Direction.DESCENDING){
        result
      }
      else {
        (-1)*result
      }
    }
  }

  private var pq = new collection.mutable.PriorityQueue[Tuple]()(customOrdering)
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    var option = input.next()
    while (option != NilTuple) {
      pq.enqueue(option.get)
      option = input.next()
    }
    if (offset.isDefined){
      pq = pq.drop(offset.get)
    }
    if (fetch.isDefined){
      pq = pq.take(fetch.get)
    }
  }
    /*
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
      //comparator.direction = direction
      //comparator.id = id
      //sorted.asJava.sort(comparator)
      sorted = sorted.sortWith(customCompare(_,_))
    }
    if (offset.isDefined){
      sorted = sorted.drop(offset.get)
    }
    if (fetch.isDefined){
      sorted = sorted.take(fetch.get)
    }
  }

  def customCompare(t1: Tuple, t2:Tuple): Boolean = {
    //Remember: DESC = > 0, ASC < 0
    val collationIter = collationList.iterator()
    var param = collationIter.next()
    var id = param.getFieldIndex
    var dir = param.getDirection
    var result = t1(id).asInstanceOf[Comparable[RelOperator.Elem]].compareTo(t2(id).asInstanceOf[Comparable[RelOperator.Elem]])
    while (result == 0 && collationIter.hasNext){
      param = collationIter.next()
      id = param.getFieldIndex
      dir = param.getDirection
      result = t1(id).asInstanceOf[Comparable[RelOperator.Elem]].compareTo(t2(id).asInstanceOf[Comparable[RelOperator.Elem]])
    }
    if (result == 0){
      return false
    }
    if (dir == RelFieldCollation.Direction.ASCENDING){
      if (result < 0){
        true
      }
      else{
        false
      }
    }
    else{
      if (result < 0){
        false
      }
      else{
        true
      }
    }
  }
  */
  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    /*
    if (index == sorted.length){
      NilTuple
    }
    else{
      index = index + 1
      Some(pq.dequeue())
    }
   */
    if (pq.isEmpty){
      NilTuple
    }
    else{
      Some(pq.dequeue())
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
