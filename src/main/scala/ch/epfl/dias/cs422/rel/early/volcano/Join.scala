package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode

import scala.collection.mutable

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getLeftKeys]]
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Join.getRightKeys]]
  */
class Join(
    left: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    right: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Join[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](left, right, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var joined = List[Tuple]()
  private var index = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    var leftR = List[Tuple]()
    var option = left.next()
    var tuple = IndexedSeq[RelOperator.Elem]()
    //take left child relation
    if (option != NilTuple){
      tuple = option.get
    }
    while (option != NilTuple){
      leftR = leftR.:+(tuple)
      option = left.next()
      tuple = option.get
    }
    //build phase
    val hashToValues = new mutable.HashMap[String, Tuple]()
    for (tuple <- leftR){
      val iterator = getLeftKeys.iterator
      var hashKey = ""
      while(iterator.hasNext){
        val i = iterator.next()
        val col = tuple(i).hashCode().toString
        hashKey += col + "_"
      }
      hashToValues.put(hashKey, tuple)
    }
    option = right.next()
    while(option != NilTuple){
      tuple = option.get
      val iterator = getRightKeys.iterator
      var hashKey = ""
      while(iterator.hasNext){
        val i = iterator.next()
        val col = tuple(i).hashCode().toString
        hashKey += col + "_"
      }
      if (hashToValues.contains(hashKey)){
        var tupleToInsert = hashToValues(hashKey)
        tupleToInsert = tupleToInsert.:+(tuple)
        joined = joined.:+(tupleToInsert)
      }
      option = right.next()
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (index == joined.length){
      NilTuple
    }
    else{
      index = index + 1
      Some(joined(index-1))
    }
  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
