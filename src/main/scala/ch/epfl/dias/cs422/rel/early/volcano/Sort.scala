package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rel.RelCollation

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
  private var unsorted = List[Tuple]()
  private var index : Int = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    //consume all unsorted tuples
    val tuple = input.next()
    while (tuple.isDefined){
      unsorted = unsorted.:+(tuple.get)
    }
    val collationList = collation.getFieldCollations
    var iterator = collationList.iterator()
    while(iterator.hasNext){
      val param = iterator.next()
      val direction = param.getDirection
      val index = param.getFieldIndex





    }
  }


  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = ???

  /**
    * @inheritdoc
    */
  override def close(): Unit = ???
}
