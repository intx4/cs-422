package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import org.apache.calcite.rex.RexNode

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

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    left.open()
    right.open()
    val leftR = List[Tuple]()
    var tuple = left.next()
    while
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {

  }


  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    left.close()
    right.close()
  }
}
