package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rex.RexNode
import util.control.Breaks._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Filter]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Filter protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    condition: RexNode
) extends skeleton.Filter[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, condition)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, evaluates the predicate [[condition]]
    * on a (non-NilTuple) tuple produced by the [[input]] operator
    */
  lazy val predicate: Tuple => Boolean = {
    val evaluator = eval(condition, input.getRowType)
    (t: Tuple) => evaluator(t).asInstanceOf[Boolean]
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    var found = false
    var option = input.next()
      while (option != NilTuple) {
          val tuple = option.get
          found = predicate(tuple)
          if (found) {
            return Some(tuple)
          }
          else{//not found
            option = input.next()
          }
        }
    NilTuple
  }
  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
