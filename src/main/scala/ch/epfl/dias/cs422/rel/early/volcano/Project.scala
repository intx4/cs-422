package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode

import scala.jdk.CollectionConverters._

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Project]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Project protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    projects: java.util.List[_ <: RexNode],
    rowType: RelDataType
) extends skeleton.Project[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, projects, rowType)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * Function that, when given a (non-NilTuple) tuple produced by the [[input]] operator,
    * it returns a new tuple composed of the evaluated projections [[projects]]
    */
  lazy val evaluator: Tuple => Tuple =
    eval(projects.asScala.toIndexedSeq, input.getRowType)

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
    val option = input.next()
    if (option.isEmpty) {
      NilTuple
    }
    else {
      var tuple = option.get
        tuple = evaluator(tuple)
        Some(tuple)
      }
    }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}

/*
next():
  Project is the root of the algebraic tree. Project.next() should do something like that:
  1 - call input.next(), where input is another operator (e.g selection) depending on the query
  2 - if input.next() is a non nil tuple, then call eval that will perform something depending on the query.
      The operation to do are stored in projects.
 */