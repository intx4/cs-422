package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.store.{RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Scan]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  */
class Scan protected (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableToStore: ScannableTable => Store //it's a function taking a ScannableTable and returning a Store
) extends skeleton.Scan[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](cluster, traitSet, table)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  /**
    * A [[Store]] is an in-memory storage the data.
    *
    * Accessing the data is store-type specific and thus
    * you have to convert it to one of the subtypes.
    * See [[getRow]] for an example.
    */
  protected val scannable: Store = tableToStore(
    table.unwrap(classOf[ScannableTable])
  )
  protected var rowId: Int

  /**
    * Helper function (you do not have to use it or implement it)
    * It's purpose is to show how to convert the [[scannable]] to a
    * specific [[Store]].
    *
    * @param rowId row number (startign from 0)
    * @return the row as a Tuple
    */
  private def getRow(rowId: Int): Tuple = {
    scannable match {
      case rowStore: RowStore =>
        /**
          * For this project, it's safe to assume scannable will always
          * be a [[RowStore]].
          */
        val rowStore: RowStore = scannable.asInstanceOf[RowStore]
        rowStore.getRow(rowId)
    }
  }

  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    var rowId = 0
  }

   /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (rowId != -1) {
      val tuple = getRow(rowId)
      rowId = rowId + 1
      Some(tuple) //builds an Option[Tuple]
    }
    else{
      None //builds a Nil type for Option[Tuple]
    }
  }
  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    rowId = -1
  }
}
