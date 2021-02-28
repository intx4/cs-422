package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable
import util.control.Breaks._
import scala.collection.mutable.HashMap


/**
  * @inheritdoc
  * @see [[ch.epfl.dias.cs422.helpers.builder.skeleton.Aggregate]]
  * @see [[ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator]]
  * @see [[ch.epfl.dias.cs422.helpers.rex.AggregateCall]]
  */
class Aggregate protected (
    input: ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator,
    groupSet: ImmutableBitSet,
    aggCalls: IndexedSeq[AggregateCall]
) extends skeleton.Aggregate[
      ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
    ](input, groupSet, aggCalls)
    with ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator {

  private var tuples = List[Tuple]()
  private var head: Int = 0
  /**
    * @inheritdoc
    */
  override def open(): Unit = {
    input.open()
    var tuplesFromLow = List[Tuple]()
    var case1 = false
    var case2 = false
    breakable{
      val option = input.next()
      if (option == None){
        case1 = true
        break
      }
      else{
        var tuple = option.get
        if (tuple == NilTuple){
          break
        }
        else{
          tuplesFromLow = tuplesFromLow.appended(tuple)
          case2 = true
        }
      }
    }
    head = 0
    if (case1){
      //Pseudo-Code 1
      val tuple = IndexedSeq[RelOperator.Elem]()
      for (agg <- aggCalls){
        //how to create a tuple with the empty values??
        //a tuple is an IndexedSeq of RelOperator.Elem
        tuple.:+(agg.emptyValue)
      }
      tuples = tuples.appended(tuple)
    }
    if (case2){
      //Pseudo-Code 2
      // For each tuple, extract the field as given by groupSet and, for each field,
      // take the hash representation of that field and concatenate it
      for(tuple <- tuplesFromLow){
        var i = 0
        var hashKey = ""
        var hashToIndex = new mutable.HashMap[String, Int]()
        for(col <- tuple){
          if (groupSet.asList().contains(i)){
            hashKey += col.hashCode().toString + "_"
          }
          i = i + 1
        }
        if (hashToIndex.contains(hashKey)){
          var index = hashToIndex.get(hashKey)
        }
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (head == tuples.length){
      NilTuple
    }
    else{
      head = head + 1
      Some(tuples(head-1))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
