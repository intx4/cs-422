package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{NilTuple, Tuple}
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable
import util.control.Breaks._



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
  private var index : Int = 0
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
      if (option.isEmpty){
        case1 = true
        break
      }
      else{
        val tuple = option.get
        if (tuple == NilTuple){
          break
        }
        else{
          tuplesFromLow = tuplesFromLow.appended(tuple)
          case2 = true
        }
      }
    }
    index = 0
    if (case1){
      //Pseudo-Code 1
      var tuple = IndexedSeq[RelOperator.Elem]()
      for (agg <- aggCalls){
        //how to create a tuple with the empty values??
        //a tuple is an IndexedSeq of RelOperator.Elem
        tuple = tuple.:+(agg.emptyValue)
      }
      tuples = tuples.:+(tuple)
    }
    if (case2){
      //Pseudo-Code 2
      // For each tuple, extract the field as given by groupSet and, for each field,
      // take the hash representation of that field and concatenate it
      val hashToFields = new mutable.HashMap[String, List[Tuple]]()
      val hashToValues = new mutable.HashMap[String, List[Tuple]]()

      //form groups based on GROUP BY keys
      for(tuple <- tuplesFromLow){
        var i = 0
        var hashKey = ""
        for(col <- tuple){
          if (groupSet.asList().contains(i)){
            hashKey += col.hashCode().toString + "_"
          }
          i = i + 1
        }
        if (hashToFields.contains(hashKey)){
          //table containts group by key: insert new aggregates value
          var list = hashToValues(hashKey)
          var tupleToInsert = IndexedSeq[RelOperator.Elem]()

          i = 0
          for(col <- tuple){
            if (!groupSet.asList().contains(i)){
              tupleToInsert = tupleToInsert.:+(col)
            }
            i = i + 1
          }
          list = list.:+(tupleToInsert)
          hashToValues.put(hashKey, list)
        }
        else{
          //table does not contain this group by key

          var listOfFields = List[Tuple]()
          var listOfValues = List[Tuple]()

          var fields = IndexedSeq[RelOperator.Elem]()
          var values = IndexedSeq[RelOperator.Elem]()

          i = 0
          for (col <- tuple){
            if (groupSet.asList().contains(i)){
              fields = fields.:+(col)
            }
            else{
              values = values.:+(col)
            }
            i = i +1
          }
          listOfFields = listOfFields.:+(fields)
          listOfValues = listOfValues.:+(values)

          hashToFields.put(hashKey, listOfValues)
          hashToValues.put(hashKey, listOfValues)
        }
      }
      //for each group...
      val iterator = hashToValues.iterator
      while (iterator.hasNext){
        //for each aggregate call...
        val entry = iterator.next()
        val values = entry._2
        val hashKey = entry._1
        var resultFin = IndexedSeq[RelOperator.Elem]()
        for (agg <- aggCalls){
          var resultInt = values
          //reduce the tuples in the group by pairs until we have one element left
          while (resultInt.length > 1) {
            val value1 = resultInt.head
            val value2 = resultInt(1)

            val args1 = agg.getArgument(value1)
            val args2 = agg.getArgument(value2)

            val value = agg.reduce(args1, args2).asInstanceOf[Tuple]
            resultInt = resultInt.drop(2).:+(value)
          }
          val finalValue  = agg.getArgument(resultInt.head)
          resultFin = resultFin.:+(finalValue)
        }
        var tuple = IndexedSeq[RelOperator.Elem]()
        val fields = hashToFields(hashKey)
        tuple = tuple.:+(fields)
        tuple = tuple.:+(resultFin)
        //add <fields, aggregate values> to tuples
        tuples = tuples.:+(tuple)
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (index == tuples.length){
      NilTuple
    }
    else{
      index = index + 1
      Some(tuples(index-1))
    }
  }

  /**
    * @inheritdoc
    */
  override def close(): Unit = {
    input.close()
  }
}
