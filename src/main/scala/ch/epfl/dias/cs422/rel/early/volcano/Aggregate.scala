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
    var option = input.next()
    if (option == NilTuple && groupSet.isEmpty) {
      case1 = true
    }
    while (option != NilTuple && !case1) {
      case2 = true
      var tuple = option.get
      tuplesFromLow = tuplesFromLow.:+(tuple)
      option = input.next()
    }
    index = 0
    if (case1) {
      //Pseudo-Code 1
      var tuple = IndexedSeq[RelOperator.Elem]()
      val agg = aggCalls.iterator
      while (agg.hasNext) {
        tuple = tuple.:+(agg.next().emptyValue)
      }
      tuples = tuples.:+(tuple)
    }
    if (case2) {
      if (!groupSet.isEmpty) {
        //Pseudo-Code 2
        // For each tuple, extract the field as given by groupSet and, for each field,
        // take the hash representation of that field and concatenate it
        val hashToFields = new mutable.HashMap[String, Tuple]()
        val hashToValues = new mutable.HashMap[String, List[Tuple]]()

        //form groups based on GROUP BY keys
        val tupleIter = tuplesFromLow.iterator
        while (tupleIter.hasNext) {
          val tuple = tupleIter.next()
          var colIter = groupSet.asList().iterator()
          var hashKey = ""
          //form hash of keys
          while (colIter.hasNext) {
            val i = colIter.next()
            hashKey += tuple(i).hashCode().toString + "_"
          }
          //given a tuple, check to which group it belongs
          if (hashToFields.contains(hashKey)) {
            //table containts group by key: insert new aggregates value

            var list = hashToValues(hashKey)
            /*
            var tupleToInsert = IndexedSeq[RelOperator.Elem]()
            i = 0
            colIter = tuple.iterator
            while (colIter.hasNext) {
              val col = colIter.next()
              if (!groupSet.asList().contains(i)) {
                tupleToInsert = tupleToInsert.:+(col)
              }
              i = i + 1
            }
            */
            list = list.:+(tuple)
            hashToValues.put(hashKey, list)
          }
          else {
            //table does not contain this group by key

            var listOfValues = List[Tuple]()

            var fields = IndexedSeq[RelOperator.Elem]()

            colIter = groupSet.asList().iterator()
            while (colIter.hasNext) {
              val i = colIter.next()
              fields = fields.:+(tuple(i))
            }
            listOfValues = listOfValues.:+(tuple)

            hashToFields.put(hashKey, fields)
            hashToValues.put(hashKey, listOfValues)
          }
        }
        //for each group...
        val groupIter = hashToValues.iterator
        while (groupIter.hasNext) {
          val entry = groupIter.next()
          val values = entry._2 //list of all tuples in the group
          val hashKey = entry._1
          var resultFin = IndexedSeq[RelOperator.Elem]()
          //for each aggregate call...
          val aggIter = aggCalls.iterator
          while (aggIter.hasNext) {
            val agg = aggIter.next()
            var resultInt = List[RelOperator.Elem]()
            val valIter = values.iterator
            while (valIter.hasNext) {
              val v = valIter.next() //a tuple in the group
              resultInt = resultInt.:+(agg.getArgument(v))
            }
            //reduce the tuples in the group by pairs until we have one element left
            while (resultInt.length > 1) {
              val args1 = resultInt.head
              val args2 = resultInt(1)

              val value = agg.reduce(args1, args2)
              resultInt = resultInt.drop(2).:+(value)
            }
            val finalValue = resultInt.head
            resultFin = resultFin.:+(finalValue)
          }
          var tuple = IndexedSeq[RelOperator.Elem]()
          val fields = hashToFields(hashKey)
          for (f <- fields) {
            tuple = tuple.:+(f)
          }
          for (r <- resultFin) {
            tuple = tuple.:+(r)
          }
          //add <fields, aggregate values> to tuples
          tuples = tuples.:+(tuple)
        }
      }
      else {
        //no group by
        val aggIter = aggCalls.iterator
        var resultFin = IndexedSeq[RelOperator.Elem]()
        while (aggIter.hasNext) {
          val agg = aggIter.next()
          var resultInt = List[RelOperator.Elem]()
          val valIter = tuplesFromLow.iterator
          while (valIter.hasNext) {
            val v = valIter.next()
            resultInt = resultInt.:+(agg.getArgument(v))
          }
          //reduce the tuples in the group by pairs until we have one element left
          while (resultInt.length > 1) {
            val args1 = resultInt.head
            val args2 = resultInt(1)

            val value = agg.reduce(args1, args2)
            resultInt = resultInt.drop(2).:+(value)
          }
          val finalValue = resultInt.head
          resultFin = resultFin.:+(finalValue)
        }
        var tuple = IndexedSeq[RelOperator.Elem]()
        for (r <- resultFin) {
          tuple = tuple.:+(r)
        }
        //add <fields, aggregate values> to tuples
        tuples = tuples.:+(tuple)
      }
    }
  }

  /**
    * @inheritdoc
    */
  override def next(): Option[Tuple] = {
    if (index >= tuples.length){
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
