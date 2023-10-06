package com.lsc

import utils.SimilarityScoreCalculation.jaccardSimilarity

import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import NetGraphAlgebraDefs.{NetGraph, NetModel, NetModelAlgebra}
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

object AlgoPerformance extends App {
  class AlgoPerformanceMap extends MapReduceBase with Mapper[Object, Text, Text, Text] {
    @throws[IOException]
    def map(key: Object, value: Text, output: OutputCollector[Text, Text],
            reporter: Reporter): Unit = {
      val line: String = value.toString
      // TODO
    }
  }
  class AlgoPerformanceReduce extends MapReduceBase with Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      // TODO
    }
  }
}
