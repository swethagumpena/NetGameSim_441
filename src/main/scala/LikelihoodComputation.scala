package com.lsc

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

object LikelihoodComputation extends App {
  class LikelihoodComputationMap extends MapReduceBase with Mapper[Object, Text, Text, Text] {
    
    def findBestScore(key: String, value: String): (String, Double) = {
      val selectedElement = key match {
        case k if k.startsWith("O_") =>
          val maxScoreElement = value.split(", ").maxBy { element =>
             val Array(_, score) = element.split('=')
             score.toDouble
          }
          val maxScore = maxScoreElement.split('=')(1).toDouble
          if (maxScore > 0.5 && maxScore <= 0.9)
            s"${key.stripPrefix("O_")}=0.0"
          else
            s"${key.stripPrefix("O_")}=$maxScore"

        case k if k.startsWith("P_") =>
          val filteredElements = value.split(", ").filter { element =>
             val Array(_, score) = element.split('=')
             val scoreValue = score.toDouble
             scoreValue < 0.1 || scoreValue > 0.9
          }
          if (filteredElements.nonEmpty) {
            filteredElements.minBy { element =>
               val Array(_, score) = element.split('=')
               score.toDouble
            }
          } else {
            s"${key.stripPrefix("P_")}=0.0"
          }
      }

      val Array(resultKey, resultValue) = selectedElement.split('=')
      (resultKey, resultValue.toDouble)
    }

    @throws[IOException]
    def map(key: Object, value: Text, output: OutputCollector[Text, Text],
            reporter: Reporter): Unit = {
      val line: String = value.toString
      val parts = line.split(":")

      val result = findBestScore(parts(0), parts(1))

      val nodeIds = result._1
      val highestScore = result._2

      val edgePattern = """[A-Z]_(\d+)-(\d+)""".r
      val nodePattern = """[A-Z]_(\d+)""".r

      parts(0) match {
        case edgePattern(x, y) =>
          if (parts(0).startsWith("O_")) {
            val category =
              if (highestScore > 0.9) "UnchangedEdges"
              else if (highestScore > 0.1) {
                "ModifiedEdges"
              } else {
                "RemovedEdges"
              }
            output.collect(new Text(category), new Text(nodeIds))
          } else if (parts(0).startsWith("P_")) {
            if (highestScore == 0.0) {
              val category = {
                "AddedEdges"
              }
              output.collect(new Text(category), new Text(nodeIds))
            }
          }
        case nodePattern(id) =>
          if (parts(0).startsWith("O_")) {
            val category =
              if (highestScore > 0.9) "UnchangedNodes"
              else if (highestScore > 0.1) "ModifiedNodes"
              else "RemovedNodes"
            output.collect(new Text(category), new Text(nodeIds))
          } else if (parts(0).startsWith("P_")) {
            if (highestScore == 0.0) {
              val category = "AddedNodes"
              output.collect(new Text(category), new Text(nodeIds))
            }
          }
        // Default case for unmatched format
        case _ =>
      }

    }
  }

  class LikelihoodComputationReduce extends MapReduceBase with Reducer[Text, Text, Text, Text] {
    override def reduce(label: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      val result = values.asScala.map(_.toString).mkString(", ")
      if (result.nonEmpty) {
        output.collect(label, new Text(result))
      }
    }
  }
}
