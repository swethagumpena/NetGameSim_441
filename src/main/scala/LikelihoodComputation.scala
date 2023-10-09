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

  // Find the best score for a given key-value pair
  def findBestScore(key: String, value: String): (String, Double) = {
    logger.trace(s"Finding best score for key: $key, value: $value")
    val selectedElement = key match {
      // Prefixed with O_ : To find Modified/Removed/Unchanged
      case k if k.startsWith("O_") =>
        val maxScoreElement = value.split(", ").maxBy { element =>
          val Array(_, score) = element.split('=')
          score.toDouble
        }
        // Returning the max score to compare with the range thresholds
        val maxScore = maxScoreElement.split('=')(1).toDouble
        if (maxScore > 0.5 && maxScore <= 0.9)
          s"${key.stripPrefix("O_")}=0.0"
        else
          s"${key.stripPrefix("O_")}=$maxScore"

      // Prefixed with P_ : To find Added
      case k if k.startsWith("P_") =>
        // If there are no elements in the range 0.1 to 0.9, would be considered to be added
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
    // Key had the nodeIds and value has the score
    (resultKey, resultValue.toDouble)
  }
  class LikelihoodComputationMap extends MapReduceBase with Mapper[Object, Text, Text, Text] {
    @throws[IOException]
    def map(key: Object, value: Text, output: OutputCollector[Text, Text],
            reporter: Reporter): Unit = {
      logger.debug(s"Mapping: key = $key, value = ${value.toString}")
      val line: String = value.toString
      val parts = line.split(":")

      // Find the best score for the given key-value pair
      val result = findBestScore(parts(0), parts(1))

      val nodeIds = result._1
      val highestScore = result._2

      // Define regular expressions to match edge and node patterns

      // Node is of the format: O_1 or P_1
      val nodePattern = """[A-Z]_(\d+)""".r
      // Edge is of the format: O_1-2 or P_1-2
      val edgePattern = """[A-Z]_(\d+)-(\d+)""".r

      parts(0) match {
        case edgePattern(x, y) =>
          if (parts(0).startsWith("O_")) {
            // Determine the category based on the highest score for edges
            val category =
              // If the max score is greater than 0.9, it means there is maximum similarity and we can say that the edge has remained unchanged
              if (highestScore > 0.9) "UnchangedEdges"
              else if (highestScore > 0.1) "ModifiedEdges"
              else "RemovedEdges"
            output.collect(new Text(category), new Text(nodeIds))
          } else if (parts(0).startsWith("P_")) {
            // If there are no elements in a particular range, it is considered to be added
            if (highestScore == 0.0) {
              val category = {
                "AddedEdges"
              }
              // Output the category and node IDs
              output.collect(new Text(category), new Text(nodeIds))
            }
          }
        case nodePattern(id) =>
          if (parts(0).startsWith("O_")) {
            // Determine the category based on the highest score for nodes
            val category =
              // If the max score is greater than 0.9, it means there is maximum similarity and we can say that the edge has remained unchanged
              if (highestScore > 0.9) "UnchangedNodes"
              else if (highestScore > 0.1) "ModifiedNodes"
              else "RemovedNodes"
            output.collect(new Text(category), new Text(nodeIds))
          } else if (parts(0).startsWith("P_")) {
            if (highestScore == 0.0) {
              val category = "AddedNodes"
              // Output the category and node IDs
              output.collect(new Text(category), new Text(nodeIds))
            }
          }
        // Default case for unmatched format
        case _ =>
      }
    }
  }

  // Reducer class for Likelihood Computation
  class LikelihoodComputationReduce extends MapReduceBase with Reducer[Text, Text, Text, Text] {
    override def reduce(label: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      val result = values.asScala.map(_.toString).mkString(", ")
      if (result.nonEmpty) {
        output.collect(label, new Text(result))
      } else {
        logger.warn(s"No values found for key: ${label.toString}")
      }
    }
  }
}
