package com.lsc

import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import NetGraphAlgebraDefs.{NetGraph, NetModel, NetModelAlgebra}
import utils.SimilarityScoreCalculation.jaccardSimilarity
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

object EdgesSimScore extends App {
  // Mapper class for Edges Similarity Score Calculation
  class EdgesSimScoreMap extends MapReduceBase with Mapper[Object, Text, Text, Text] {

    // Defining a case class for representing an EdgeObject
    case class EdgeObject(
        sourceId: Int, sourceChildren: Int, sourceProps: Int, sourceCurrentDepth: Int,
        sourcePropValueRange: Int, sourceMaxDepth: Int, sourceMaxBranchingFactor: Int,
        sourceMaxProperties: Int, sourceStoredValue: Double, destId: Int, destChildren: Int,
        destProps: Int, destCurrentDepth: Int, destPropValueRange: Int, destMaxDepth: Int,
        destMaxBranchingFactor: Int, destMaxProperties: Int, destStoredValue: Double,
        actionType: Int, cost: Double) {
      def toSet: Set[Int] =
        Set(sourceChildren, sourceProps, sourceCurrentDepth, sourcePropValueRange, sourceMaxDepth,
            sourceMaxBranchingFactor, sourceMaxProperties, sourceStoredValue.round.toInt,
            destChildren, destProps, destCurrentDepth, destPropValueRange, destMaxDepth,
            destMaxBranchingFactor, destMaxProperties, destStoredValue.round.toInt, actionType,
            cost.round.toInt)
    }

    // Parse an EdgeObject from its string representation
    def parseEdgeObject(nodeString: String): EdgeObject = {
      val pattern =
        """\(NodeObject\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),([\d.]+)\)-NodeObject\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),([\d.]+)\)-(\d+)-([\d.]+)\)""".r
      nodeString match {
        case pattern(sourceId, sourceChildren, sourceProps, sourceCurrentDepth,
                     sourcePropValueRange, sourceMaxDepth, sourceMaxBranchingFactor,
                     sourceMaxProperties, sourceStoredValue, destId, destChildren, destProps,
                     destCurrentDepth, destPropValueRange, destMaxDepth, destMaxBranchingFactor,
                     destMaxProperties, destStoredValue, actionType, cost) =>
          EdgeObject(
            sourceId.toInt,
            sourceChildren.toInt,
            sourceProps.toInt,
            sourceCurrentDepth.toInt,
            sourcePropValueRange.toInt,
            sourceMaxDepth.toInt,
            sourceMaxBranchingFactor.toInt,
            sourceMaxProperties.toInt,
            sourceStoredValue.toDouble,
            destId.toInt,
            destChildren.toInt,
            destProps.toInt,
            destCurrentDepth.toInt,
            destPropValueRange.toInt,
            destMaxDepth.toInt,
            destMaxBranchingFactor.toInt,
            destMaxProperties.toInt,
            destStoredValue.toDouble,
            actionType.toInt,
            cost.toDouble
          )
        case _ =>
          logger.trace("Invalid EdgeObject string format")
          throw new IllegalArgumentException("Invalid EdgeObject string format")
      }
    }

    // Map function
    @throws[IOException]
    def map(key: Object, value: Text, output: OutputCollector[Text, Text],
            reporter: Reporter): Unit = {
      val line: String = value.toString
      val parts = line.split("x")

      // Splitting into original and perturbed parts
      val originalPart: Array[String] =
        if (parts(0).trim.nonEmpty) parts(0).trim.split(" \\| ") else Array[String]()
      val perturbedPart: Array[String] =
        if (parts(1).trim.nonEmpty) parts(1).trim.split(" \\| ") else Array[String]()

      // Parsing EdgeObjects from the parts
      val parsedOriginalPart: Array[EdgeObject] = originalPart.map(parseEdgeObject)
      val parsedPerturbedPart: Array[EdgeObject] = perturbedPart.map(parseEdgeObject)

      // Calculate Jaccard similarity scores
      val similarities = parsedOriginalPart.flatMap { original =>
        parsedPerturbedPart.map { perturbed =>
          val originalEdge = new Text(s"O_${original.sourceId}-${original.destId}")
          val perturbedEdge = new Text(s"P_${perturbed.sourceId}-${perturbed.destId}")
          val similarityScore = jaccardSimilarity(original.toSet, perturbed.toSet)
          (originalEdge, perturbedEdge,
            new Text(
              s"(${original.sourceId}-${original.destId} | ${perturbed.sourceId}-${perturbed.destId})=$similarityScore"))
        }
      }

      // Emit similarity scores
      // one grouping for original edges, other grouping for perturbed edges
      similarities.foreach { case (originalKey, perturbedKey, value) =>
        output.collect(originalKey, value)
        output.collect(perturbedKey, value)
      }
    }
  }

  // Reducer class for Edges Similarity Score Calculation
  class EdgesSimScoreReduce extends MapReduceBase with Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text],
                        reporter: Reporter): Unit = {
      // Combine values for the same key
      val result = values.asScala.map(_.toString).mkString(", ")
      // Emit the combined result
      if (result.nonEmpty) {
        output.collect(key, new Text(result))
      } else {
        logger.warn(s"No values found for key: ${key.toString}")
      }
    }
  }
}
