package com.lsc

import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*
import NetGraphAlgebraDefs.{NetGraph, NetModel, NetModelAlgebra}
import utils.SimilarityScoreCalculation.jaccardSimilarity

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

object NodesSimScore extends App {
  class NodesSimScoreMap extends MapReduceBase with Mapper[Object, Text, Text, Text] {
    case class NodeObject(id: Int, children: Int, props: Int, currentDepth: Int,
                          propValueRange: Int, maxDepth: Int, maxBranchingFactor: Int,
                          maxProperties: Int, storedValue: Double) {
      def toSet: Set[Int] = Set(children, props, currentDepth, propValueRange, maxDepth,
                                maxBranchingFactor, maxProperties, storedValue.round.toInt)
    }

    def parseNodeObject(nodeString: String): NodeObject = {
      val pattern = """NodeObject\((\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+),(\d+\.\d+)\)""".r
      nodeString match {
        case pattern(id, children, props, currentDepth, propValueRange, maxDepth,
                     maxBranchingFactor, maxProperties, storedValue) =>
          NodeObject(
            id.toInt,
            children.toInt,
            props.toInt,
            currentDepth.toInt,
            propValueRange.toInt,
            maxDepth.toInt,
            maxBranchingFactor.toInt,
            maxProperties.toInt,
            storedValue.toDouble
          )
        case _ => throw new IllegalArgumentException("Invalid NodeObject string format")
      }
    }

    @throws[IOException]
    def map(key: Object, value: Text, output: OutputCollector[Text, Text],
            reporter: Reporter): Unit = {
      val line: String = value.toString
      val parts = line.split("x")

      val originalPart: Array[String] =
        if (parts(0).trim.nonEmpty) parts(0).trim.split(", ") else Array[String]()
      val perturbedPart: Array[String] =
        if (parts(1).trim.nonEmpty) parts(1).trim.split(", ") else Array[String]()

      val parsedOriginalPart: Array[NodeObject] = originalPart.map(parseNodeObject)

      val parsedPerturbedPart: Array[NodeObject] = perturbedPart.map(parseNodeObject)

      val similarities = parsedOriginalPart.flatMap { original =>
        parsedPerturbedPart.map { perturbed =>
          val originalId = new Text(s"O_${original.id}") // using a prefix to record it as original ID
          val perturbedId = new Text(s"P_${perturbed.id}") // using a prefix to record it as perturbed ID
          val similarityScore = jaccardSimilarity(original.toSet, perturbed.toSet)
          (originalId, perturbedId, new Text(s"(${original.id} | ${perturbed.id})=$similarityScore"))
        }
      }

      similarities.foreach { case (originalKey, perturbedKey, value) =>
        output.collect(originalKey, value)
        output.collect(perturbedKey, value)
      }
    }
  }
  class NodesSimScoreReduce extends MapReduceBase with Reducer[Text, Text, Text, Text] {
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      val result = values.asScala.map(_.toString).mkString(", ")
      if (result.nonEmpty) {
        output.collect(key, new Text(result))
      }
    }
  }
}
