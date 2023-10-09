package com.lsc

import utils.SimilarityScoreCalculation.jaccardSimilarity

import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import NetGraphAlgebraDefs.{NetGraph, NetModel, NetModelAlgebra}
import utils.ParseYaml.parseFile
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.mapred.*
import org.apache.hadoop.util.*

import java.io.IOException
import java.util
import scala.jdk.CollectionConverters.*

object AlgoPerformance extends App {
  class AlgoPerformanceMap extends MapReduceBase with Mapper[Object, Text, Text, IntWritable] {
    private var yamlData: Map[String, List[String]] = _

    // Configure method to initialize YAML data
    @throws[IOException]
    override def configure(job: JobConf): Unit = {
      // Retrieve YAML file path that is got from the arguments
      val filePath = job.get("NGS_yaml_file")
      yamlData = parseFile(filePath)
    }
    // creating a variable one to use with the mapper
    val one = new IntWritable(1)

    // Helper method to process unchanged values
    def processUnchangedValues(key: String, strValue: String, dataMap: Map[String, List[String]], output: OutputCollector[Text, IntWritable]): Unit = {
      val valuesToCheck = strValue.split(",").map(_.trim)

      val (inAdded, inModified, inRemoved) = key match {
        // In the case of unchangedNodes, the value should not be present in Added/Modified/Removed Nodes of the Golden Set YAML
        case "UnchangedNodes" =>
          (
            valuesToCheck.exists(dataMap("AddedNodes").contains),
            valuesToCheck.exists(dataMap("ModifiedNodes").contains),
            valuesToCheck.exists(dataMap("RemovedNodes").contains)
          )
        // In the case of unchangedEdges, the value should not be present in Added/Modified/Removed Edges of the Golden Set YAML
        case "UnchangedEdges" =>
          (
            valuesToCheck.exists(dataMap("AddedEdges").contains),
            valuesToCheck.exists(dataMap("ModifiedEdges").contains),
            valuesToCheck.exists(dataMap("RemovedEdges").contains)
          )
        case _ => (false, false, false)
      }

      // Correctly predicted unchanged Nodes/Edges are ATL (Correctly accepted Traceability Link)
      if (!(inAdded || inModified || inRemoved)) {
        valuesToCheck.foreach { valueToCheck =>
          output.collect(new Text("ATL"), one)
        }
      }
    }

    @throws[IOException]
    def map(key: Object, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      val line: String = value.toString
      val Array(key, strValue) = line.split(":")

      // Check if YAML data contains the key
      if (yamlData.contains(key)) {
        val yamlDataValue = yamlData(key)

        val valuesToCheck = strValue.split(",").map(_.trim)

        valuesToCheck.foreach { valueToCheck =>
          if (yamlDataValue.contains(valueToCheck)) {
            if (key == "AddedNodes" || key == "RemovedNodes" || key == "AddedEdges" || key == "RemovedEdges") {
              // correctly predicted Added/Removed Nodes/Edges are DTLs, as they don't have a TL with the original
              output.collect(new Text("DTL"), one)
            } else {
              // correctly predicted Modified/Unchanged Nodes/Edges are ATLs, as they have a traceability link with the original
              output.collect(new Text("ATL"), one)
            }
          } else {
            // incorrectly predicted Added/Modified/Removed Nodes/Edges are WTLs (wrong traceability link that our algo accepts)
            output.collect(new Text("WTL"), one)
          }
        }

        val strValueList = strValue.split(",").map(_.trim)
        yamlDataValue.foreach { valueToCheck =>
          if (!strValueList.contains(valueToCheck)) {
            // if value in golden set is not present in our prediction, it is a CTL (correct TLs mistakenly discarded by our algo)
            output.collect(new Text("CTL"), one)
          }
        }
      } else if (key == "UnchangedNodes" || key == "UnchangedEdges") {
        processUnchangedValues(key, strValue, yamlData, output)
      } else {
        logger.warn(s"Key $key not found in compareWith")
      }
    }
  }

  class AlgoPerformanceReduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    @throws[IOException]
    override def reduce(key: Text, values: java.util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      // compute the sum of values based on key
      val sum = Iterator(values.asScala).flatten.map(_.get()).sum
      output.collect(key, new IntWritable(sum))
    }
  }
}
