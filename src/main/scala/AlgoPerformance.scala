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

    @throws[IOException]
    override def configure(job: JobConf): Unit = {
      val filePath = job.get("NGS_yaml_file")
      yamlData = parseFile(filePath)
    }

    @throws[IOException]
    def map(key: Object, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      val one = new IntWritable(1)

      def processUnchangedValues(key: String, strValue: String, dataMap: Map[String, List[String]], output: OutputCollector[Text, IntWritable]): Unit = {
        val valuesToCheck = strValue.split(",").map(_.trim)

        val (inAdded, inModified, inRemoved) = key match {
          case "UnchangedNodes" =>
            (
              valuesToCheck.exists(dataMap("AddedNodes").contains),
              valuesToCheck.exists(dataMap("ModifiedNodes").contains),
              valuesToCheck.exists(dataMap("RemovedNodes").contains)
            )
          case "UnchangedEdges" =>
            (
              valuesToCheck.exists(dataMap("AddedEdges").contains),
              valuesToCheck.exists(dataMap("ModifiedEdges").contains),
              valuesToCheck.exists(dataMap("RemovedEdges").contains)
            )
          case _ => (false, false, false)
        }

        if (!(inAdded || inModified || inRemoved)) {
          valuesToCheck.foreach { valueToCheck =>
            output.collect(new Text("ATL"), one)
          }
        }
      }

      val line: String = value.toString
      val Array(key, strValue) = line.split(":")

      if (yamlData.contains(key)) {
        val yamlDataValue = yamlData(key)

        val valuesToCheck = strValue.split(",").map(_.trim)

        valuesToCheck.foreach { valueToCheck =>
          if (yamlDataValue.contains(valueToCheck)) {
            if (key == "AddedNodes" || key == "RemovedNodes" || key == "AddedEdges" || key == "RemovedEdges") {
              output.collect(new Text("DTL"), one)
            } else {
              output.collect(new Text("ATL"), one)
            }
          } else {
            output.collect(new Text("WTL"), one)
          }
        }

        val strValueList = strValue.split(",").map(_.trim)
        yamlDataValue.foreach { valueToCheck =>
          if (!strValueList.contains(valueToCheck)) {
            output.collect(new Text("CTL"), one)
          }
        }
      } else if (key == "UnchangedNodes" || key == "UnchangedEdges") {
        processUnchangedValues(key, strValue, yamlData, output)
      } else {
        logger.info(s"Key $key not found in compareWith")
      }
    }
  }

  class AlgoPerformanceReduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] {
    @throws[IOException]
    override def reduce(key: Text, values: java.util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      val sum = Iterator(values.asScala).flatten.map(_.get()).sum
      output.collect(key, new IntWritable(sum))
    }
  }
}
