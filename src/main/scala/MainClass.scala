package com.lsc

import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import NodesSimScore.{NodesSimScoreMap, NodesSimScoreReduce}
import EdgesSimScore.{EdgesSimScoreMap, EdgesSimScoreReduce}
import LikelihoodComputation.{LikelihoodComputationMap, LikelihoodComputationReduce}
import AlgoPerformance.{AlgoPerformanceMap, AlgoPerformanceReduce}

import NetGraphAlgebraDefs.NetGraph
import utils.WriteNodePairsToFile.createNodePairsAndWrite
import utils.WriteEdgePairsToFile.createEdgePairsAndWrite
import utils.GoodnessEstimation.calculateGoodness
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.*
import org.apache.hadoop.conf.*
import scala.jdk.CollectionConverters.*
import scala.io.Source

object MainClass {
  def main(args: Array[String]): Unit = {
    val netGraphOriginal = NetGraph.load(args(0))
    val netGraphPerturbed = NetGraph.load(args(1))

    if netGraphOriginal.isEmpty || netGraphPerturbed.isEmpty then
      logger.warn("Input is not of the right format")
    else logger.info("Graphs successfully loaded")

    val config = ConfigFactory.load()
    val pathConfig = config.getConfig("NGSimulator").getConfig("OutputPath")

    val headOriginal = netGraphOriginal.head.sm
    val headPerturbed = netGraphPerturbed.head.sm

    createNodePairsAndWrite(headOriginal, headPerturbed, s"${args(3)}${pathConfig.getString("nodePairs")}")
    createEdgePairsAndWrite(headOriginal, headPerturbed, s"${args(3)}${pathConfig.getString("edgePairs")}")

    def runNodesSimScoreMapReduceJob(args: Array[String]): Boolean = {
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("FindNodesSimilarityScores")
      conf.set("mapred.textoutputformat.separator", ":")
      conf.set("mapreduce.job.maps", "1")
      conf.set("mapreduce.job.reduces", "1")
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[Text])
      conf.setMapperClass(classOf[NodesSimScoreMap])
      conf.setReducerClass(classOf[NodesSimScoreReduce])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
      FileInputFormat.setInputPaths(conf, new Path(s"${args(3)}${pathConfig.getString("nodePairs")}"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(3)}${pathConfig.getString("nodesSimScore")}"))
      val runningJob = JobClient.runJob(conf)

      if (runningJob.isSuccessful) {
        logger.info("Nodes Sim score MapReduce job completed successfully")
        true
      } else {
        logger.error("Nodes Sim score MapReduce job failed")
        false
      }
    }

    def runEdgesSimScoreMapReduceJob(args: Array[String]): Boolean = {
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("FindEdgesSimilarityScores")
      conf.set("mapred.textoutputformat.separator", ":")
      conf.set("mapreduce.job.maps", "1")
      conf.set("mapreduce.job.reduces", "1")
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[Text])
      conf.setMapperClass(classOf[EdgesSimScoreMap])
      conf.setReducerClass(classOf[EdgesSimScoreReduce])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
      FileInputFormat.setInputPaths(conf, new Path(s"${args(3)}${pathConfig.getString("edgePairs")}"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(3)}${pathConfig.getString("edgesSimScore")}"))
      val runningJob = JobClient.runJob(conf)

      if (runningJob.isSuccessful) {
        logger.info("Edges Sim score MapReduce job completed successfully")
        true
      } else {
        logger.error("Edges Sim score MapReduce job failed")
        false
      }
    }

    def runLikelihoodMapReduceJob(args: Array[String]): Boolean = {
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("FindLikelihoodOfChange")
      conf.set("mapred.textoutputformat.separator", ":")
      conf.set("mapreduce.job.maps", "1")
      conf.set("mapreduce.job.reduces", "1")
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[Text])
      conf.setMapperClass(classOf[LikelihoodComputationMap])
      conf.setReducerClass(classOf[LikelihoodComputationReduce])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
      FileInputFormat.addInputPath(conf, new Path(s"${args(3)}${pathConfig.getString("nodesSimScore")}/part-00000"))
      FileInputFormat.addInputPath(conf, new Path(s"${args(3)}${pathConfig.getString("edgesSimScore")}/part-00000"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(3)}${pathConfig.getString("likelihoodComputation")}"))
      val runningJob = JobClient.runJob(conf)

      if (runningJob.isSuccessful) {
        logger.info("Likelihood Computation job completed successfully")
        true
      } else {
        logger.error("Likelihood Computation job failed")
        false
      }
    }

    def runAlgoPerformanceMapReduceJob(args: Array[String]): Boolean = {
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("EstimateAlgorithmPerformance")
      conf.set("mapred.textoutputformat.separator", ":")
      conf.set("NGS_yaml_file", args(2))
      conf.set("mapreduce.job.maps", "1")
      conf.set("mapreduce.job.reduces", "1")
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])
      conf.setMapperClass(classOf[AlgoPerformanceMap])
      conf.setReducerClass(classOf[AlgoPerformanceReduce])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
      FileInputFormat.addInputPath(conf, new Path(s"${args(3)}${pathConfig.getString("likelihoodComputation")}"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(3)}${pathConfig.getString("algorithmPerformance")}"))
      val runningJob = JobClient.runJob(conf)

      if (runningJob.isSuccessful) {
        logger.info("Estimate Algorithm Performance job completed successfully")
        true
      } else {
        logger.error("Estimate Algorithm Performance job failed")
        false
      }
    }

    logger.info(s"Running the jobs")
    if (runNodesSimScoreMapReduceJob(args) &&
      runEdgesSimScoreMapReduceJob(args) &&
      runLikelihoodMapReduceJob(args) &&
      runAlgoPerformanceMapReduceJob(args)) {
      calculateGoodness(s"${args(3)}${pathConfig.getString("algorithmPerformance")}/part-00000", s"${args(3)}${pathConfig.getString("results")}")
    }
  }
}
