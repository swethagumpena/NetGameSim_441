package com.lsc

import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import NodesSimScore.{NodesSimScoreMap, NodesSimScoreReduce}
import EdgesSimScore.{EdgesSimScoreMap, EdgesSimScoreReduce}
import LikelihoodComputation.{LikelihoodComputationMap, LikelihoodComputationReduce}
import AlgoPerformance.{AlgoPerformanceMap, AlgoPerformanceReduce}

import NetGraphAlgebraDefs.NetGraph
import utils.WriteNodePairsToFile.createNodePairsAndWrite
import utils.WriteEdgePairsToFile.createEdgePairsAndWrite
import utils.ParseYaml.parseFile
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.*
import org.apache.hadoop.conf.*
import scala.jdk.CollectionConverters._

object MainClass {
  def main(args: Array[String]): Unit = {
    val netGraphOriginal = NetGraph.load(args(0), outputDirectory)
    val netGraphPerturbed = NetGraph.load(args(1), outputDirectory)

    val result: Map[String, List[String]] = parseFile("/Users/swethagumpena/UIC/441_Cloud_computing/Project1/NetGameSim/outputs/NetGameSimNetGraph_23-09-23-10-49-19.ngs.yaml")
    println(result)

    val config: Config = ConfigFactory.load("application.conf")
    val preprocessingConfig = config.getConfig("graphComparison.preprocessing.outputPath")
    val funcConfig = config.getConfig("graphComparison.functionalityconfigs.outputPath")

    if netGraphOriginal.isEmpty || netGraphPerturbed.isEmpty then
       logger.warn("Input is not of the right format")

    val headOriginal = netGraphOriginal.head.sm
    val headPerturbed = netGraphPerturbed.head.sm

    createNodePairsAndWrite(headOriginal, headPerturbed, s"${args(2)}${preprocessingConfig.getString("NodePairs")}")
    createEdgePairsAndWrite(headOriginal, headPerturbed, s"${args(2)}${preprocessingConfig.getString("EdgePairs")}")

    logger.info(s"Intermediate nodes pre-processed file created at ${preprocessingConfig.getString("NodePairs")} and edges pre-processed file created at ${preprocessingConfig.getString("EdgePairs")}")

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
      FileInputFormat.setInputPaths(conf, new Path(s"${args(2)}${preprocessingConfig.getString("NodePairs")}"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(2)}${funcConfig.getString("NodesSimScore")}"))
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
      FileInputFormat.setInputPaths(conf, new Path(s"${args(2)}${preprocessingConfig.getString("EdgePairs")}"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(2)}${funcConfig.getString("EdgesSimScore")}"))
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
      FileInputFormat.addInputPath(conf, new Path(s"${args(2)}${funcConfig.getString("NodesSimScore")}/part-00000"))
      FileInputFormat.addInputPath(conf, new Path(s"${args(2)}${funcConfig.getString("EdgesSimScore")}/part-00000"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(2)}${funcConfig.getString("LikelihoodComputation")}"))
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
      conf.set("mapreduce.job.maps", "1")
      conf.set("mapreduce.job.reduces", "1")
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[Text])
      conf.setMapperClass(classOf[AlgoPerformanceMap])
      conf.setReducerClass(classOf[AlgoPerformanceReduce])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
      FileInputFormat.addInputPath(conf, new Path(s"${args(2)}${funcConfig.getString("LikelihoodComputation")}"))
      FileOutputFormat.setOutputPath(conf, new Path(s"${args(2)}${funcConfig.getString("AlgorithmPerformance")}"))
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
    val nodesSimScoreJobSuccess = runNodesSimScoreMapReduceJob(args)
    val edgesSimScoreJobSuccess = runEdgesSimScoreMapReduceJob(args)
    if (nodesSimScoreJobSuccess && edgesSimScoreJobSuccess) {
      val likelihoodComputationJobSuccess = runLikelihoodMapReduceJob(args)
      if (likelihoodComputationJobSuccess) {
        runAlgoPerformanceMapReduceJob(args)
      }
    }
  }
}
