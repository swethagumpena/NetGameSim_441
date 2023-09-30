package com.lsc

import NetGraphAlgebraDefs.NetModelAlgebra.{logger, outputDirectory}
import NodesSimScore.{NodesSimScoreMap, NodesSimScoreReduce}
import EdgesSimScore.{EdgesSimScoreMap, EdgesSimScoreReduce}
import LikelihoodComputation.{LikelihoodComputationMap, LikelihoodComputationReduce}

import NetGraphAlgebraDefs.NetGraph
import utils.WriteNodePairsToFile.createNodePairsAndWrite
import utils.WriteEdgePairsToFile.createEdgePairsAndWrite
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.*
import org.apache.hadoop.conf.*
import scala.jdk.CollectionConverters._

object MainClass {
  def main(args: Array[String]): Unit = {
    val netGraphOriginal = NetGraph.load(args(0), outputDirectory);
    val netGraphPerturbed = NetGraph.load(args(1), outputDirectory);

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
      val conf2: JobConf = new JobConf(this.getClass)
      conf2.setJobName("FindLikelihoodOfChange")
      conf2.set("mapred.textoutputformat.separator", ":")
      conf2.set("mapreduce.job.maps", "1")
      conf2.set("mapreduce.job.reduces", "1")
      conf2.setOutputKeyClass(classOf[Text])
      conf2.setOutputValueClass(classOf[Text])
      conf2.setMapperClass(classOf[LikelihoodComputationMap])
      conf2.setReducerClass(classOf[LikelihoodComputationReduce])
      conf2.setInputFormat(classOf[TextInputFormat])
      conf2.setOutputFormat(classOf[TextOutputFormat[Text, Text]])
      FileInputFormat.addInputPath(conf2, new Path(s"${args(2)}${funcConfig.getString("NodesSimScore")}/part-00000"))
      FileInputFormat.addInputPath(conf2, new Path(s"${args(2)}${funcConfig.getString("EdgesSimScore")}/part-00000"))
      FileOutputFormat.setOutputPath(conf2, new Path(s"${args(2)}${funcConfig.getString("LikelihoodComputation")}"))
      val runningJob = JobClient.runJob(conf2)

      if (runningJob.isSuccessful) {
        logger.info("Likelihood Computation job completed successfully")
        true
      } else {
        logger.error("Likelihood Computation job failed")
        false
      }
    }

    logger.info(s"Running the jobs")
    val nodesSimScoreJobSuccess = runNodesSimScoreMapReduceJob(args)
    val edgesSimScoreJobSuccess = runEdgesSimScoreMapReduceJob(args)
    if (nodesSimScoreJobSuccess && edgesSimScoreJobSuccess) {
      val likelihoodComputationJobSuccess = runLikelihoodMapReduceJob(args)
    }
  }
}
