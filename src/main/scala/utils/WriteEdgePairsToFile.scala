package com.lsc
package utils

import NetGraphAlgebraDefs.{NetModel, NetStateMachine, NodeObject}
import com.google.common.graph.EndpointPair

import java.io.PrintWriter
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, S3Exception}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.core.sync.RequestBody
import NetGraphAlgebraDefs.NetModelAlgebra.logger

object WriteEdgePairsToFile {

  def getEdgeInfo(edge: EndpointPair[NodeObject], sm: NetStateMachine): String = {
    val ithNode = edge.nodeU()
    val jthNode = edge.nodeV()

    val cost = sm.edgeValue(ithNode, jthNode).get.cost.toFloat
    val actionType = sm.edgeValue(ithNode, jthNode).get.actionType
    s"($ithNode-$jthNode-$actionType-$cost)"
  }

  def createEdgePairsAndWrite(ogSm: NetStateMachine, pbSm: NetStateMachine, filePath: String): Unit = {
    if (filePath.startsWith("s3://")) {
      writeToS3(ogSm, pbSm, filePath)
    } else {
      writeToLocal(ogSm, pbSm, filePath)
    }
  }

  private def writeToS3(ogSm: NetStateMachine, pbSm: NetStateMachine, s3Path: String): Unit = {
    val s3Client = S3Client.builder().region(Region.US_EAST_1).build()
    logger.info("in write to s3 edges")

    try {
      val originalGraphEdges = ogSm.edges().asScala.toArray
      val perturbedGraphEdges = pbSm.edges().asScala.toArray

      val content = for {
        i <- originalGraphEdges.indices by 2
        j <- perturbedGraphEdges.indices by 2
      } yield {
        val ogEdgePair =
          if (originalGraphEdges.lift(i + 1).isDefined)
            s"${getEdgeInfo(originalGraphEdges(i), ogSm)} | ${getEdgeInfo(originalGraphEdges(i + 1), ogSm)}"
          else
            s"${getEdgeInfo(originalGraphEdges(i), ogSm)}"
        val pbEdgePair = if (j + 1 < perturbedGraphEdges.length)
          s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)} | ${getEdgeInfo(perturbedGraphEdges(j + 1), pbSm)}"
        else
          s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)}"

        s"$ogEdgePair x $pbEdgePair"
      }

      val contentString = content.mkString("\n")

      val request = PutObjectRequest.builder()
        .bucket(getBucketName(s3Path))
        .key(getKey(s3Path))
        .contentType("text/plain")
        .build()

      val inputStream = new java.io.ByteArrayInputStream(contentString.getBytes("UTF-8"))
      val requestBody = RequestBody.fromInputStream(inputStream, contentString.length())

      s3Client.putObject(request, requestBody)
      logger.info("successfully wrote edges")
    } catch {
      case e: S3Exception => e.printStackTrace()
    } finally {
      s3Client.close()
    }
  }

  private def writeToLocal(ogSm: NetStateMachine, pbSm: NetStateMachine, localPath: String): Unit = {
    val writer = new PrintWriter(localPath)
    try {
      val originalGraphEdges = ogSm.edges().asScala.toArray
      val perturbedGraphEdges = pbSm.edges().asScala.toArray

      for {
        i <- originalGraphEdges.indices by 2
        j <- perturbedGraphEdges.indices by 2
      } {
        val ogEdgePair =
          if (originalGraphEdges.lift(i + 1).isDefined)
            s"${getEdgeInfo(originalGraphEdges(i), ogSm)} | ${getEdgeInfo(originalGraphEdges(i + 1), ogSm)}"
          else
            s"${getEdgeInfo(originalGraphEdges(i), ogSm)}"
        val pbEdgePair = if (j + 1 < perturbedGraphEdges.length)
          s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)} | ${getEdgeInfo(perturbedGraphEdges(j + 1), pbSm)}"
        else
          s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)}"

        writer.println(s"$ogEdgePair x $pbEdgePair")
      }
    } finally {
      writer.close()
    }
  }

  private def getBucketName(s3Path: String): String = {
    s3Path.drop(5).takeWhile(_ != '/')
  }

  private def getKey(s3Path: String): String = {
    s3Path.drop(5 + getBucketName(s3Path).length + 1)
  }
}

