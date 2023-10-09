package com.lsc
package utils

import com.google.common.graph.EndpointPair
import java.io.PrintWriter
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.s3.model.S3Exception
import software.amazon.awssdk.services.s3.S3Client
import NetGraphAlgebraDefs.NetModel
import NetGraphAlgebraDefs.NetModelAlgebra.logger
import NetGraphAlgebraDefs.NetStateMachine
import NetGraphAlgebraDefs.NodeObject

object WriteEdgePairsToFile {

  // Returns a formatted string with information about an edge: fromNodeData-toNodeData-action-cost
  def getEdgeInfo(edge: EndpointPair[NodeObject], sm: NetStateMachine): String = {
    val ithNode = edge.nodeU()
    val jthNode = edge.nodeV()

    val cost = sm.edgeValue(ithNode, jthNode).get.cost.toFloat
    val actionType = sm.edgeValue(ithNode, jthNode).get.actionType
    s"($ithNode-$jthNode-$actionType-$cost)"
  }

  /** Creates edge pairs from two NetStateMachines and writes them to a file, either locally or on
    * S3.
    *
    * @param ogSm
    *   The original NetStateMachine
    * @param pbSm
    *   The perturbed NetStateMachine
    * @param filePath
    *   The path where the file will be written (local path or S3 path starting with "s3://")
    */
  def createEdgePairsAndWrite(ogSm: NetStateMachine, pbSm: NetStateMachine,
                              filePath: String): Unit = {
    if (filePath.startsWith("s3://")) {
      writeToS3(ogSm, pbSm, filePath)
    } else {
      writeToLocal(ogSm, pbSm, filePath)
    }
  }

  /** Writes edge pairs to an S3 bucket.
    *
    * @param ogSm
    *   The original NetStateMachine
    * @param pbSm
    *   The perturbed NetStateMachine
    * @param s3Path
    *   The S3 path where the file will be written
    */
  private def writeToS3(ogSm: NetStateMachine, pbSm: NetStateMachine, s3Path: String): Unit = {
    val s3Client = S3Client.builder().region(Region.US_EAST_1).build()
    logger.info("in write to s3 edges")

    try {
      // Retrieve edges as arrays
      val originalGraphEdges = ogSm.edges().asScala.toArray
      val perturbedGraphEdges = pbSm.edges().asScala.toArray

      // If there are two elements concatenate the first and second element using a ',' else we only consider the one element
      val content = originalGraphEdges.indices.by(2).flatMap { i =>
        val ogEdgePair = if (originalGraphEdges.lift(i + 1).isDefined)
          s"${getEdgeInfo(originalGraphEdges(i), ogSm)} | ${getEdgeInfo(originalGraphEdges(i + 1), ogSm)}"
        else
          s"${getEdgeInfo(originalGraphEdges(i), ogSm)}"

        perturbedGraphEdges.indices.by(2).map { j =>
          val pbEdgePair = if (j + 1 < perturbedGraphEdges.length)
            s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)} | ${getEdgeInfo(perturbedGraphEdges(j + 1), pbSm)}"
          else
            s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)}"
          // Write to the intermediate file with ' x ' as the delimiter
          s"$ogEdgePair x $pbEdgePair"
        }
      }

      val contentString = content.mkString("\n")

      val request = PutObjectRequest
        .builder()
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
    } finally s3Client.close()
  }

  /** Writes edge pairs to a local file.
    *
    * @param ogSm
    *   The original NetStateMachine
    * @param pbSm
    *   The perturbed NetStateMachine
    * @param localPath
    *   The local path where the file will be written
    */
  private def writeToLocal(ogSm: NetStateMachine, pbSm: NetStateMachine,
                           localPath: String): Unit = {
    val writer = new PrintWriter(localPath)
    try {
      val originalGraphEdges = ogSm.edges().asScala.toArray
      val perturbedGraphEdges = pbSm.edges().asScala.toArray

      originalGraphEdges.indices.by(2).foreach { i =>
         val ogEdgePair = if (originalGraphEdges.lift(i + 1).isDefined)
           s"${getEdgeInfo(originalGraphEdges(i), ogSm)} | ${getEdgeInfo(originalGraphEdges(i + 1), ogSm)}"
         else
           s"${getEdgeInfo(originalGraphEdges(i), ogSm)}"

         perturbedGraphEdges.indices.by(2).foreach { j =>
         val pbEdgePair = if (j + 1 < perturbedGraphEdges.length)
           s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)} | ${getEdgeInfo(perturbedGraphEdges(j + 1), pbSm)}"
         else
           s"${getEdgeInfo(perturbedGraphEdges(j), pbSm)}"
         writer.println(s"$ogEdgePair x $pbEdgePair")
        }
      }
    } finally writer.close()
  }

  /** Extracts the bucket name from an S3 path.
    *
    * @param s3Path
    *   The S3 path
    * @return
    *   The bucket name
    */
  private def getBucketName(s3Path: String): String =
    s3Path.drop(5).takeWhile(_ != '/')

  /** Extracts the key from an S3 path.
    *
    * @param s3Path
    *   The S3 path
    * @return
    *   The key
    */
  private def getKey(s3Path: String): String =
    s3Path.drop(5 + getBucketName(s3Path).length + 1)
}
