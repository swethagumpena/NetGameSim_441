package com.lsc
package utils

import NetGraphAlgebraDefs.{NetModel, NetStateMachine, NodeObject}
import software.amazon.awssdk.core.sync.RequestBody
import NetGraphAlgebraDefs.NetModelAlgebra.logger

import java.io.PrintWriter
import scala.jdk.CollectionConverters.*
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{PutObjectRequest, S3Exception}
import software.amazon.awssdk.regions.Region

object WriteNodePairsToFile {
  def createNodePairsAndWrite(ogSm: NetStateMachine, pbSm: NetStateMachine, filePath: String): Unit = {
    if (filePath.startsWith("s3://")) {
      writeToS3(ogSm, pbSm, filePath)
    } else {
      writeToLocal(ogSm, pbSm, filePath)
    }
  }

  private def writeToS3(ogSm: NetStateMachine, pbSm: NetStateMachine, s3Path: String): Unit = {
    val s3Client = S3Client.builder().region(Region.US_EAST_1).build()

    try {
      val originalGraphNodes = ogSm.nodes().asScala.toArray
      val perturbedGraphNodes = pbSm.nodes().asScala.toArray

      val content = for {
        i <- originalGraphNodes.indices by 2
        j <- perturbedGraphNodes.indices by 2
      } yield {
        val ogPair = if (originalGraphNodes.lift(i + 1).isDefined)
          s"${originalGraphNodes(i)}, ${originalGraphNodes(i + 1)}"
        else
          s"${originalGraphNodes(i)}"
        val pbPair = if (j + 1 < perturbedGraphNodes.length)
          s"${perturbedGraphNodes(j)}, ${perturbedGraphNodes(j + 1)}"
        else
          s"${perturbedGraphNodes(j)}"

        s"$ogPair x $pbPair"
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
      logger.info("successfully wrote")
    } catch {
      case e: S3Exception => e.printStackTrace()
    } finally {
      s3Client.close()
    }
  }

  private def writeToLocal(ogSm: NetStateMachine, pbSm: NetStateMachine, localPath: String): Unit = {
    val writer = new PrintWriter(localPath)
    try {
      val originalGraphNodes = ogSm.nodes().asScala.toArray
      val perturbedGraphNodes = pbSm.nodes().asScala.toArray

      for {
        i <- originalGraphNodes.indices by 2
        j <- perturbedGraphNodes.indices by 2
      } {
        val ogPair = if (originalGraphNodes.lift(i + 1).isDefined)
          s"${originalGraphNodes(i)}, ${originalGraphNodes(i + 1)}"
        else
          s"${originalGraphNodes(i)}"
        val pbPair = if (j + 1 < perturbedGraphNodes.length)
          s"${perturbedGraphNodes(j)}, ${perturbedGraphNodes(j + 1)}"
        else
          s"${perturbedGraphNodes(j)}"

        writer.println(s"$ogPair x $pbPair")
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
