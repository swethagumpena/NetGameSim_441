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
  /**
   * Creates node pairs from two NetStateMachines and writes them to a file, either locally or on S3.
   *
   * @param ogSm     The original NetStateMachine
   * @param pbSm     The perturbed NetStateMachine
   * @param filePath The path where the file will be written (local path or S3 path starting with "s3://")
   */
  def createNodePairsAndWrite(ogSm: NetStateMachine, pbSm: NetStateMachine, filePath: String): Unit = {
    if (filePath.startsWith("s3://")) {
      writeToS3(ogSm, pbSm, filePath)
    } else {
      writeToLocal(ogSm, pbSm, filePath)
    }
  }

  /**
   * Writes node pairs to an S3 bucket.
   *
   * @param ogSm   The original NetStateMachine
   * @param pbSm   The perturbed NetStateMachine
   * @param s3Path The S3 path where the file will be written
   */
  private def writeToS3(ogSm: NetStateMachine, pbSm: NetStateMachine, s3Path: String): Unit = {
    val s3Client = S3Client.builder().region(Region.US_EAST_1).build()

    try {
      // Retrieve nodes as arrays
      val originalGraphNodes = ogSm.nodes().asScala.toArray
      val perturbedGraphNodes = pbSm.nodes().asScala.toArray

      // If there are two elements concatenate the first and second element using a ',' else we only consider the one element
      val content = originalGraphNodes.indices.by(2).flatMap { i =>
        val ogElement1 = originalGraphNodes.lift(i).map(_.toString).getOrElse("")
        val ogElement2 = originalGraphNodes.lift(i + 1).map(_.toString).getOrElse("")
        val ogPair = if (ogElement2.nonEmpty) {
          s"${ogElement1}, ${ogElement2}"
        } else {
          s"${ogElement1}"
        }

        perturbedGraphNodes.indices.by(2).map { j =>
          val pbElement1 = perturbedGraphNodes.lift(j).map(_.toString).getOrElse("")
          val pbElement2 = perturbedGraphNodes.lift(j + 1).map(_.toString).getOrElse("")
          val pbPair = if (pbElement2.nonEmpty) {
            s"${pbElement1}, ${pbElement2}"
          } else {
            s"${pbElement1}"
          }
          // Write to the intermediate file with ' x ' as the delimiter
          s"$ogPair x $pbPair"
        }
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

  /**
   * Writes node pairs to a local file.
   *
   * @param ogSm      The original NetStateMachine
   * @param pbSm      The perturbed NetStateMachine
   * @param localPath The local path where the file will be written
   */
  private def writeToLocal(ogSm: NetStateMachine, pbSm: NetStateMachine, localPath: String): Unit = {
    val writer = new PrintWriter(localPath)
    try {
      val originalGraphNodes = ogSm.nodes().asScala.toArray
      val perturbedGraphNodes = pbSm.nodes().asScala.toArray

      originalGraphNodes.indices.by(2).foreach { i =>
        val ogElement1 = originalGraphNodes.lift(i).map(_.toString).getOrElse("")
        val ogElement2 = originalGraphNodes.lift(i + 1).map(_.toString).getOrElse("")
        val ogPair = if (ogElement2.nonEmpty) {
          s"${ogElement1}, ${ogElement2}"
        } else {
          s"${ogElement1}"
        }

        perturbedGraphNodes.indices.by(2).foreach { j =>
          val pbElement1 = perturbedGraphNodes.lift(j).map(_.toString).getOrElse("")
          val pbElement2 = perturbedGraphNodes.lift(j + 1).map(_.toString).getOrElse("")
          val pbPair = if (pbElement2.nonEmpty) {
            s"${pbElement1}, ${pbElement2}"
          } else {
            s"${pbElement1}"
          }
          writer.println(s"$ogPair x $pbPair")
        }
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Extracts the bucket name from an S3 path.
   *
   * @param s3Path The S3 path
   * @return The bucket name
   */
  private def getBucketName(s3Path: String): String = {
    s3Path.drop(5).takeWhile(_ != '/')
  }

  /**
   * Extracts the key from an S3 path.
   *
   * @param s3Path The S3 path
   * @return The key
   */
  private def getKey(s3Path: String): String = {
    s3Path.drop(5 + getBucketName(s3Path).length + 1)
  }
}
