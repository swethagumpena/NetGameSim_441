package com.lsc
package utils

import java.io.PrintWriter
import scala.io.Source
import NetGraphAlgebraDefs.NetModelAlgebra.logger
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, PutObjectRequest, S3Exception}
import software.amazon.awssdk.regions.Region

import scala.annotation.tailrec

object GoodnessEstimation {

  /**
   * Reads lines from a local file.
   *
   * @param filePath The path to the local file.
   * @return A List of lines from the file.
   */
  private def readFromLocal(filePath: String): List[String] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines().toList
    } finally {
      source.close()
    }
  }

  /**
   * Reads lines from an S3 object.
   *
   * @param s3Path The S3 path (s3://bucketName/fileName).
   * @return A List of lines from the S3 object.
   */
  private def readFromS3(s3Path: String): List[String] = {
    val s3Client = S3Client.builder().region(Region.US_EAST_1).build()

    try {
      val bucketName = getBucketName(s3Path)
      val key = getKey(s3Path)

      val request = GetObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .build()

      val response = s3Client.getObject(request)
      val inputStream = response

      Source.fromInputStream(inputStream).getLines().toList
    } catch {
      case e: S3Exception =>
        e.printStackTrace()
        List.empty[String]
    } finally {
      s3Client.close()
    }
  }

  /**
   * Reads values from a file and processes them into a tuple.
   *
   * @param filePath The path to the file (local or S3).
   * @return A tuple containing four integers.
   */
  def readValuesFromFile(filePath: String): (Int, Int, Int, Int) = {
    val lines = if (filePath.startsWith("s3://")) {
      readFromS3(filePath)
    } else {
      readFromLocal(filePath)
    }

    @tailrec
    def processLines(lines: List[String], acc: (Int, Int, Int, Int)): (Int, Int, Int, Int) = lines match {
      case Nil => acc
      case line :: rest =>
        val Array(key, value) = line.split(":")
        val count = value.trim.toInt

        val updatedAcc = key match {
          case "ATL" => (count, acc._2, acc._3, acc._4)
          case "DTL" => (acc._1, count, acc._3, acc._4)
          case "CTL" => (acc._1, acc._2, count, acc._4)
          case "WTL" => (acc._1, acc._2, acc._3, count)
          case _ => acc
        }
        processLines(rest, updatedAcc)
    }
    processLines(lines, (0, 0, 0, 0))
  }

  /**
   * Calculates metrics based on input values.
   *
   * @param ATL Number of ATL.
   * @param DTL Number of DTL.
   * @param CTL Number of CTL.
   * @param WTL Number of WTL.
   * @return A tuple containing three Double values.
   */
  def calculateMetrics(ATL: Int, DTL: Int, CTL: Int, WTL: Int): (Double, Double, Double) = {
    val GTL = ATL + DTL
    val BTL = CTL + WTL
    val RTL = GTL + BTL
    val ACC = ATL.toDouble / RTL
    val BTLR = (WTL.toDouble) / RTL
    val VPR = (GTL - BTL).toDouble / (2 * RTL) + 0.5
    (ACC, BTLR, VPR)
  }

  /**
   * Writes content to an S3 object.
   *
   * @param filePath The S3 path (s3://bucketName/fileName).
   * @param content  The content to write.
   */
  private def writeToS3(filePath: String, content: String): Unit = {
    val s3Client = S3Client.builder().region(Region.US_EAST_1).build()

    try {
      val inputStream = new java.io.ByteArrayInputStream(content.getBytes("UTF-8"))
      val requestBody = RequestBody.fromInputStream(inputStream, content.length())

      val bucketName = getBucketName(filePath)
      val key = getKey(filePath)

      val request = PutObjectRequest.builder()
        .bucket(bucketName)
        .key(key)
        .contentType("text/plain")
        .build()

      s3Client.putObject(request, requestBody)
    } catch {
      case e: S3Exception => e.printStackTrace()
    } finally {
      s3Client.close()
    }
  }

  /**
   * Writes content to a local file.
   *
   * @param filePath The path to the local file.
   * @param content  The content to write.
   */
  private def writeToLocal(filePath: String, content: String): Unit = {
    val writer = new PrintWriter(filePath)
    writer.println(content)
    writer.close()
  }

  /**
   * Writes content to an S3 object or local file.
   *
   * @param filePath The path (S3 or local) to write to.
   * @param content  The content to write.
   */
  def writeOutput(filePath: String, content: String): Unit = {
    if (filePath.startsWith("s3://")) {
      writeToS3(filePath, content)
    } else {
      writeToLocal(filePath, content)
    }
  }

  /**
   * Calculates the goodness of the algorithm based on provided input values.
   *
   * @param inputPath  The path to the input file.
   * @param outputPath The path to the output file.
   */
  def calculateGoodness(inputPath: String, outputPath: String): Unit = {
    logger.info("Calculating the Goodness of the algorithm")
    val (atl, dtl, ctl, wtl) = readValuesFromFile(inputPath)
    val (acc, btlr, vpr) = calculateMetrics(atl, dtl, ctl, wtl)
    val outputContent = s"ACC: $acc\nBTLR: $btlr\nVPR: $vpr"
    writeOutput(outputPath, outputContent)
    logger.info("Successfully calculated the performance of the algorithm in terms of ACC, BTLR and VPR")
  }

   def getBucketName(s3Path: String): String = {
    s3Path.drop(5).takeWhile(_ != '/')
  }

   def getKey(s3Path: String): String = {
    s3Path.drop(5 + getBucketName(s3Path).length + 1)
  }
}
