package com.lsc
package utils

import java.io.PrintWriter
import scala.io.Source
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, PutObjectRequest, S3Exception}
import software.amazon.awssdk.regions.Region

import scala.annotation.tailrec

object GoodnessEstimation {
  private def readFromLocal(filePath: String): List[String] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines().toList
    } finally {
      source.close()
    }
  }

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

  def calculateMetrics(ATL: Int, DTL: Int, CTL: Int, WTL: Int): (Double, Double, Double) = {
    val GTL = ATL + DTL
    val BTL = CTL + WTL
    val RTL = GTL + BTL
    val ACC = ATL.toDouble / RTL
    val BTLR = (WTL.toDouble) / RTL
    val VPR = (GTL - BTL).toDouble / (2 * RTL) + 0.5
    (ACC, BTLR, VPR)
  }

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

  private def writeToLocal(filePath: String, content: String): Unit = {
    val writer = new PrintWriter(filePath)
    writer.println(content)
    writer.close()
  }

  def writeOutput(filePath: String, content: String): Unit = {
    if (filePath.startsWith("s3://")) {
      writeToS3(filePath, content)
    } else {
      writeToLocal(filePath, content)
    }
  }

  def calculateGoodness(inputPath: String, outputPath: String): Unit = {
    val (atl, dtl, ctl, wtl) = readValuesFromFile(inputPath)
    val (acc, btlr, vpr) = calculateMetrics(atl, dtl, ctl, wtl)
    val outputContent = s"ACC: $acc\nBTLR: $btlr\nVPR: $vpr"
    writeOutput(outputPath, outputContent)
  }

  private def getBucketName(s3Path: String): String = {
    s3Path.drop(5).takeWhile(_ != '/')
  }

  private def getKey(s3Path: String): String = {
    s3Path.drop(5 + getBucketName(s3Path).length + 1)
  }
}
