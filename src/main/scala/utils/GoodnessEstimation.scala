package com.lsc
package utils

import java.io.PrintWriter
import scala.io.Source

object GoodnessEstimation {
  def readValuesFromFile(filePath: String): (Int, Int, Int, Int) = {
    val source = Source.fromFile(filePath)
    val lines = source.getLines().toList
    source.close()

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
          case _     => acc
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

  def writeOutputToFile(filePath: String, content: String): Unit = {
    val writer = new PrintWriter(filePath)
    writer.println(content)
    writer.close()
  }

  def calculateGoodness(inputPath: String, outputPath: String): Unit = {
    val (atl, dtl, ctl, wtl) = readValuesFromFile(inputPath)
    val (acc, btlr, vpr) = calculateMetrics(atl, dtl, ctl, wtl)
    val outputContent = s"ACC: $acc\nBTLR: $btlr\nVPR: $vpr"
    writeOutputToFile(outputPath, outputContent)
  }
}
