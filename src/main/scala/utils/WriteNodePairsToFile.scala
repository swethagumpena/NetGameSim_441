package com.lsc
package utils

import NetGraphAlgebraDefs.{NetModel, NetStateMachine, NodeObject}

import java.io.PrintWriter
import scala.jdk.CollectionConverters.*

object WriteNodePairsToFile {
  def createNodePairsAndWrite(ogSm: NetStateMachine, pbSm: NetStateMachine, filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
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
}
