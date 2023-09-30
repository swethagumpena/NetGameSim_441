package com.lsc
package utils

import NetGraphAlgebraDefs.{NetModel, NetStateMachine, NodeObject}
import com.google.common.graph.EndpointPair

import java.io.PrintWriter
import scala.jdk.CollectionConverters.*

object WriteEdgePairsToFile {

  def getEdgeInfo(edge: EndpointPair[NodeObject], sm: NetStateMachine): String = {
    val ithNode = edge.nodeU()
    val jthNode = edge.nodeV()

    val cost = sm.edgeValue(ithNode, jthNode).get.cost.toFloat
    val actionType = sm.edgeValue(ithNode, jthNode).get.actionType
    s"($ithNode-$jthNode-$actionType-$cost)"
  }
  
  def createEdgePairsAndWrite(ogSm: NetStateMachine, pbSm: NetStateMachine, filePath: String): Unit = {
    val writer = new PrintWriter(filePath)
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
}
