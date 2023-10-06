package com.lsc
package utils

import scala.io.Source
import scala.util.Using

object ParseYaml {
  def parseFile(filePath: String): Map[String, List[String]] = {
    Using(Source.fromFile(filePath)) { source =>
      val lines = source.getLines()

      val result = lines.foldLeft(
        (false, false, false, false, false,
          List.empty[String], List.empty[String], List.empty[String],
          List.empty[String], List.empty[String], List.empty[String])
      ) {
        case ((nodes, edges, modified, added, removed,
        addedNodes, modifiedNodes, removedNodes,
        addedEdges, modifiedEdges, removedEdges), line) =>
          if (line.startsWith("Nodes:")) {
            (true, false, false, false, false,
              addedNodes, modifiedNodes, removedNodes,
              addedEdges, modifiedEdges, removedEdges)
          } else if (line.startsWith("Edges:")) {
            (false, true, false, false, false,
              addedNodes, modifiedNodes, removedNodes,
              addedEdges, modifiedEdges, removedEdges)
          } else if (line.startsWith("\tModified:")) {
            val parts = line.split(":").map(_.trim)
            val newModifiedNodes = if (parts.length == 2) parts(1).stripPrefix("[").stripSuffix("]").split(",").map(_.trim).toList else modifiedNodes
            (nodes, edges, true, false, false,
              addedNodes, newModifiedNodes, removedNodes,
              addedEdges, modifiedEdges, removedEdges)
          } else if (line.startsWith("\tRemoved:")) {
            val parts = line.split(":").map(_.trim)
            val newRemovedNodes = if (parts.length == 2) parts(1).stripPrefix("[").stripSuffix("]").split(",").map(_.trim).toList else removedNodes
            (nodes, edges, false, false, true,
              addedNodes, modifiedNodes, newRemovedNodes,
              addedEdges, modifiedEdges, removedEdges)
          } else if (line.startsWith("\tAdded:")) {
            (nodes, edges, false, true, false,
              addedNodes, modifiedNodes, removedNodes,
              addedEdges, modifiedEdges, removedEdges)
          } else {
            if (nodes) {
              val parts = line.split(":").map(_.trim)
              if (parts.length == 2) {
                val newAddedNodes =
                  if (added) parts(1) :: addedNodes else addedNodes
                (nodes, edges, modified, added, removed,
                  newAddedNodes, modifiedNodes, removedNodes,
                  addedEdges, modifiedEdges, removedEdges)
              } else {
                (nodes, edges, modified, added, removed,
                  addedNodes, modifiedNodes, removedNodes,
                  addedEdges, modifiedEdges, removedEdges)
              }
            } else if (edges) {
              val parts = line.split(":").map(_.trim)
              if (parts.length == 2) {
                val edge = s"${parts(0)}-${parts(1)}"
                val newAddedEdges =
                  if (added) edge :: addedEdges else addedEdges
                val newModifiedEdges =
                  if (modified) edge :: modifiedEdges else modifiedEdges
                val newRemovedEdges =
                  if (removed) edge :: removedEdges else removedEdges
                (nodes, edges, modified, added, removed,
                  addedNodes, modifiedNodes, removedNodes,
                  newAddedEdges, newModifiedEdges, newRemovedEdges)
              } else {
                (nodes, edges, modified, added, removed,
                  addedNodes, modifiedNodes, removedNodes,
                  addedEdges, modifiedEdges, removedEdges)
              }
            } else {
              (nodes, edges, modified, added, removed,
                addedNodes, modifiedNodes, removedNodes,
                addedEdges, modifiedEdges, removedEdges)
            }
          }
      }

      val (_, _, _, _, _,
      addedNodes, modifiedNodes, removedNodes,
      addedEdges, modifiedEdges, removedEdges) = result

      Map(
        "addedNodes" -> addedNodes.reverse,
        "modifiedNodes" -> modifiedNodes,
        "removedNodes" -> removedNodes,
        "addedEdges" -> addedEdges.reverse,
        "modifiedEdges" -> modifiedEdges.reverse,
        "removedEdges" -> removedEdges.reverse
      )
    }.get // This will handle any exceptions thrown by Using and return the result
  }
}
