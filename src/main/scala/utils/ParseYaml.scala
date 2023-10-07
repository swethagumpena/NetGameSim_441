package com.lsc
package utils

import java.io.FileInputStream
import java.io.InputStream
import java.net.URL
import scala.io.Source
import scala.util.Try
import scala.util.Using

object ParseYaml {
  def parseFile(filePath: String): Map[String, List[String]] = {
    // Define a helper function to open an InputStream based on the filePath
    def openStream(filePath: String): Option[InputStream] = {
      if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
        Try(new URL(filePath).openStream()).toOption
      } else {
        Try(new FileInputStream(filePath)).toOption
      }
    }

    val streamOption = openStream(filePath)

    streamOption.flatMap { stream =>
       val lines = Source.fromInputStream(stream).getLines()

       val result = lines.foldLeft(
         (false, false, false, false, false, List.empty[String], List.empty[String],
          List.empty[String], List.empty[String], List.empty[String], List.empty[String])
       ) {
         case ((nodes, edges, modified, added, removed, addedNodes, modifiedNodes, removedNodes,
                addedEdges, modifiedEdges, removedEdges), line) =>
           if (line.startsWith("Nodes:")) {
             (true, false, false, false, false, addedNodes, modifiedNodes, removedNodes, addedEdges,
              modifiedEdges, removedEdges)
           } else if (line.startsWith("Edges:")) {
             (false, true, false, false, false, addedNodes, modifiedNodes, removedNodes, addedEdges,
              modifiedEdges, removedEdges)
           } else if (line.startsWith("\tModified:")) {
             val parts = line.split(":").map(_.trim)
             val newModifiedNodes =
               if (parts.length == 2)
                 parts(1).stripPrefix("[").stripSuffix("]").split(",").map(_.trim).toList
               else modifiedNodes
             (nodes, edges, true, false, false, addedNodes, newModifiedNodes, removedNodes,
              addedEdges, modifiedEdges, removedEdges)
           } else if (line.startsWith("\tRemoved:")) {
             val parts = line.split(":").map(_.trim)
             val newRemovedNodes =
               if (parts.length == 2)
                 parts(1).stripPrefix("[").stripSuffix("]").split(",").map(_.trim).toList
               else removedNodes
             (nodes, edges, false, false, true, addedNodes, modifiedNodes, newRemovedNodes,
              addedEdges, modifiedEdges, removedEdges)
           } else if (line.startsWith("\tAdded:")) {
             (nodes, edges, false, true, false, addedNodes, modifiedNodes, removedNodes, addedEdges,
              modifiedEdges, removedEdges)
           } else {
             if (nodes) {
               val parts = line.split(":").map(_.trim)
               if (parts.length == 2) {
                 val newAddedNodes =
                   if (added) parts(1) :: addedNodes else addedNodes
                 (nodes, edges, modified, added, removed, newAddedNodes, modifiedNodes,
                  removedNodes, addedEdges, modifiedEdges, removedEdges)
               } else {
                 (nodes, edges, modified, added, removed, addedNodes, modifiedNodes, removedNodes,
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
                 (nodes, edges, modified, added, removed, addedNodes, modifiedNodes, removedNodes,
                  newAddedEdges, newModifiedEdges, newRemovedEdges)
               } else {
                 (nodes, edges, modified, added, removed, addedNodes, modifiedNodes, removedNodes,
                  addedEdges, modifiedEdges, removedEdges)
               }
             } else {
               (nodes, edges, modified, added, removed, addedNodes, modifiedNodes, removedNodes,
                addedEdges, modifiedEdges, removedEdges)
             }
           }
       }

       val (_, _, _, _, _, addedNodes, modifiedNodes, removedNodes, addedEdges, modifiedEdges,
            removedEdges) = result

       Some(Map(
              "AddedNodes"    -> addedNodes.reverse,
              "ModifiedNodes" -> modifiedNodes,
              "RemovedNodes"  -> removedNodes,
              "AddedEdges"    -> addedEdges.reverse,
              "ModifiedEdges" -> modifiedEdges.reverse,
              "RemovedEdges"  -> removedEdges.reverse
            ))
    }.getOrElse {
      // Handle case where opening the stream failed
      Map.empty[String, List[String]]
    }
  }
}
