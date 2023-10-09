package com.lsc
package utils

import java.io.FileInputStream
import java.io.InputStream
import java.net.URL
import scala.io.Source
import scala.util.Try
import scala.util.Using

object ParseYaml {

  /**
   * Parses a YAML file and extracts specific information related to nodes and edges.
   *
   * @param filePath The path to the YAML file.
   * @return A Map containing lists of added, modified, and removed nodes and edges.
   */
  def parseFile(filePath: String): Map[String, List[String]] = {
    // Define a helper function to open an InputStream based on the filePath
    def openStream(filePath: String): Option[InputStream] = {
      if (filePath.startsWith("http://") || filePath.startsWith("https://")) {
        Try(new URL(filePath).openStream()).toOption
      } else {
        Try(new FileInputStream(filePath)).toOption
      }
    }

    // Attempt to open the specified file or URL
    val streamOption = openStream(filePath)

    // Process the stream if successfully opened, otherwise return an empty Map
    streamOption.flatMap { stream =>
       val lines = Source.fromInputStream(stream).getLines()

      // Extract nodes and edges information
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
             // Extract modified nodes information
             val parts = line.split(":").map(_.trim)
             val newModifiedNodes =
               if (parts.length == 2)
                 parts(1).stripPrefix("[").stripSuffix("]").split(",").map(_.trim).toList
               else modifiedNodes
             (nodes, edges, true, false, false, addedNodes, newModifiedNodes, removedNodes,
              addedEdges, modifiedEdges, removedEdges)
           } else if (line.startsWith("\tRemoved:")) {
             // Extract removed nodes information
             val parts = line.split(":").map(_.trim)
             val newRemovedNodes =
               if (parts.length == 2)
                 parts(1).stripPrefix("[").stripSuffix("]").split(",").map(_.trim).toList
               else removedNodes
             (nodes, edges, false, false, true, addedNodes, modifiedNodes, newRemovedNodes,
              addedEdges, modifiedEdges, removedEdges)
           } else if (line.startsWith("\tAdded:")) {
             // Mark that added information is being processed
             (nodes, edges, false, true, false, addedNodes, modifiedNodes, removedNodes, addedEdges,
              modifiedEdges, removedEdges)
           } else {
             if (nodes) {
               val parts = line.split(":").map(_.trim)
               if (parts.length == 2) {
                 // Extract added nodes information
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
                 // Extract edge information
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

      // Extract the relevant information from the result
       val (_, _, _, _, _, addedNodes, modifiedNodes, removedNodes, addedEdges, modifiedEdges,
            removedEdges) = result

      // Create a Map with keys for the extracted information
       Some(Map(
              "AddedNodes"    -> addedNodes.reverse,
              "ModifiedNodes" -> modifiedNodes,
              "RemovedNodes"  -> removedNodes,
              "AddedEdges"    -> addedEdges.reverse,
              "ModifiedEdges" -> modifiedEdges.reverse,
              "RemovedEdges"  -> removedEdges.reverse
            ))
    }.getOrElse {
      // Handle case where opening the stream failed by returning an empty Map
      Map.empty[String, List[String]]
    }
  }
}
