package com.lsc
package utils

object SimilarityScoreCalculation {
  /**
   * Calculates the Jaccard Similarity between two sets.
   *
   * @param set1 The first set of elements
   * @param set2 The second set of elements
   * @return The Jaccard Similarity as a Double value
   */
  def jaccardSimilarity(set1: Set[Int], set2: Set[Int]): Double = {
    // Calculate the size of the intersection between set1 and set2
    val intersectionSize = set1.intersect(set2).size

    // Calculate the size of the union of set1 and set2
    val unionSize = set1.union(set2).size

    // Calculate the Jaccard Similarity
    if (unionSize == 0) 0.0 else intersectionSize.toDouble / unionSize.toDouble
  }
}
