package com.lsc
package utils

object SimilarityScoreCalculation {
  def jaccardSimilarity(set1: Set[Int], set2: Set[Int]): Double = {
    val intersectionSize = set1.intersect(set2).size
    val unionSize = set1.union(set2).size
    if (unionSize == 0) 0.0 else intersectionSize.toDouble / unionSize.toDouble
  }
}
