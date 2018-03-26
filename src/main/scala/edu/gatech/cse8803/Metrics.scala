/**
  * @author Hang Su <hangsu@gatech.edu>.
  */

package edu.gatech.cse8803

import org.apache.spark.rdd.RDD

object Metrics {
  /**
    * Given input RDD with tuples of assigned cluster id by clustering,
    * and corresponding real class. Calculate the purity of clustering.
    * Purity is defined as
    * \fract{1}{N}\sum_K max_j |w_k \cap c_j|
    * where N is the number of samples, K is number of clusters and j
    * is index of class. w_k denotes the set of samples in k-th cluster
    * and c_j denotes set of samples of class j.
    *
    * @param clusterAssignmentAndLabel RDD in the tuple format
    *                                  (assigned_cluster_id, class)
    * @return purity
    */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    clusterAssignmentAndLabel.groupBy(a => a._1).map(m =>
      (
        m._1, m._2.map(c => c._2).toList
      )
    ).map(a => (
      a._1,
      a._2.groupBy(a => a).mapValues(a => a.size).toSeq.sortBy(_._2)(Ordering[Int].reverse).toMap
    )).map(m => m._2.head._2).sum() / clusterAssignmentAndLabel.count().toDouble
  }
}
