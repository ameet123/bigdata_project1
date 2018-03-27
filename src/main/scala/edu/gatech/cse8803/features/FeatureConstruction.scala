package edu.gatech.cse8803.features

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object FeatureConstruction {
  type FeatureTuple = ((String, String), Double)

  /**
    *
    * @param subjectRdd RDD of topics
    * @return RDD of feature tuples
    */
  def constructTopicFeatureTuple(subjectRdd: RDD[(String, String)]): RDD[FeatureTuple] = {
    subjectRdd.map {
      case (subject_id, topic) => ((subject_id, topic), 1)
    }.reduceByKey((v1, v2) => v1 + v2).map(m => (m._1, m._2.toDouble))
  }

  def constructMultiTopicFeatureTuple(subjectRdd: RDD[(String, String, Double)]): RDD[FeatureTuple] = {
    subjectRdd.map {
      case (subject_id, topic, weight) => ((subject_id, topic), weight)
    }
  }

  /**
    * Given a feature tuples RDD, construct features in vector
    * format for each patient. feature name should be mapped
    * to some index and convert to sparse feature format.
    *
    * @param sc      SparkContext to run
    * @param feature RDD of input feature tuples
    * @return
    */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {
    feature.cache()
    val featureMap = feature.map(f => f._1._2).distinct().collect().zipWithIndex.toMap
    val scFeatureMap = sc.broadcast(featureMap)

    val result = feature.map(m => (m._1._1, m._1._2, m._2)).groupBy(g => g._1).
      map(m =>
        (m._1,
          Vectors.sparse(scFeatureMap.value.size, m._2.map(c => (scFeatureMap.value(c._2), c._3)).toSeq)
        )
      )
    result
  }
}
