package edu.gatech.cse8803

import edu.gatech.cse8803.features.FeatureConstruction
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Matrix, Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * This will process a file where each subject has multiple topics with a Double weight attached to it.
  */
object MultiTopicKmeans {

  def main(args: Array[String]): Unit = {
    val subjectTopicMapFile: String = args(0)
    val subjectMortalityFile: String = args(1)
    val conf: SparkConf = new SparkConf().setAppName("bigdata_proj").set("spark.ui.port", "34050")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    println(s">>MultiTopic Kmeans: topic-file:$subjectTopicMapFile mortality:$subjectMortalityFile")
    exec(spark, subjectTopicMapFile, subjectMortalityFile)
  }

  /**
    * @param spark               spark session
    * @param subjectTopicMapFile to subject->topic file
    */
  def exec(spark: SparkSession, subjectTopicMapFile: String, subjectMortalityFile: String): Unit = {
    val topicMapRDD: RDD[(String, String, Double)] = spark.read.format("csv").option("header", "false").
      load(subjectTopicMapFile).rdd.map(r => (r.getString(0), r.getString(1), r.getString(2).toDouble))

    val subjectMortalityRDD: RDD[(String, Int)] = spark.read.format("csv").option("header", "false").
      load(subjectMortalityFile).rdd.map(r => (r.getString(0), r.getString(1).toInt))

    // get feature tuple
    val topicFeature: RDD[((String, String), Double)] = FeatureConstruction.constructMultiTopicFeatureTuple(topicMapRDD)
    val rawFeatures: RDD[(String, linalg.Vector)] = FeatureConstruction.construct(spark.sparkContext, topicFeature)

    // invoke k-means
    println(">>MultiTopic Kmeans: Running kmeans on features...")
    val purity: Double = kmeans(rawFeatures, 2, subjectMortalityRDD)
    println(s"MultiTopic K-means purity=>$purity")
  }

  def kmeans(rawFeatures: RDD[(String, Vector)], numClusters: Int, realLabels: RDD[(String, Int)]): Double = {
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map {
      case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))
    }
    val rawFeatureVectors = features.map(_._2).cache()
    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows

    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]

    /** transform a feature into its reduced dimension representation */
    def transform(feature: Vector): Vector = {
      Vectors.dense(Matrices.dense(1, feature.size, feature.toArray).multiply(densePc).toArray)
    }

    // Kmeans training
    val numIterations = 20
    val seed = 8803L
    import org.apache.spark.mllib.clustering.KMeans
    val clusters = KMeans.train(featureVectors, numClusters, numIterations)

    // predict
    val predicted: RDD[(String, Int)] = rawFeatures.map(m =>
      (m._1, clusters.predict(transform(m._2)))
    )
    // join with real labels
    val predRealVector = predicted.keyBy(m => m._1).join(realLabels.keyBy(p => p._1)).
      map(m => (m._2._1._2 + 1, m._2._2._2))

    // purity
    Metrics.purity(predRealVector)
  }
}
