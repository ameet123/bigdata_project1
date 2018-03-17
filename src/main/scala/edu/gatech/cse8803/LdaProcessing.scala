package edu.gatech.cse8803

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object LdaProcessing {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  @transient lazy val LOGGER: Logger = Logger.getLogger("LdaProcessing")

  val vocabSize: Int = 10000
  val maxTermsPerTopic = 10
  val minWordLen = 5
  val notes_path = "/user/af55267/project/notes/consolidated_notes.csv"

  // Field descriptions
  val PATIENT_ID = "subject_id"
  val TEXT_FIELD = "text"
  val TOKENIZED_FIELD = "words"
  val FILTERED_FIELD = "filtered"
  val FEATURES_FIELD = "features"
  // model save/load
  val MODEL_LOCATION = "/user/af55267/project/lda_model"

  def main(args: Array[String]): Unit = {
    val ldaConf: LdaConf = new ProcessArguments().exec(args)

    val conf: SparkConf = new SparkConf().setAppName("bigdata_proj").set("spark.ui.port", "34050")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // Load the raw articles, assign docIDs, and convert to DataFrame
    val rawTextDF: DataFrame = spark.read.option("header", "false").option("quote", "\"").option("escape", "\"").
      option("mode", "DROPMALFORMED").option("delimiter", ",").csv(notes_path).toDF(PATIENT_ID, TEXT_FIELD)


    // Split each document into words
    val tokens: DataFrame = new RegexTokenizer().setGaps(false).setPattern("\\p{L}+").
      setInputCol(TEXT_FIELD).setOutputCol(TOKENIZED_FIELD).setMinTokenLength(minWordLen).transform(rawTextDF)

    val stopwords: Array[String] = spark.read.format("csv").load(ldaConf.stopwords).rdd.
      map(a => a.getString(0)).collect()

    val filteredTokens: DataFrame = new StopWordsRemover().setStopWords(stopwords).setCaseSensitive(false).
      setInputCol(TOKENIZED_FIELD).setOutputCol(FILTERED_FIELD).transform(tokens)

    // Limit to top `vocabSize` most common words and convert to word count vector features
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol(FILTERED_FIELD).
      setOutputCol(FEATURES_FIELD).setVocabSize(vocabSize).fit(filteredTokens)

    val countVectors: RDD[(Long, linalg.Vector)] = cvModel.transform(filteredTokens).select(PATIENT_ID, FEATURES_FIELD).
      rdd.
      map {
        case Row(docId: String, feature: MLVector) => (docId.toLong, Vectors.fromML(feature))
      }.cache()

    val mbf: Double = {
      val corpusSize = countVectors.count()
      2.0 / ldaConf.maxIterations + 1.0 / corpusSize
    }
    // this only gets local, we need distributed
    //    val lda: LDA = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(math.min(1.0, mbf))).
    //      setK(ldaConf.numTopics).setMaxIterations(ldaConf.maxIterations).setDocConcentration(-1)
    // .setTopicConcentration(-1)

    val lda: LDA = new LDA().setK(ldaConf.numTopics).setMaxIterations(ldaConf.maxIterations).setDocConcentration(-1).
      setTopicConcentration(-1)

    val startTime: Long = System.currentTimeMillis()
    val ldaModel: DistributedLDAModel = lda.run(countVectors).asInstanceOf[DistributedLDAModel]
    val elapsed: Long = (System.currentTimeMillis() - startTime) / 1000

    // Print training time
    println(s"Finished training LDA model.  Summary:")
    println(s"Training time (sec)\t$elapsed")
    println("=" * 80)

    val vocabArray: Array[String] = cvModel.vocabulary

    val topicIndices: Array[(Array[Int], Array[Double])] = ldaModel.describeTopics(maxTermsPerTopic)

    val topics: Array[Array[(String, Double)]] = topicIndices.map {
      case (terms, termWeights) => terms.map(vocabArray(_)).zip(termWeights)
    }

    val topicString = new StringBuilder
    println(s"${ldaConf.numTopics} topics:")
    topicString.append(s"${ldaConf.numTopics} topics:\n")
    topics.zipWithIndex.foreach { case (topic, i) =>
      println(s"TOPIC $i")
      topicString.append(s"TOPIC $i \n")
      topic.foreach { case (term, weight) =>
        println(s"$term\t$weight")
        topicString.append(s"$term\t$weight\n")
      }
      println("=" * 80)
      topicString.append("=" * 80 + "\n")
    }

    LOGGER.info("Saving topics to output:" + ldaConf.outputTopics)
    val outRdd = sc.parallelize(Seq(topicString.toString())).repartition(1)
    outRdd.saveAsTextFile(ldaConf.outputTopics)
    // topic assignment by doc
    val topDocPerTopic = ldaModel.topDocumentsPerTopic(1)
    LOGGER.info(">>Saving top topics for each document")
    ldaModel.topTopicsPerDocument(ldaConf.numTopics).repartition(1).saveAsTextFile(ldaConf.outputTopTopicsPerDoc)
  }
}