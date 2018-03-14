import org.apache.spark.ml.feature.{CountVectorizer, RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)

val masterUrl = "local[*]"
val conf = new SparkConf().setAppName("bigdata_proj").setMaster(masterUrl).set("spark.ui.port", "34050")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().config(conf).getOrCreate()

val numTopics: Int = 3
val maxIterations: Int = 10
val vocabSize: Int = 10000
val maxTermsPerTopic = 5
val minWordLen = 4
val notes_path = "/home/af55267/script/learning/w2/proj/data/small_cleansed_noteevents.csv"
val stopwords_path = "/home/af55267/script/learning/w2/proj/data/stopwords"
/**
  * Data Preprocessing:
  * to save data removing newline
  * 1. load data using psql -d mimic
  * `\copy  NOTEEVENTS_SMALL from 'NOTEEVENTS.csv' delimiter ',' csv header NULL '';`
  *  2. extract:
  * COPY ( select row_id, subject_id,
  * regexp_replace(text, E'[\\n\\r]+', ' ', 'g' ) as text from noteevents )
  * to '/home/af55267/script/learning/w2/proj/data/cleansed_noteevents.csv'
  * WITH CSV DELIMITER ','  QUOTE '"';
  *  3. copy to HDFS
  * hdfs dfs -put <> /path/in/hdfs
  **/
// Load the raw articles, assign docIDs, and convert to DataFrame
val rawTextDF = spark.read.format("csv").option("header", "false").load(notes_path).toDF("rowid", "subjid", "text").
  select("rowid", "text")


// Split each document into words
val tokens = new RegexTokenizer().setGaps(false).setPattern("\\p{L}+").setInputCol("text").setOutputCol("words").
  setMinTokenLength(minWordLen).transform(rawTextDF)

// Filter out stopwords
val stopwords: Array[String] = spark.read.format("csv").load(stopwords_path).rdd.
  map(a => a.getString(0)).collect()
val filteredTokens = new StopWordsRemover().setStopWords(stopwords).setCaseSensitive(false).setInputCol("words").
  setOutputCol("filtered").transform(tokens)

// Limit to top `vocabSize` most common words and convert to word count vector features
val cvModel = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(vocabSize).
  fit(filteredTokens)
val countVectors = cvModel.transform(filteredTokens).
  select("rowid", "features").rdd.
  map {
    case Row(docId: String, feature: MLVector) => (docId.toLong, Vectors.fromML(feature))
  }.cache()

/**
  * Configure and run LDA
  */
val mbf = {
  val corpusSize = countVectors.count()
  2.0 / maxIterations + 1.0 / corpusSize
}
val lda = new LDA().setOptimizer(new OnlineLDAOptimizer().setMiniBatchFraction(math.min(1.0, mbf))).setK(numTopics).
  setMaxIterations(maxIterations).setDocConcentration(-1).setTopicConcentration(-1)

val startTime = System.currentTimeMillis()
val ldaModel = lda.run(countVectors)
val elapsed = (System.currentTimeMillis() - startTime) / 1000

/**
  * Print results.
  */
// Print training time
println(s"Finished training LDA model.  Summary:")
println(s"Training time (sec)\t$elapsed")
println(s"==========")

// Print the topics, showing the top-weighted terms for each topic.
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = maxTermsPerTopic)
val vocabArray = cvModel.vocabulary
val topics = topicIndices.map { case (terms, termWeights) =>
  terms.map(vocabArray(_)).zip(termWeights)
}
println(s"$numTopics topics:")
topics.zipWithIndex.foreach { case (topic, i) =>
  println(s"TOPIC $i")
  topic.foreach { case (term, weight) => println(s"$term\t$weight") }
  println(s"==========")
}