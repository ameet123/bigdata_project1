import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

Logger.getLogger("org").setLevel(Level.ERROR)
Logger.getLogger("akka").setLevel(Level.ERROR)

val masterUrl = "local[*]"
val conf = new SparkConf().setAppName("bigdata_proj").setMaster(masterUrl).set("spark.ui.port", "34050")
val sc = new SparkContext(conf)
val spark = SparkSession.builder().config(conf).getOrCreate()

def processTopicsFile(path: String, spark: SparkSession, out: String): Unit = {
  val topicsPerDoc = spark.read.format("csv").option("delimiter", ":").option("header", "false").load(path).toDF
  ("subject_id", "topic_list")
  val subjectTopicRDD = topicsPerDoc.rdd.map(
    r => r.getString(0) + "," + r.getString(1).replaceAll("[\\{\\}\\)\\(]", "").split(",")(0)
  )
  subjectTopicRDD.take(4).foreach(println)
  subjectTopicRDD.saveAsTextFile(out)
}