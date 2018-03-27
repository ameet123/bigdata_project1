import org.apache.log4j.{Level, Logger}
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
  subjectTopicRDD.saveAsTextFile(out)
}

def flattenTopicsFile(in: String, spark: SparkSession, out: String): Unit = {
  val topicsPerDoc = spark.read.format("csv").option("delimiter", ":").option("header", "false").load(in)
  import java.util.regex.Pattern
  def getToken(str: String, subject: String): List[(String, String, Double)] = {
    var topicList: List[(String, String, Double)] = List()
    val topicPattern = Pattern.compile("\\((.*?)\\)")
    val matchPattern = topicPattern.matcher(str)
    while (matchPattern.find) {
      val kvArray = matchPattern.group(1).split(",")
      topicList = topicList :+ (subject, kvArray(0), kvArray(1).toDouble)
    }
    topicList
  }

  val subjectTopicRDD = topicsPerDoc.rdd.map {
    r => getToken(r.getString(1).replaceAll("[\\{\\}]", ""), r.getString(0))
  }.flatMap(a => a)
  import spark.implicits._
  subjectTopicRDD.repartition(1).toDF().write.option("header", "false").csv(out)
}