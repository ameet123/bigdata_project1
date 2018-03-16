package edu.gatech.cse8803

import org.apache.log4j.Logger

class ProcessArguments {
  @transient lazy val LOGGER: Logger = Logger.getLogger(getClass.getName)
  private val TOTAL_ARGS: Int = 3

  def exec(args: Array[String]): LdaConf = {
    if (args.length != TOTAL_ARGS) {
      LOGGER.error("ERR: passed:" + args.length + " Required:" + TOTAL_ARGS)
      LOGGER.error(s"Usage : spark-submit --class  ${LdaProcessing.getClass.getName} <numTopics> <maxIterations> " +
        s"<output HDFS dir>")
      System.exit(1)
    }
    val numTopics: Int = args(0).toInt
    val maxIterations: Int = args(1).toInt
    val output: String = args(2)
    LdaConf(numTopics, maxIterations, output)
  }
}
