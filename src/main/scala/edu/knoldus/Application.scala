package edu.knoldus

import com.typesafe.config.ConfigFactory
import edu.knoldus.operation.TwitterOperation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils


object Application {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val log = Logger.getLogger(this.getClass)
    val configSpark = new SparkConf().setAppName("spark-assignment3").setMaster("local[*]")
    val spark = new SparkContext(configSpark)
    val time = 5
    val ssc = new StreamingContext(spark, Seconds(time))

    val twitterObject =  new TwitterOperation()

//    twitterObject.getTwittes(ssc).print;
    twitterObject.getTwitterCount(ssc)
    ssc.start()
    ssc.awaitTermination()
  }
}

