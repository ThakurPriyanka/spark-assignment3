package edu.knoldus.operation

import java.sql.Connection

import edu.knoldus.ConnectionDb
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

class TwitterOperation {
  Logger.getLogger("org").setLevel(Level.OFF)
  val log = Logger.getLogger(this.getClass)

  def getTwittes(ssc: StreamingContext): DStream[String] = {
    val tweets = TwitterUtils.createStream(ssc, None)
    val statuses = tweets.map(status => status.getText())
    statuses
  }

  def getTwitterCount(ssc: StreamingContext): Unit = {
    val stream = TwitterUtils.createStream(ssc, None)
    val hashTags = stream.flatMap(status => status.getHashtagEntities)
    val hashTagPairs = hashTags.map(hashtag => ("#" + hashtag.getText, 1))
    val topCounts3 = hashTagPairs.reduceByKeyAndWindow((tag1, tag2) => {
      tag1 + tag2
    }, Seconds(5))
    val sortedTopCounts3 = topCounts3.transform(rdd =>
      rdd.sortBy(hashtagPair => hashtagPair._2, false))
    var flag = false
    sortedTopCounts3.foreachRDD(rdd => {
      val topList = rdd.take(3)
      var connection: Connection = null
      try {
        val instance = ConnectionDb.getInstance
        connection = instance.getConnection
        val statement = connection.createStatement()
        val instertData = connection.prepareStatement("INSERT INTO tweetCount (hashTag,count) VALUES (?,?) ")

        val tweetResult = statement.executeQuery("SELECT hashtag FROM tweetCount")
        val updateTweet = connection.prepareStatement("Update tweetCount set count = ? where hashTag = ?")

        log.info("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
        topList.foreach { case (tag, count) => log.info("%s (%d tweets)".format(tag, count)) }
        topList.foreach { case (tag, count) => {

          while (tweetResult.next()) {
            val hashTag = tweetResult.getString("hashTag")
            if (hashTag.equals(tag)) {
              updateTweet.setInt(1, count)
              updateTweet.setString(2, tag)
              flag = true
            }
          }
          if (!flag.equals(true)) {
            instertData.setString(1, tag)

            instertData.setInt(2, count)

            instertData.executeUpdate
          }
        }
        }
      }
      catch {
        case e => e.printStackTrace
      }
    })
  }
}
