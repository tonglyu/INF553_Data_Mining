import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TrackHashTags {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TrackHashTags")
      .setMaster("local[*]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    Logger.getLogger("org").setLevel(Level.OFF)

    if (args.length < 4) {
      System.err.println("Usage: TwitterHashTagJoinSentiments <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    // Set the system properties so that Twitter4j library used by Twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val ssc = new StreamingContext(sc, Seconds(2))
    val stream = TwitterUtils.createStream(ssc, None).filter(_.getLang != "ar")
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))


    val topCounts120 = hashTags.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(120), Seconds(2))
      .transform(_.sortBy(_._2, ascending = false))

    topCounts120.foreachRDD(rdd => {
      val topList = rdd.take(5)
      println("\nPopular topics in last 120 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (tag, count) => println("%s,count:%s".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
