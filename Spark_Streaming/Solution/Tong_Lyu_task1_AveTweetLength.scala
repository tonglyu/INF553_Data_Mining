import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object AveTweetLength {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AveTweetLength")
      .setMaster("local[*]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    Logger.getLogger("org").setLevel(Level.OFF)

    if (args.length < 4) {
      System.err.println("Usage: TwitterHashTagJoinSentiments <consumer key> <consumer secret> <access token> <access token secret> [<filters>]")
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
    val stream = TwitterUtils.createStream(ssc, None)
    val avg_len = stream.map(x => (x.getText.length, 1))
      .reduce { case ((ti_len, ti_cnt), (tj_len, tj_cnt)) => (ti_len + tj_len, ti_cnt + tj_cnt) }


    var total_len = 0
    var total_count = 0
    avg_len.foreachRDD(rdd => {
      val data = rdd.collect()
      data.foreach { case (len, count) => {
        total_len += len
        total_count += count
        println("Total tweets:" + count + ", Average length: " + total_len.toDouble / total_count)
      }
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
