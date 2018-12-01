import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object UniqueUserCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("UniqueUserCount")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(2))

    Logger.getLogger("org").setLevel(Level.OFF)

    val hostname = args(0)
    val port = args(1).toInt
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream(hostname, port)
    val DStream = lines.map(_.toInt)
    val numGroup = 23
    val numFun = 3

    var hash = ListBuffer.tabulate(numGroup, numFun)((i, j) => 0)
    DStream.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        val users = rdd.collect().toList
        val result = estimate(users, numGroup, numFun, hash)
        println("Estimated number of unique users: " + result + "\n")
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  private def generateHash(userId: Int, a: Int, b: Int, m: Int): Int = (a * userId + b) % m

  def trailZeros(n: Int): Int = {
    var count = 1
    var number = n
    if (number == 0) {
      count = 1
    }
    else {
      while (number % 2 == 0) {
        count = count * 2
        number = number / 2
      }
    }
    count
  }

  def estimate(users: List[Int], numGroup: Int, numFun: Int, hash: ListBuffer[ListBuffer[Int]]): Int = {
    val coeffs = (0 until numGroup).map(x =>
      (0 until numFun).map(_ => (Random.nextInt(8069), Random.nextInt(8069), 8069)).toList).toList
    var estimate = coeffs.map(group => {
      group.map(coeff => {
        users.map(userId => trailZeros(generateHash(userId, coeff._1, coeff._2, coeff._3))).max
      }).to[ListBuffer]
    }).to[ListBuffer]

    for (i <- 0 until numGroup) {
      for (j <- 0 until numFun) {
        if (estimate(i)(j) > hash(i)(j)) {
          hash(i)(j) = estimate(i)(j)
        }
        else {
          estimate(i)(j) = hash(i)(j)
        }
      }
    }

    val avg_group = estimate.map(x => x.sum / x.size).sorted
    var median = 0
    if (avg_group.size % 2 != 0) {
      median = avg_group(avg_group.size / 2)
    } else {
      median = (avg_group(avg_group.size / 2 - 1) + avg_group(avg_group.size / 2)) / 2
    }

    Math.floor(median * 0.7).toInt
  }
}
