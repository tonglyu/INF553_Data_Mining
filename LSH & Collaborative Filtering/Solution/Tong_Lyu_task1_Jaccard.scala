import java.io.PrintWriter

import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

object JaccardLSH {
  val bands = 25
  val rows = 4
  val numMinhash = bands * rows
  val threshold = 0.5

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val numPartitions = 8

    val conf = new SparkConf()
      .setAppName("JaccardLSH")
      .setMaster("local[4]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)

    //1. Load and parse data
    val file = sc.textFile(System.getProperty("user.dir") + args(0), numPartitions)
    val header = file.first()
    val users = file.filter(_ != header).map(line => line.split(",")(0).toInt).collect()
    val products = file.filter(_ != header).map(line => {
      val split = line.split(",")
      (split(1).toInt, split(0).toInt)
    }).groupByKey().sortBy(_._1).collect().toSeq
    val product_map = products.toMap
    val numPro = products.size
    val numUser = users.length

    //2. Calculate the minHash signatures for each product, and validate candidate pairs with threshold
    val signatures = minHash(products, numPro, numUser)
    val candidates = getCandidates(signatures)
    val similar_pairs = candidates.map(pair => {
      val vector1 = product_map(pair._1).toSet
      val vector2 = product_map(pair._2).toSet
      val jaccard_sim = vector1.intersect(vector2).size / vector1.union(vector2).size.toDouble
      (pair._1, pair._2, jaccard_sim)
    }).filter(_._3 >= threshold).sortBy(x => (x._1, x._2))

    val out = new PrintWriter(System.getProperty("user.dir") + args(1))
    for (pred <- similar_pairs) {
      val line = pred._1 + "," + pred._2 + "," + pred._3 + "\n"
      out.write(line)
    }
    out.close()

    //Calculate the precision and recall with the ground_truth values
    val test = sc.textFile(System.getProperty("user.dir") + "/video_small_ground_truth_jaccard.csv", numPartitions)
    val test_header = test.first()
    val test_set = test.filter(_ != test_header).map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)).collect().toSet
    val model_set = similar_pairs.map { case (proId1, proId2, sim) => (proId1, proId2) }.toSet
    val true_pos = test_set.intersect(model_set).size
    val precision = true_pos / model_set.size.toDouble
    val recall = true_pos / test_set.size.toDouble
    println("Precision: " + precision)
    println("Recall: " + recall)
    //

    val end = System.nanoTime()
    val time = (end - start) / 1000000000
    println("Time: " + time + " sec.")
    sc.stop()
  }

  def minHash(products: Seq[(Int, Iterable[Int])], numPro: Int, numUser: Int): DenseMatrix[Int] = {
    val signatures = DenseMatrix.tabulate(numMinhash, numPro) { case (i, j) => Int.MaxValue }
    val coeffs = (0 until numMinhash).map(_ => (Random.nextInt(numUser), Random.nextInt(numUser), numUser))

    products.foreach(pro_users => {
      val proId = pro_users._1
      val users = pro_users._2
      for (hashId <- 0 until numMinhash) {
        signatures(hashId, proId) = users.map(userId => {
          generateHash(userId, coeffs(hashId)._1, coeffs(hashId)._2, coeffs(hashId)._3)
        }).min
      }
    })
    signatures
  }
  private def generateHash(userId: Int, a: Int, b: Int, m: Int): Int = (a * userId + b) % m

  def getCandidates(signatures: DenseMatrix[Int]): List[(Int, Int)] = {
    var candidates = ListBuffer.empty[(Int, Int)]
    for (i <- 0 until signatures.cols) {
      for (j <- i + 1 until signatures.cols) {
        breakable(
          for (bandId <- 0 until bands) {
            val vec1 = signatures(bandId * rows until bandId * rows + rows, i)
            val vec2 = signatures(bandId * rows until bandId * rows + rows, j)
            if (vec1.equals(vec2)) {
              candidates += Tuple2(i, j)
              break()
            }
          }
        )
      }
    }
    candidates.distinct.toList
  }
}
