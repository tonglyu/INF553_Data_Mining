package com.soundcloud.lsh

import java.io.PrintWriter

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    val start = System.nanoTime()

    // init spark context
    val numPartitions = 8
    val conf = new SparkConf()
      .setAppName("LSH-Cosine")
      .setMaster("local[4]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)


    val file = sc.textFile(System.getProperty("user.dir") + args(0), numPartitions)
    val header = file.first()
    val products = file.filter(_ != header).map {
      line =>
        val split = line.split(",")
        (split(1).toInt, (split(0).toInt, 1.0))
    }

    val numUser = products.map(x => x._2._1).max() + 1

    val data = products.groupByKey().map(ele => {
      val vec = Vectors.sparse(numUser, ele._2.toArray).toDense
      (ele._1, vec)
    })

    // create an unique id for each word by zipping with the RDD index
    val indexed = data.zipWithIndex.persist(storageLevel)

    // create indexed row matrix where every row represents one word
    val rows = indexed.map {
      case ((product, users), index) =>
        IndexedRow(index, users)
    }

    // store index for later re-mapping (index to word)
    val index = indexed.map {
      case ((product, users), index) =>
        (index, product)
    }.persist(storageLevel)

    // create an input matrix from all rows and run lsh on it
    val matrix = new IndexedRowMatrix(rows)

    val lsh = new Lsh(
      minCosineSimilarity = 0.5,
      dimensions = 20,
      numNeighbours = 215,
      numPermutations = 10,
      partitions = numPartitions,
      storageLevel = storageLevel
    )
    val similarityMatrix = lsh.join(matrix)

    // remap both ids back to words
    val remapFirst = similarityMatrix.entries.keyBy(_.i).join(index).values
    val remapSecond = remapFirst.keyBy { case (entry, word1) => entry.j }.join(index).values.map {
      case ((entry, proId1), proId2) =>
        if (proId1 < proId2) {
          ((proId1, proId2), entry.value)
        }else {
          ((proId2, proId1), entry.value)
        }
    }.sortBy(_._1)

    val out = new PrintWriter(System.getProperty("user.dir") + args(1))
    val model = remapSecond.collect().toList
    for (pred <- model) {
      val line = pred._1._1 + "," + pred._1._2 + "," + pred._2 + "\n"
      out.write(line)
    }
    out.close()

    val test = sc.textFile(System.getProperty("user.dir")+"/data/video_small_ground_truth_cosine.csv", numPartitions)
    val test_header = test.first()
    val test_set = test.filter(_ != test_header).map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt)).collect().toSet
    val model_set = remapSecond.map { case ((word1, word2), entry) => (word1, word2) }.collect().toSet
    val true_pos = test_set.intersect(model_set).size
    val precision = true_pos / model_set.size.toDouble
    val recall = true_pos / test_set.size.toDouble
    println("Precision: " + precision)
    println("Recall: " + recall)

    val end = System.nanoTime()
    val time = (end - start) / 1000000000
    println("Time: " + time + " sec.")
    sc.stop()
  }
}
