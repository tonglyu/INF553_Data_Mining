import java.io.PrintWriter

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

object ModelBasedCF {
  def main(args: Array[String]): Unit = {
    System.setProperty("spark.storage.blockManagerTimeoutIntervalMs", "120000")
    val start_time = System.nanoTime()
    val conf = new SparkConf().setAppName("ModelBasedCF").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //1. Load and parse the data
    // allFile refers to all the rating dataset: video_small_num.csv
    val allFile = sc.textFile(System.getProperty("user.dir") + args(0))
    val allHeader = allFile.first()
    val allRdd = allFile.filter(_ != allHeader).map(line => (
      line.split(",")(0).toInt, line.split(",")(1).toInt, line.split(",")(2).toDouble) match {
      case (user, product, rate) => ((user, product), rate)
    })

    // testFile refers to the testing dataset: video_small_testing_num.csv
    val testFile = sc.textFile(System.getProperty("user.dir") + args(1))
    val testHeader = testFile.first()
    val testing = testFile.filter(_ != testHeader).map(line => (
      line.split(",")(0).toInt, line.split(",")(1).toInt, line.split(",")(2).toDouble) match {
      case (user, product, rate) =>
        Rating(user.toInt, product.toInt, rate.toDouble)
    }).cache()

    // Filter the testing dataset from all dataset to obtain training dataset
    val trainRdd = allRdd.subtractByKey(testing.map { case Rating(user, product, rate) => ((user, product), rate) })
      .map { case ((user, product), rate) => Rating(user, product, rate) }.cache()

    //2. Build the recommendation model using ALS
    //The best model refers to script in github from the url of Spark MLib page
    val ranks = List(3, 5)
    val lambdas = List(0.5, 0.7, 1)
    val numIters = (10 to 15) toList
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    val numValidation = testing.count()
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(trainRdd, rank, numIter, lambda)
      val validationRmse = computeRmse(model, testing, numValidation)
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    //3. Evaluate the model on rating data
    val predictions = bestModel.get.predict(testing.map(x => (x.user, x.product)))
      .map { case Rating(user, product, rate) => ((user, product), rate) }
    val min = predictions.values.min()
    val range = predictions.values.max() - min
    val predictions_normal = predictions.map { case ((user, product), rate) =>
      val nor_rate = 5 * (rate - min) / range
      ((user, product), nor_rate)
    }

    val ratesAndPreds = testing.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions_normal)

    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean()
    val RMSE = Math.sqrt(MSE)

    val accum01 = sc.accumulator(0)
    val accum12 = sc.accumulator(0)
    val accum23 = sc.accumulator(0)
    val accum34 = sc.accumulator(0)
    val acccum4x = sc.accumulator(0)
    ratesAndPreds.foreach(error => {
      val err = Math.abs(error._2._1 - error._2._2)
      if (err < 1 && err >= 0) {
        accum01 += 1
      } else if (err < 2 && err >= 1) {
        accum12 += 1
      } else if (err < 3 && err >= 2) {
        accum23 += 1
      } else if (err < 4 && err >= 3) {
        accum34 += 1
      } else {
        acccum4x += 1
      }
    })

    val out = new PrintWriter(System.getProperty("user.dir") + args(2))
    val result = predictions_normal.sortByKey().collect().toList
    for (pred <- result) {
      val line = pred._1._1 + "," + pred._1._2 + "," + pred._2 + "\n"
      out.write(line)
    }
    out.close()

    println(">= 0 and < 1: " + accum01)
    println(">= 1 and < 2: " + accum12)
    println(">= 2 and < 3: " + accum23)
    println(">= 3 and < 4: " + accum34)
    println(">= 4: " + acccum4x)
    println("RMSE: " + RMSE)
    val end_time = System.nanoTime()
    val time = (end_time - start_time) / 1000000000
    println("Time: " + time + " sec")
    sc.stop()
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions = model.predict(data.map(x => (x.user, x.product)))
      .map { case Rating(user, product, rate) => ((user, product), rate) }
    val min = predictions.values.min()
    val range = predictions.values.max() - min
    val predictions_normal = predictions.map { case ((user, product), rate) =>
      val nor_rate = 5 * (rate - min) / range
      ((user, product), nor_rate)
    }
    val predictionsAndRatings = predictions_normal
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }
}