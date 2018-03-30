import java.io.PrintWriter
import org.apache.spark.{SparkConf, SparkContext}


object UserBasedCF {
  def main(args: Array[String]): Unit = {
    val start_time = System.nanoTime()
    val numPartitions = 8
    val conf = new SparkConf().setAppName("UserBasedCF").setMaster("local[2]")
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
    val testRdd = testFile.filter(_ != testHeader).map(line => (line.split(",")(0).toInt, line.split(",")(1).toInt))
    val testing = testFile.filter(_ != testHeader).map(line => (
      (line.split(",")(0).toInt, line.split(",")(1).toInt), line.split(",")(2).toDouble))

    // Filter the testing dataset from all dataset to obtain training dataset
    val trainRdd = allRdd.subtractByKey(testing)
    val train_product = trainRdd.map { case ((user, product), rate) => (product, (user, rate)) }
    val train_pro2 = train_product.join(train_product)
    val user_sum_size = trainRdd.map { case ((user, product), rate) => (user, rate) }.groupByKey()
      .map(tuple => (tuple._1, (tuple._2.sum[Double], tuple._2.size)))
    val user_avgRate = user_sum_size.map { case (user, (sum, size)) => (user, sum / size) }

    // 2. Calculate pearson similarity
    val rate_matrix = train_pro2.map { case (product, ((useri, ratei), (userj, ratej))) => ((useri, userj), (ratei, ratej)) }
    val useri_avgrate = rate_matrix.map { case ((useri, userj), (ratei, ratej)) => ((useri, userj), ratei) }.groupByKey()
      .map(tuple => (tuple._1, tuple._2.sum / tuple._2.size))
    val userj_avgrate = rate_matrix.map { case ((useri, userj), (ratei, ratej)) => ((useri, userj), ratej) }.groupByKey()
      .map(tuple => (tuple._1, tuple._2.sum / tuple._2.size))
    val copro_avgrate = useri_avgrate.join(userj_avgrate)
    val copro_r_m = rate_matrix.filter(x => x._1._1 != x._1._2).join(copro_avgrate)
      .map { case ((useri, userj), ((ratei, ratej), (meani, meanj))) => ((useri, userj), (ratei - meani, ratej - meanj)) }
    val numerator = copro_r_m.map(x => (x._1, x._2._1 * x._2._2)).reduceByKey(_ + _)
    val denom_i = copro_r_m.map(x => (x._1, Math.pow(x._2._1, 2))).reduceByKey(_ + _)
    val denom_j = copro_r_m.map(x => (x._1, Math.pow(x._2._2, 2))).reduceByKey(_ + _)
    val denominator = denom_i.join(denom_j)
      .map { case ((useri, userj), (sum_powi, sum_powj)) => ((useri, userj), Math.sqrt(sum_powi) * Math.sqrt(sum_powj)) }
    val weight = numerator.join(denominator).map { case ((useri, userj), (num, denom)) =>
      if (denom == 0) ((useri, userj), 0.0)
      else ((useri, userj), num / denom)
    }

    //3. calculate the effect of other users
    val test_product = testRdd.map { case (user, product) => (product, user) }
    val test_train = test_product.join(train_product)
      .map { case (product, (useri, (userj, ratej))) => (userj, (useri, product, ratej)) }
    //(sumj - ratej) / (sizej - 1) refers to the average rating for userj except itemj
    val test_weight = test_train.join(user_sum_size)
      .map { case (userj, ((useri, product, ratej), (sumj, sizej))) =>
        ((useri, userj), (product, ratej - (sumj - ratej) / (sizej - 1))) }.join(weight)

    val pred_num = test_weight.map { case ((useri, userj), ((product, r_rm), w)) => ((useri, product), r_rm * w) }.reduceByKey(_ + _)
    val pred_deno = test_weight.map { case ((useri, userj), ((product, r_rm), w)) => ((useri, product), Math.abs(w)) }.reduceByKey(_ + _)
    val pred_other = pred_num.join(pred_deno).map { case ((user, product), (num, denom)) =>
      if (denom == 0) (user, (product, 0.0))
      else (user, (product, num / denom))
    }

    //4. calculate the predictions
    val predictions = user_avgRate.join(pred_other).map { case (user, ((ru), (product, ro))) => ((user, product), ru + ro) }
    val min = predictions.values.min()
    val range = predictions.values.max() - min
    val predictions_normal = predictions.map { case ((user, product), rate) =>
      if (rate > 5) {
        ((user, product), 5.0)
      } else if (rate < 0){
        ((user, product), 0.0)
      }else{
        ((user, product),rate)
      }
    }

    val ratesAndPreds = testing.join(predictions_normal)
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
  }
}
