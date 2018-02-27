import java.io.PrintWriter
import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.immutable
import scala.collection.mutable.{ListBuffer, Map, Set}
import scala.util.control.Breaks.{break, breakable}


object son {

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)

    val caseNumber = args(0).toInt
    val file = sc.textFile(System.getProperty("user.dir")+args(1))
    val header = file.first()
    val input = file.filter(_ != header)
    val partition = input.partitions.length
    val minSupport = args(2).toInt
    val name = args(1).split('.')(0).filter(_ != '/')
    val path = new File("Tong_Lyu_SON_"+name+".case"+caseNumber.toString+"-"
      +minSupport.toString+".txt")
    val out = new PrintWriter(path)

    var baskets: RDD[List[String]] = null
    if (caseNumber == 1) {
      val record = input.map(line => (line.split(',')(0), line.split(',')(1)))
        .groupByKey().map(_._2.toList)
      baskets = record
    } else {
      val record = input.map(line => (line.split(',')(1), line.split(',')(0)))
        .groupByKey().map(_._2.toList)
      baskets = record
    }

    //Phase1: use apriori to select all candidate itemsets
    val phase1 = baskets.mapPartitions(lists => apriori(lists, minSupport/partition))
      .reduceByKey((a,b) => List.concat(a,b).distinct).flatMap(_._2.map(set => (set,1)))
    val candidate = phase1.map(_._1.toSet).collect()

    //Phase2: count the real occurrence for each candidate itemset
    val phase2 = baskets.flatMap(lists => {
      val bSet = lists.toSet
      var counts = Map.empty[immutable.Set[String], Int]
      for (set <- candidate) {
        if (set.subsetOf(bSet)) {
          if (counts.contains(set)) {
            counts(set) += 1
          } else {
            counts += (set -> 1)
          }
        }
      }
      counts
    })

    val freqItemsets = phase2.reduceByKey(_+_).filter(_._2 >= minSupport)
    val result = freqItemsets.keys.map(_.toList).groupBy(_.size).sortByKey().collect()

    //Override the ordering to maintain lexicographical order of itemsets
    implicit object listOrdering extends Ordering[List[String]] {
      override def compare(p1: List[String], p2: List[String]): Int = {
        val size = p1.size
        var flag = 0
        breakable {
          for (k <- 0 until size) {
            val tmp = p1(k) compareTo p2(k)
            if (tmp != 0) {
              flag = tmp
              break()
            }
          }
        }
        flag
      }
    }

    //Self-ordering
    for (kTuple <- result) {
      val list = kTuple._2.toList
      var selfSortList = ListBuffer.empty[List[String]]
      for (row <- list) {
        val set = row.sorted
        selfSortList += set
      }

      val sortList = selfSortList.sorted(listOrdering)
      for (set <- sortList) {
        if (set == sortList.last) {
          val line = "('" + set.mkString("','") + "')"
          out.write(line)
        } else {
          val line = "('" + set.mkString("','") + "')" + ","
          out.write(line)
        }
      }
      out.write("\n\n")
    }


    val endTime = System.nanoTime()
    val time = (endTime - startTime) / 1000000000

    println("The execution time is:" + time.toString +"s")
    out.close()
  }


  def apriori(iter: Iterator[List[String]], minSupport: Int): Iterator[(Int, List[Set[String]])] = {
    var items = Map.empty[String, Int]
    var baskets = ListBuffer.empty[List[String]]
    var freqSets = ListBuffer.empty[Set[String]]
    var results = Map.empty[Int, List[Set[String]]]

    //Construct the list of baskets and single items
    while (iter.hasNext) {
      val next = iter.next()
      baskets += next
      for (single <- next) {
        if (items.contains(single)) {
          items(single) += 1
        } else {
          items += (single -> 1)
        }
      }
    }

    var freqItems= Set.empty[String]
    items.foreach(t => {
      if (t._2  >= minSupport) {
        freqSets += Set(t._1)
        freqItems += t._1
      }
    })

    results += (1 -> freqSets.toList)
    var canKset = freqSets.toList
    var k = 2
    while (canKset.nonEmpty) {
      canKset = getCandidate(canKset, k, freqItems)
      val freqKset = getFrequent(canKset, baskets.toList, minSupport)
      if (freqKset.nonEmpty) {
        results += (k -> freqKset)
      }
      canKset = freqKset
      k = k + 1
    }//while there is no candidate with k size, the loop is over

    results.toIterator
  }

  def getCandidate(canK1set: List[Set[String]], k: Int, freqItems: Set[String]): List[Set[String]] = {
    var canKSet = Set.empty[Set[String]]
    for (itemset <- canK1set) {
      for (item <- freqItems) {
        var newItemset = itemset.clone()
        newItemset += item
        if (newItemset.size == k) {//filter the duplicate
        val subK1set = newItemset.subsets(k - 1).toList
          var flag = true
          if(k > 2) {
            //check if all the k-1 subsets of new itemsets all frequent in k-1 frequent itemsets
            subK1set.foreach(set => if (!canK1set.contains(set)) flag = false)
          }

          if(flag) {
            canKSet += newItemset
          }
        }
      }
    }
    canKSet.toList
  }

  def getFrequent(canKset: List[Set[String]], baskets: List[List[String]], minSupport: Int) : List[Set[String]] = {
    var countKset = Map.empty[Set[String], Int]
    baskets.foreach(basket => {
      val bSet = basket.toSet
      for (set <- canKset) {
        //Count the occurrence of all candidate itemsets of k size
        if (set.subsetOf(bSet)) {
          if (countKset.contains(set)) {
            countKset(set) += 1
          } else {
            countKset += (set -> 1)
          }
        }
      }
    })

    var freqKset = ListBuffer.empty[Set[String]]
    countKset.foreach(set => if (set._2 >= minSupport) freqKset += set._1)
    freqKset.toList
  }
}
