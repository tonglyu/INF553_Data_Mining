import java.io.PrintWriter
import java.io.File
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.mutable.{ListBuffer, Map, Set}
import scala.util.control.Breaks.{break, breakable}


object son {

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime()
    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)

    val caseNumber = args(0).toInt
    val file = sc.textFile(System.getProperty("user.dir")+args(1),1)
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
    val phase1 = baskets.mapPartitions(lists => apriori(lists, minSupport/partition)).distinct()
    val candidate = phase1.collect()

    //Phase2: count the real occurrence for each candidate itemset
    val phase2 = baskets.flatMap(lists => {
      val bSet = lists.toSet
      var counts = Map.empty[Set[String], Int]
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
    val totalCount = freqItemsets.count()
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
      var selfSortList = new ListBuffer[List[String]]
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
    out.close()

    val endTime = System.nanoTime()
    val time = (endTime - startTime) / 1000000000

    println("There are total "+ totalCount.toString + " frequent itemsets. \nThe execution time is: " + time.toString +"s")

  }


  def apriori(iter: Iterator[List[String]], minSupport: Int): Iterator[Set[String]] = {
    var items = Map.empty[String, Int]
    var baskets = new ListBuffer[List[String]]
    var freqItemsets = new ListBuffer[Set[String]]

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
        freqItemsets += Set(t._1)
        freqItems += t._1
      }
    })

    var canKset = freqItemsets.toList
    var k = 2
    while (canKset.nonEmpty) {
      canKset = getCandidate(canKset, k, freqItems)
      val freqKset = getFrequent(canKset, baskets.toList, minSupport)
      if (freqKset.nonEmpty) {
        freqItemsets ++= freqKset
      }
      canKset = freqKset
      k = k + 1
    }//while there is no candidate with k size, the loop is over

    freqItemsets.toIterator
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

    var freqKset = new ListBuffer[Set[String]]
    countKset.foreach(set => if (set._2 >= minSupport) freqKset += set._1)
    freqKset.toList
  }
}
