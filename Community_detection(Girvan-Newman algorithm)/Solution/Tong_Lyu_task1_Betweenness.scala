import java.io. PrintWriter

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{ListBuffer, Queue, Stack}
import scala.collection.{Map, mutable}

object Betweenness {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val numPartitions = 8
    val conf = new SparkConf()
      .setAppName("Betweenness")
      .setMaster("local[*]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)

    //1. Load and parse data
    val file = sc.textFile(System.getProperty("user.dir") + args(0), numPartitions)
    val header = file.first()
    val products = file.filter(_ != header).map(line => {
      val split = line.split(",")
      (split(1).toInt, split(0).toInt)
    })

    //2. Construct the graph
    val edges = products.join(products).filter(x => x._2._1 != x._2._2).map { case (product, (useri, userj)) => ((useri, userj), 1) }
      .reduceByKey(_ + _).filter(_._2 >= 7).map(_._1)

    val graph = edges.groupByKey().mapValues(_.toList).collect().toMap
    val vertices = graph.keySet

    //3. Compute Betweenness
    val betweenness = getBetweenness(graph, vertices).toList.map(x => (x._1._1, x._1._2, x._2)).sortBy(x => (x._1, x._2))
    val out = new PrintWriter(System.getProperty("user.dir") + args(1))
    betweenness.foreach(x => {
      out.write(x.toString() + "\n")
    })
    out.close()
    val end = System.nanoTime()
    val time = (end - start) / 1000000000
    println("Time: " + time + "sec.")
    sc.stop()
  }

  def bfs(graphMap: Map[Int, List[Int]], root: Int): (Stack[Int], Map[Int, ListBuffer[Int]],Map[Int, Int]) = {
    var queue = Queue.empty[Int]
    var stack = new Stack[Int]
    var depth = mutable.Map.empty[Int, Int]
    var shortest_path = mutable.Map.empty[Int, Int]
    var reverse_tree = mutable.Map.empty[Int, ListBuffer[Int]]

    for (vertex <- graphMap.keySet) {
      depth += (vertex -> -1)
      shortest_path += (vertex -> 0)
      reverse_tree += (vertex -> ListBuffer())
    }

    depth.update(root, 0)
    shortest_path.update(root, 1)
    queue.enqueue(root)

    while (queue.nonEmpty) {
      val pre_node = queue.dequeue()
      stack.push(pre_node)
      val cur_nodes = graphMap(pre_node)
      for (node <- cur_nodes) {
        if (depth(node) == -1) {
          depth(node) = depth(pre_node) + 1
          queue.enqueue(node)
        }
        if (depth(node) == depth(pre_node) + 1) {
          shortest_path.update(node, shortest_path(node) + shortest_path(pre_node))
          reverse_tree.update(node, pre_node +: reverse_tree(node))
        }
      }
    }
    (stack, reverse_tree, shortest_path)
  }

  def getCredit(stack: Stack[Int],reverse:Map[Int, ListBuffer[Int]],shortest_path:Map[Int, Int]): Map[(Int, Int), Double]={
    var vertex_credit = mutable.Map.empty[Int, Double]
    var edge_credit = mutable.Map.empty[(Int, Int), Double]

    shortest_path.foreach(x => {
      vertex_credit(x._1) = 1.0
    })

    while (stack.nonEmpty) {
      val child = stack.pop()
      for (parent <- reverse(child)) {
        val credit = vertex_credit(child) * shortest_path(parent).toDouble / shortest_path(child)
        if (edge_credit.contains((parent, child))) {
          if (parent < child) {
            edge_credit.update((parent, child), edge_credit(parent, child) + credit)
          } else {
            edge_credit.update((child, parent), edge_credit(parent, child) + credit)
          }
        } else {
          if (parent < child) {
            edge_credit += ((parent, child) -> credit)
          } else {
            edge_credit += ((child, parent) -> credit)
          }
        }
        vertex_credit.update(parent, vertex_credit(parent) + credit)
      }
    }
    edge_credit
  }

  def getBetweenness(graph: Map[Int, List[Int]], vertices: Set[Int]): Map[(Int, Int), Double] = {
    var betweenness = mutable.Map.empty[(Int, Int), Double]
    for (vertex <- vertices) {
      val bfs_tree = bfs(graph, vertex)
      val edge_credits = getCredit(bfs_tree._1,bfs_tree._2,bfs_tree._3)
      for ((edge, credit) <- edge_credits) {
        if (!betweenness.contains(edge)) {
          betweenness += (edge -> credit)
        } else {
          betweenness.update(edge, betweenness(edge) + credit)
        }
      }
    }
    betweenness.mapValues(_ / 2.0).toMap
  }
}