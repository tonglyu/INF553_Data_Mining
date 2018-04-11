import java.io.PrintWriter
import org.apache.spark.rdd.RDD._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import Betweenness.getBetweenness

import scala.collection.{immutable, mutable}
import scala.collection.mutable.{ListBuffer, Queue}


object Community {
  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    val numPartitions = 8
    val conf = new SparkConf()
      .setAppName("Community")
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

    //3. Compute Betweenness and Modularity
    val betweenness = getBetweenness(graph, vertices).toMap
    val com_mod = maxModularity(graph, vertices, betweenness)
    val community = com_mod._1.map(_.toList.sorted).sortBy(x => x.head)
    val modularity = com_mod._2

    val out = new PrintWriter(System.getProperty("user.dir") + args(1))
    community.foreach(list => {
      var line = "["
      list.foreach(x => {
        if (x == list.last) {
          line += x.toString
        }else{
          line += x.toString + ","
        }
      })
      line += "]\n"
      out.write(line)
    })
    out.close()
    val end = System.nanoTime()
    val time = (end - start) / 1000000000
    println("Time: " + time + "sec.")
    sc.stop()
  }

  def maxModularity(graph: Map[Int, List[Int]], vertices: Set[Int], betweenness: Map[(Int, Int), Double]): (List[Set[Int]], Double) = {
    var newGraph = mutable.Map(graph.toSeq: _*)
    var maxModularity = -1.0
    var maxCommunity = List.empty[Set[Int]]
    var btw = betweenness
    val numEdges = btw.size
    while (btw.nonEmpty) {
      val community = getCommunity(newGraph,vertices)
      val modularity = getModularity(community, newGraph, numEdges)
      if (modularity > maxModularity) {
        maxModularity = modularity
        maxCommunity = community
      }
      val btwMax = btw.toList.sortBy(_._2).reverse.head._2
      val deleteEdge = ListBuffer.empty[(Int, Int)]
      btw.foreach(x => {
        if (x._2 == btwMax) {
          deleteEdge += x._1
        }
      })
      deleteEdge.foreach { case (v1, v2) => {
        newGraph.update(v1, newGraph(v1).filter(_ != v2))
        newGraph.update(v2, newGraph(v2).filter(_ != v1))
        btw -= Tuple2(v1, v2)
      }
      }
    }
    (maxCommunity, maxModularity)
  }


  def getModularity(community: List[Set[Int]], newGraph: mutable.Map[Int, List[Int]], numEdges: Int): Double = {
    var modularity = 0.0
    for (com <- community) {
      var comMod = 0.0
      for (i <- com) {
        for (j <- com) {
          if (i < j) {
            val ki = newGraph(i).size
            val kj = newGraph(j).size
            if (newGraph.contains(i) &&newGraph(i).contains(j)) {
              comMod += 1.0 - ki * kj.toDouble / (2 *numEdges)
            } else {
              comMod += 0.0 - ki * kj.toDouble / (2 *numEdges)
            }
          }
        }
      }
      modularity += comMod
    }
    modularity / (2 *numEdges)
  }


  def getCommunity(graph: mutable.Map[Int, List[Int]], vertices: Set[Int]): List[Set[Int]] = {
    var vertex_rest = vertices
    val community = ListBuffer.empty[Set[Int]]
    while (vertex_rest.nonEmpty) {
      val comSet = connectComponent(vertex_rest.head, graph)
      community += comSet
      vertex_rest = vertex_rest.diff(comSet)
    }
    community.toList
  }

  def connectComponent(vertex: Int, graph: mutable.Map[Int, List[Int]]): Set[Int] = {
    val queue = Queue.empty[Int]
    val visitVertex = mutable.Set.empty[Int]
    val component = mutable.Set.empty[Int]
    queue.enqueue(vertex)

    while (queue.nonEmpty) {
      val vertex = queue.dequeue()
      visitVertex += vertex
      component.add(vertex)
      for (node <- graph(vertex)) {
        if (!visitVertex.contains(node)) {
          queue.enqueue(node)
        }
      }
    }
    component.toSet
  }
}
