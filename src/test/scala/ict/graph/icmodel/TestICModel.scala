package ict.graph.icmodel

import ict.graph.common._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.io.PrintWriter

object TestICModel {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestICModel").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val vs = sc.parallelize(Array((1L, 0), (2L, 0), (3L, 0), (4L, 0), (5L, 0), (6L, 0), (7L, 0)))
    val es = sc.parallelize(Array(Edge(1L, 3L, 0.3), Edge(1L, 4L, 0.9),
      Edge(2L, 4L, 0.6), Edge(2L, 5L, 0.95), Edge(3L, 6L, 0.2), Edge(4L, 6L, 0.3), Edge(6L, 7L, 0.9)))
    val seeds = sc.parallelize(Array((1L, true), (2L, true)))
    var graph = Graph(vs, es)
    val numRand = 10
    val (vSample, aveActivated): (RDD[VertexId], Long) =
      ICTICModel.runICModel1(sc)(graph, seeds, numRand)

    vSample.foreach(println)
    println(s"The total number of activated vertices is $aveActivated on everage")
    sc.stop()
  }

  def TestfileGraph(): Unit = {
    val conf = new SparkConf().setAppName("ICTICModel").setMaster("local[2]")

    val sc = new SparkContext(conf)

    //val verticesFile="hdfs://s1:8020/user/root/input/vertices.txt"
    // val verticesFile="file:///home/liuwei/workspace/v.txt"
    val verticesFile = "vertices.txt"
    val verminPartitions = 2

    //val edgesFile="hdfs://s1:8020/user/root/input/edges.txt"
    // val edgesFile="hdfs://s1:8020/user/root/input/v.txt"
    val edgesFile = "edges.txt"
    val edgminPartitions = 2

    //load graph from file
    val loader = new IctGraphLoader(sc)
    val graph = loader.LoaderFile(verticesFile, verminPartitions, edgesFile, edgminPartitions)
    //println("num edges = " + graph.numEdges);
    //println("num vertices = " + graph.numVertices); 

    val seedFile = "seeds.txt"
    //read seedfile for propagation
    val seedread = sc.textFile(seedFile, verminPartitions)
    val seeds: RDD[(VertexId, Boolean)] = seedread.map(
      line => {
        val verfields = line.trim().split("\\s+", 2)
        (verfields(0).toLong, true)
      })

    val numRand = 1

    val (vSample, aveActivated): (RDD[VertexId], Long) =
      ICTICModel.runICModel1(sc)(graph, seeds, numRand)

    //vSample.saveAsTextFile("activatedVertices.txt")
    val out = new PrintWriter("activatedVertices.txt")
    out.println(vSample.collect.mkString("\n"))
    out.close()

    println(s"The total number of activated vertices is $aveActivated")
    sc.stop()
  }

}