package ict.graph.NMF

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.mllib.linalg.Matrices
import java.io.PrintWriter
import org.apache.spark.Logging
import org.apache.log4j.PropertyConfigurator
object ICTGraphNMFTest extends Logging {
  /**
   * Verify Graph NMF
   *
   * Graph Information:
   *
   * 	1 --> 2      5--->6
   *    |    /       |   /
   *    |   /        |  /
   *    V  <         V <
   *    3 --> 4 <--- 7
   *
   *    7 vertex
   *    8 edges:
   *    	(1,2,0.8)
   *     	(1,3,0.6)
   *      	(2,3,0.9)
   *       	(3,4,0.3)
   *        (5,6,0.4)
   *        (5,7,0.8)
   *        (6,7,0.9)
   *        (7,4,1.0)
   *
   *    The matrix D :
   *    [ 0,0.8,0.6,0,0,0,0;
   *      0,0,0.9,0,0,0,0;
   *      0,0,0,0.3,0,0,0;
   *      0,0,0,0,0,0,0;
   *      0,0,0,0,0,0.4,0.8;
   *      0,0,0,0,0,0,0.9;
   *      0,0,0,1.0,0,0,0]
   */
  def main(args: Array[String]): Unit = {
    //    PropertyConfigurator.configure("src/log4j.properties");
    val conf: SparkConf = new SparkConf().setAppName("ICTGraphNMFTest").setMaster("local[2]")
    //    conf.set("log4j.configuration", "/Users/lifuxin/Program/GitLab/BDA/sparkml/ictgraph/log4j.properties")
    val sc: SparkContext = new SparkContext(conf)
    logDebug("info test felix")
    val users = sc.parallelize(Array(
      (1L, ("one")), (2L, ("Two")), (3L, ("Three")),
      (4L, ("Four")), (5L, ("Five")), (6L, ("Six")), (7L, ("Seven"))))
    val relationships = sc.parallelize(Array(
      Edge(1, 2, 0.8), Edge(1, 3, 0.6), Edge(2, 3, 0.9), Edge(3, 4, 0.3),
      Edge(5, 6, 0.4), Edge(5, 7, 0.8), Edge(6, 7, 0.9), Edge(7, 4, 1.0)))
    var graph = Graph(users, relationships)

    val reducedDim = 3
    val maxIteration = 1
    val theta = 0.01
    val lambda = 0.1

    //    val result = ICTGraphNMF.run(graph, maxIteration, theta, lambda, reducedDim)
    val result = ICTGraphNMF.runWithZero(graph, maxIteration, theta, lambda, reducedDim)

    //    println(result.vertices.collect().mkString("\n"))
    //    println(result.edges.collect().mkString("\n"))
    val debugfile = "debug.txt"
    val debugwrite = new PrintWriter(debugfile)
    val (arrayW, arrayH) = {
      val tmp = result.vertices.collect().sortWith((V1, V2) => V1._1 < V2._1)
      val w = tmp.map(elem => elem._2._1.elements)
      val h = tmp.map(elem => elem._2._2.elements)

      debugwrite.write("matrix W\n")
      for (elemArray: Array[Double] <- w) {
        for (elem <- elemArray)
          debugwrite.write(elem.toString + "\t")
        debugwrite.write("\n")
      }
      debugwrite.write("matrix H\n")
      for (elemArray: Array[Double] <- h) {
        for (elem <- elemArray)
          debugwrite.write(elem.toString + "\t")
        debugwrite.write("\n")
      }

      val W = w.reduce((vec1, vec2) => vec1 ++ vec2)
      val H = h.reduce((vec1, vec2) => vec1 ++ vec2)
      (W, H)
    }
    val rows = arrayW.length / reducedDim

    val matrixW = Matrices.dense(reducedDim, rows, arrayW).asInstanceOf[DenseMatrix]
    val matrixH = Matrices.dense(reducedDim, rows, arrayH).asInstanceOf[DenseMatrix]

    debugwrite.flush()
    debugwrite.close()
    //    val conf: SparkConf = new SparkConf().setAppName("IctNMF").setMaster("local")
    //
    //    //    val conf = new SparkConf().setAppName("IctNMF")
    //    val sc = new SparkContext(conf)
    //
    //    //    val desfile = "/home/liuwei/workspace/println.txt"
    //    //    val pw = new PrintWriter(desfile)
    //
    //    val verticesFile = "hdfs://s1:8020/user/root/input/vertices.txt"
    //
    //    //    val verticesFile = "file:///home/liuwei/workspace/v.txt"
    //    val verminPartitions = 2
    //
    //    val edgesFile = "hdfs://s1:8020/user/root/input/edges.txt"
    //    //    val edgesFile = "hdfs://s1:8020/user/root/input/v.txt"
    //    val edgminPartitions = 2
    //
    //    //load graph from file
    //    val loader = new IctGraphLoader(sc)
    //        val graph = loader.LoaderFile(verticesFile, verminPartitions, edgesFile, edgminPartitions)

  }

}