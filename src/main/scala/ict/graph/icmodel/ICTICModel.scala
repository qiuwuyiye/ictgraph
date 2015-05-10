package ict.graph.icmodel

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import scala.concurrent.duration._
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.SparkContext._
import ict.graph._
import akka.dispatch.Foreach

object ICTICModel extends Serializable with Logging {
  private var _numRand: Int = 0
  private val _rand = scala.util.Random

  def init[VD: ClassTag, ED: ClassTag](
    graph: Graph[VD, ED], seeds: RDD[(VertexId, Boolean)], numRand: Int): Graph[(Boolean, Boolean), ED] =
    {
      // Initialize the icmodel graph with activated seeds
      // the data of vertex is (status:Boolean, isSent:Boolean)
      var icgraph: Graph[(Boolean, Boolean), ED] = graph
        // Associate the outdegree with each vertex
        .outerJoinVertices(seeds) {
          (vid, vdata, seed) => (seed.getOrElse(false), false)
        }
      _numRand = numRand
      icgraph
    }

  //msgSum: whether any activated neighbor succeeds activating the vertex of VertexId.    
  def vertexProgram(id: VertexId, attr: (Boolean, Boolean), msgSum: Boolean): (Boolean, Boolean) = {
    if (msgSum == true && attr._1 == false) { //vertex is just activated
      (true, attr._2)
      //accumulator +1

    } else {
      attr
    }
  }

  def sendMessage(edge: EdgeTriplet[(Boolean, Boolean), Double]) = {
    //if src vertex is not activated, or 
    //if src vertex sent activation message before, or dst vertex is already activated.
    /*
	    if( edge.srcAttr._1 == false || edge.srcAttr._2 == true || edge.dstAttr._1 == true) {
	      Iterator.empty
	    }else {
	      if( _rand.nextDouble < edge.attr )
	        Iterator((edge.dstId, true))
	    }
	    * 
	    */

    if (edge.srcAttr._1 == true && edge.srcAttr._2 == false && edge.dstAttr._1 == false
      && _rand.nextDouble < edge.attr) {
      Iterator((edge.dstId, true))
    } else {
      Iterator.empty
    }

  }

  def messageCombiner(a: Boolean, b: Boolean): Boolean = a || b

  /**
   * return a proper sample of information cascade, and the average activation count by $numRand times
   */
  def runICModel1[VD: ClassTag](@transient sc: SparkContext)(
    graph: Graph[VD, Double], seeds: RDD[(VertexId, Boolean)], numRand: Int): (RDD[VertexId], Long) = {

    val initgraph = init(graph, seeds, numRand).cache
    val initalmessage: Boolean = false
    var tot: Long = 0
    var iternum = 0
    var procVertices: RDD[VertexId] = null
    val cntArray = new ArrayBuffer[Long]
    val vArray = new ArrayBuffer[RDD[VertexId]]
    while (iternum < _numRand) {
      procVertices = Pregel(initgraph, initalmessage, Int.MaxValue, activeDirection = EdgeDirection.Out)(
        vertexProgram,
        sendMessage,
        messageCombiner).vertices.filter(vertex => vertex._2._1).map(vertex => vertex._1)
      val cnt: Long = procVertices.count
      tot += cnt
      cntArray.append(cnt)
      vArray.append(procVertices)
      iternum += 1
    }
    //average activated number
    val ave = tot / _numRand

    (vArray(0), ave)

  }

  /**
   * return a vertex list with activation probability after $numRand samples,
   * and the average activation count by $numRand times
   *
   */
  def runICModel2[VD: ClassTag](@transient sc: SparkContext)(
    graph: Graph[VD, Double], seeds: RDD[(VertexId, Boolean)], numRand: Int): (RDD[(VertexId, Double)], Double) = {

    val initgraph = init(graph, seeds, numRand).cache
    val initalmessage: Boolean = false
    var iternum = 0
    var procVertices: RDD[VertexId] = null
    val cntArray = new ArrayBuffer[Long]
    val vArray = new ArrayBuffer[RDD[VertexId]]
    var re: RDD[(VertexId, Double)] = sc.parallelize(new Array[(VertexId, Double)](0), 2)

    while (iternum < numRand) {
      re = re ++ Pregel(initgraph, initalmessage, Int.MaxValue, activeDirection = EdgeDirection.Out)(
        vertexProgram,
        sendMessage,
        messageCombiner).vertices.filter(vertex => vertex._2._1).map(vertex => (vertex._1, 1.0))
      re = re.reduceByKey(_ + _)
      iternum += 1
    }
    //average activated number
    val tot: Double = re.map(v => v._2).sum()
    val ave = tot / numRand
    re = re.map(v => (v._1, v._2 / numRand))
    (re, ave)
  }

  /**
   * return a vertex list with activation probability after $numRand samples,
   * and the average activation count by $numRand times
   * This algorithm using Future for co-currency
   *
   * ==Example==
   * import scala.concurrent.duration._
   * import scala.concurrent.ExecutionContext.Implicits.global
   *
   * val tasks: Seq[Future[..]] = for (i <- 1 to 10) yield future {
   * //submit a spark job
   * }
   * val aggregated: Future[Seq[..]] = Future.sequence(tasks) // start running jobs
   * val squares: Seq[..] = Await.result(aggregated)  // wait until finish
   *
   */
  def runICModel3[VD: ClassTag](@transient sc: SparkContext)(
    graph: Graph[VD, Double], seeds: RDD[(VertexId, Boolean)], numRand: Int): (RDD[(VertexId, Double)], Double) = {

    val initgraph = init(graph, seeds, numRand).cache
    val initalmessage: Boolean = false
    var iternum = 0
    val vArray = new ArrayBuffer[RDD[VertexId]]
    val tasks: Seq[Future[RDD[VertexId]]] = for (i <- 1 to numRand) yield future {
      Pregel(initgraph, initalmessage, Int.MaxValue, activeDirection = EdgeDirection.Out)(
        vertexProgram,
        sendMessage,
        messageCombiner).vertices.filter(vertex => vertex._2._1).map(vertex => vertex._1)
    }

    val aggregated: Future[Seq[RDD[VertexId]]] = Future.sequence(tasks) //start running jobs
    //wait until finish
    val allresults: Seq[RDD[VertexId]] = Await.result(aggregated, Duration.Inf) //2 seconds
    initgraph.unpersistVertices(blocking=false)
    initgraph.edges.unpersist(blocking=false)
       
    val results: RDD[VertexId] = allresults.reduce(_ ++ _)
    var re: RDD[(VertexId, Double)] = results.groupBy(v => v)
      .map(viter => (viter._1, viter._2.count(x => true).toDouble))
    //average activated number
    val tot: Double = re.map(v => v._2).sum()
    val ave = tot / numRand
    re = re.map(v => (v._1, v._2 / numRand))
    (re, ave)
  }

}