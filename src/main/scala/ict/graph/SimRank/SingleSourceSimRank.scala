package ict.graph.SimRank

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import scala.reflect.ClassTag
import org.apache.spark.SparkContext
import org.apache.spark.Logging
import java.io.PrintWriter
import scala.collection.mutable.HashMap
class SingleSourceSimRank() extends serializable{
//  val out = new PrintWriter("estimate_process.txt")
  def runSimRank(graph: Graph[Double, Double], Source: Long, K: Int,C:Double) =
    {
      //初始化新图
      //C为衰减系数
      val g = graph.cache
      val indegree = g.inDegrees.map(v => (v._1, 1.0 / v._2)).cache
      /*   val newnewgraph=g.outerJoinVertices(indegree)((id,old,newopt)=>newopt.getOrElse(0.0))
    					 .mapTriplets(triplet=>triplet.dstAttr)
    					 .outerJoinVertices(graph.vertices)((id,old,newopt)=>newopt.getOrElse(0.0))
    					 * 
    					 */

      //生成一个新的graph，vertex的属性值分别为（初始对角矩阵对应的值，x，temp，simscore=0）
      val newgraph: Graph[(Double, Double, Double, Double), Double] =
        g.outerJoinVertices(indegree)((id, old, newopt) => newopt.getOrElse(0.0))
          .mapTriplets(triplet => triplet.dstAttr)
          .joinVertices(g.vertices)((id, old, newopt) => newopt)
          .mapVertices((vertex, value) => {
            if (vertex == Source)
              (value, 1.0, value, 0.0)
            else
              (value, 0.0, 0.0, 0.0)
          }).cache
      var i = 0
      var graphx: Graph[(Double, Double, Double, Double), Double] =newgraph
      
      indegree.unpersist(blocking=false)
      g.unpersistVertices(blocking = false)
      g.edges.unpersist(blocking=false)
      
      val vprog = { (id: VertexId, attr: (Double, Double, Double, Double), newScore: Option[Double]) => (attr._1, attr._2, newScore.getOrElse(0.0), attr._4)
      }

      //消息传递函数
      val sendMsg1 = { triplet: EdgeTriplet[(Double, Double, Double, Double), Double] =>
        { // Send Message
          if (triplet.dstAttr._3 > 0) {
            //    				println((triplet.srcId,triplet.dstAttr._3*triplet.attr))
            Iterator((triplet.srcId, triplet.dstAttr._3 * triplet.attr))

          } else {
            Iterator.empty
          }
        }
      }

      val sendMsg2 = { triplet: EdgeTriplet[(Double, Double, Double, Double), Double] =>
        { // Send Message
          if (triplet.srcAttr._3 > 0) {
            //    				println((triplet.srcId,triplet.dstAttr._3*triplet.attr))
            Iterator((triplet.dstId, triplet.srcAttr._3 * triplet.attr))

          } else {
            Iterator.empty
          }
        }
      }
      //消息合并函数
      val mergeMsg = { (a: Double, b: Double) => a + b }
      while (i < K) {
        val oldgraphx = graphx
        //    	graphx=simrankPregel(graphx,1)(vprog,sendMsg,mergeMsg)
        //    			.mapVertices((vertex,value)=>(value._1,value._3,value._3,value._4)).cache
        //    	val oldgraphx2=graphx
 //       graphx = graphx.mapVertices((vertexid, vd) => (vd._1, vd._2, vd._1 * vd._2, vd._4))

        val ratio: Double = doublepow(C, i)
        graphx = simrankPregel(graphx, i)(vprog, sendMsg2, mergeMsg)
          .mapVertices((vertex, value) => (value._1, value._2, value._2, value._4 + value._3 * ratio)).cache

        val oldgraphx2 = graphx
        graphx = simrankPregel(graphx, 1)(vprog, sendMsg1, mergeMsg)
          .mapVertices((vertexid, vd) => (vd._1, vd._3, vd._1*vd._3, vd._4)).cache

        oldgraphx.unpersistVertices(blocking = false)
        oldgraphx2.unpersistVertices(blocking = false)
        i += 1
      }

      graphx.vertices.map(v => (v._1, v._2._4))
    }

  /*
   * 专为simrank打造的pregel
   * 去除了初始消息这一步
   * 在更新定点信息的时候，如果接受到消息，接受新消息，如果没有，则将temp值置为0
   */
  def simrankPregel(graph: Graph[(Double, Double, Double, Double), Double],
    maxIterations: Int = Int.MaxValue)(vprog: (VertexId, (Double, Double, Double, Double), Option[Double]) => (Double, Double, Double, Double),
      sendMsg: EdgeTriplet[(Double, Double, Double, Double), Double] => Iterator[(VertexId, Double)],
      mergeMsg: (Double, Double) => Double): Graph[(Double, Double, Double, Double), Double] =
    {
      var g = graph.cache()
      // compute the messages
      var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
      //    var activeMessages = messages.count()
      // Loop
      var prevG: Graph[(Double, Double, Double, Double), Double] = null
      var i = 0
      while (i < maxIterations) {
        // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
        //      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
        // Update the graph with the new vertices.
        prevG = g
        //      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old._1,old._2,0,old._4) }
        //     g = g.outerJoinVertices(messages) { 
        //   	(vid, old, newOpt) => (old._1,old._2,newOpt.getOrElse(0),old._4) }
        g = g.outerJoinVertices(messages)(vprog)
        g.cache()
        val oldMessages = messages
        // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
        // get to send messages. We must cache messages so it can be materialized on the next line,
        // allowing us to uncache the previous iteration.
        messages = g.mapReduceTriplets(sendMsg, mergeMsg).cache()
        // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
        // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
        // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
        //      activeMessages = messages.count()

        // Unpersist the RDDs hidden by newly-materialized RDDs
        oldMessages.unpersist(blocking = false)
        //      newVerts.unpersist(blocking=false)
        prevG.unpersistVertices(blocking = false)
        prevG.edges.unpersist(blocking = false)
        // count the iteration
        i += 1
      }

      g
    } // end of apply
  def preprocessing(graph: Graph[Int, Double], L: Int, T: Int, R: Int,C:Double): Map[VertexId, Double] = {

    var iteration: Int = 1
    val vertexNum: Long = graph.vertices.count

    val initialScore:RDD[(VertexId,Double)]=initialD(graph,C)
    var mapVertices: Map[VertexId, Double] = initialScore.toArray.toMap
    
    var g :Graph[Double,Double]=graph
    					.outerJoinVertices(initialScore)((vertexid,old,newopt)=>newopt.getOrElse(0.0))
    					.cache
    
/*    var mapD: HashMap[VertexId, Double] = HashMap[VertexId, Double]()
    for (k <- mapVertices) {
      mapD += (k)
    }
    * 
    */
    val mapInlinks: Map[VertexId, Array[VertexId]] =
      graph.edges
        .map(edges => (edges.dstId, Array(edges.srcId)))
        .reduceByKey((a: Array[VertexId], b: Array[VertexId]) => a ++ b)
        .toArray.toMap[VertexId, Array[VertexId]]
    
    
   val doublepow=(a: Double, K: Int)=>{
    var result: Double = 1
    var i = 0
    while (i < K) {
      result *= a
      i += 1
    }
    result
  }
    
//estimateL start   
    val estimateSL=(
    Dscore: Map[VertexId, Double],
    vertexid: VertexId,
    T: Int,
    R: Int,
    C: Double,
    mapInlinks: Map[VertexId, Array[VertexId]])=>{
    //      val g :Graph[Double,Double]=graph.cache
    var i: Int = 0
    var samples: Array[VertexId] = new Array(R)
    var a: Double = 0
    var b: Double = 0
    //      val DDscore=Dscore.collect.toMap[VertexId,Double]
    val random = scala.util.Random
    //初始化随机数组
    for (r <- 0 until (R)) {
      samples(r) = vertexid
    }
    while (i < T) {
      val mapSample = new scala.collection.mutable.HashMap[VertexId, Int]()
      for (vertexid <- samples) {
        if (mapSample.contains(vertexid)) mapSample(vertexid) = mapSample(vertexid) + 1
        else mapSample += ((vertexid, 1))
      }

      for (dstId <- mapSample) {
        //          val scoreArray:Array[Double] = Dscore.filter(v=>v._1 == dstId._1).map(v=>v._2).collect
        //          val score:Double = if(scoreArray.length>0) scoreArray(0) else 0
        val score: Double = if (Dscore.contains(dstId._1)) Dscore(dstId._1) else 0
        val p: Double = dstId._2 * 1.0 / R
        val ratio = doublepow(C, i) * p * p
        if (dstId._1 == vertexid)
          a += ratio
        b += ratio * score
      }

      //设置R个随机样本的下一个Step所在的vertexId
      for (j <- 0 until (R)) {
        val next: Array[VertexId] = if (mapInlinks.contains(samples(j))) mapInlinks(samples(j)) else Array(0)
        val randomWalk: VertexId = if (next.length > 0) next(scala.math.floor(scala.math.random * next.length).toInt)
        else -1
        samples.update(j, randomWalk)
        samples(j) = randomWalk
      }
      //        g.unpersistVertices(blocking=false)
      i += 1
    }
//    println("vertex: " + vertexid + " : " + " a:" + a + "  b:" + b + ";" + (1.0 - b) / a)
    (1.0 - b) / a
  }
    
//estimate end
  
    
    while (iteration <= L) {
      //        var tempD:Map[VertexId,Double]=mapD
/*      val tempD: HashMap[VertexId, Double] = mapD
      var just_mark:Int = 0
      for (k <- tempD) {
        just_mark +=1
        println(just_mark+"  it's " + k + " iteration")
        val p = estimateSL(mapD, k._1, T, R, C, mapInlinks)
        //        	tempD=tempD.map(v=>{if(v._1 == k._1)(v._1,p+v._2) else v})
        tempD(k._1) = tempD(k._1) + p
        //        	println(k+":"+ramdom.nextPrintableChar)
      }
      mapD = tempD
      * 
      */
//     out.println("\niteration " + iteration + " result:")
//     out.println(mapD.map(v=>v._1+"\t"+v._2).mkString("\n"))
//     out.println()

        val oldg = g
        g=g.mapVertices((vertexid,value)=>{
 //       		println(vertexid+";"+value)
        		value+estimateSL(mapVertices,vertexid,T,R,C,mapInlinks)
        }).cache
        mapVertices=g.vertices.toArray.toMap
        
        oldg.unpersistVertices(blocking=false)
        
      iteration += 1
    }
//    out.close()
    mapVertices
  }
  
  //初始化对角线矩阵D的值
  def initialD(graph: Graph[Int, Double],C:Double): RDD[(VertexId,Double)] = {
    val g:Graph[Int,Double]=graph
    val indegree = g.inDegrees.map(v => (v._1, 1.0 / v._2))
    val temp:RDD[(VertexId,Double)]=g
      					.outerJoinVertices(indegree)((id, old, newopt) => newopt.getOrElse(0.0))
      					.mapTriplets(triplet => triplet.dstAttr)
      					.triplets
      					.map(triplet=>(triplet.dstId,triplet.attr))
      					.reduceByKey((a,b)=>a*a+b*b)
    val mapVertices:RDD[(VertexId,Double)]=g
    						.vertices
    						.leftJoin(temp)((vertexid,old,newopt)=>1.0-C*newopt.getOrElse(0.0))
   
    mapVertices
  }

  //计算a的k次方
  def doublepow(a: Double, K: Int): Double = {

    var result: Double = 1
    var i = 0
    while (i < K) {
      result *= a
      i += 1
    }
    result
  }
}