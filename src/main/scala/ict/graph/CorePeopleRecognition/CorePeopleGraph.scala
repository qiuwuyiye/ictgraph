package ict.graph.CorePeopleRecognition
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.util.Date;
import java.text.SimpleDateFormat;


class CorePeopleGraph(@transient sc: SparkContext) extends Serializable{
/**
 * Provides utilities for loading [[Graph]]s from files.
 */
  private var coreGraph:Graph[Int,Long]=null
  
  //根据边信息构造图
  def LoaderFile(
      edgesFile:String,
      edgminPartitions:Int)
  :Graph[Int,Long]=
  {
	  //去读包含点属性的边文件
    
     val edgesInfo:RDD[String]=sc.textFile(edgesFile,edgminPartitions)
    	  									
	  val edges:RDD[Edge[Long]]=edgesInfo.map(edges=>{val temp = edges.split("\t")
	    								   Edge(temp(0).toLong,
	    								       temp(1).toLong,
	    								       new Date(temp(2).toString()).getTime())})
	    								       
	 val vertices:RDD[(VertexId,Int)]=edgesInfo.flatMap(edges=>{val temp = edges.split("\t")
		  					Array(
			  				(temp(0).toLong,temp(3).toInt),
			  				(temp(1).toLong,temp(4).toInt)
			  				)})
			  				
	 coreGraph=Graph(vertices,edges).cache
	 coreGraph
//	 vertices.unpersist(true)
//	 edges.unpersist(true)
	 
  }
  
  //根据输入的节点集合，对图进行节点过滤
  def filterGraph(
      vertexFile:String,
      verminPartitions:Int
		  )
  :RDD[String]={
    //读取待过滤的点集合
    val vertexfile=sc.textFile(vertexFile, verminPartitions)
    val vertex:RDD[(VertexId,Int)]=vertexfile.map(line=>(line.toLong,1))
    coreGraph=coreGraph
    .outerJoinVertices(vertex)((id,oldv,newv)=>if(newv.isEmpty) Int.MaxValue else oldv)
    .subgraph(vpred=(vertex,v)=>v != Int.MaxValue)
    
    coreGraph.triplets.map(triplet=>(triplet.srcId+"\t"+
        triplet.dstId+"\t"+
        {val sdfnew=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
        sdfnew.format(new Date(triplet.attr))}+"\t"+
        triplet.srcAttr+"\t"+
        triplet.dstAttr))	
  }
  //在图中找出信息源节点，并返回其节点ID
  def SourceNode():Long={
		  val sortEdges:RDD[Edge[Long]]=coreGraph.edges.sortBy(edge=>edge.attr, true, coreGraph.edges.partitions.size).cache
		  sortEdges.first.srcId
  }
  
  
  //在图中找到出度最大的Top K节点，并返回其节点ID和出度
  def MaxOutDegreeNode(K:Int):RDD[(VertexId,Int)]={
    val outdegree:VertexRDD[Int]=coreGraph.outDegrees
    val sortOutDegree:RDD[(VertexId,Int)]=outdegree.sortBy(v=>v._2,false,outdegree.partitions.size)
    sc.parallelize(sortOutDegree.take(K),1)
  }
  
  
  //在图中找到入度最大的Top K节点，并返回其节点ID和入度
  def MaxInDegreeNode(K:Int):RDD[(VertexId,Int)]={
    val indegree:VertexRDD[Int]=coreGraph.inDegrees
    val sortInDegree:RDD[(VertexId,Int)]=indegree.sortBy(v=>v._2,false,indegree.partitions.size)
    sc.parallelize(sortInDegree.take(K),1)
  }
  
  
  //在图中找到最早跨区域的Top K关键节点，并返回其节点ID
  def TransRegionalNode(K:Int):RDD[(Long,String)]={
    //过滤出跨区域的边
    val trip:RDD[EdgeTriplet[Int,Long]]=coreGraph.triplets.filter(triplet=>triplet.srcAttr!=triplet.dstAttr)
    
    //将triplet转换为VertexRDD,其中VertexId为起始点ID，点的属性为时间，对于多次出现的同一个srcId,取时间最小的那个点
    val edges:RDD[(VertexId,Long)]=trip.map(triplet=>(triplet.srcId,triplet.attr)).groupByKey.map(v=>(v._1,v._2.min))
    //将这些边按时间进行排序
    val sortEdges:RDD[(VertexId,String)]=edges.sortBy(v=>v._2, true, edges.partitions.size).map(vertex=>(vertex._1,{val sdfnew=new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    							sdfnew.format(new Date(vertex._2))})).cache
    edges.unpersist(blocking=false)							
    //输出TOPK的跨区域边的传播定点以及传播时间
    sc.parallelize(sortEdges.take(K),1)     
  }
  

  //在图中找到Top K关键区域（包含节点个数最多的区域，并返回其区域ID）
  def CriticalRegion(K:Int):RDD[(Int,Int)]={
	val vertex:VertexRDD[Int]=coreGraph.vertices
	//计算每个区域包含的节点个数
    val region:RDD[(Int,Int)]=vertex.map(v=>(v._2,1)).reduceByKey((a,b)=>a+b)
    //按包含的节点个数由大到小排序
    val sortRegion:RDD[(Int,Int)]=region.sortBy(v=>v._2,false, region.partitions.size)
    region.unpersist(blocking=false)
    sc.parallelize(sortRegion.take(K),1)
  }
  
  //以输入节点为源的出度最大的top K节点，并返回其节点ID和出度
  def KeySpreadNode(K:Int,Source:Long)={
    val OutDegree:VertexRDD[Int]=coreGraph.outDegrees
    
    val initialGraph:Graph[Double,Int]=coreGraph.mapVertices((id,_)=>if(id == Source)0L else Double.PositiveInfinity).mapEdges(e=>1)
    //迭代计算，过程类似pregel
    val initialMsg=Double.PositiveInfinity //每个顶点接受的初始消息
    val vprog={(id:VertexId, dist:Double, newDist:Double) => math.min(dist, newDist)}
    val sendMsg={triplet:EdgeTriplet[Double,Int] => {  // Send Message
    			if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
    				Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
    			} else {
    				Iterator.empty
    			}
    		}}
    val mergeMsg={(a:Double,b:Double) => math.min(a,b)}
    val maxIterations=Int.MaxValue
    val activeDirection=EdgeDirection.Either
    var vnum:Int=0
    
    var g = initialGraph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache() 
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()
    // Loop
    var prevG: Graph[Double, Int] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations && vnum < K) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
      vnum +=newVerts.innerJoin(OutDegree)((v,oldv,newv)=>oldv).count.toInt
      // Update the graph with the new vertices.
      prevG = g
      g = g.outerJoinVertices(newVerts) { (vid, old, newOpt) => newOpt.getOrElse(old) }
      g.cache()

      val oldMessages = messages
      // Send new messages. Vertices that didn't get any messages don't appear in newVerts, so don't
      // get to send messages. We must cache messages so it can be materialized on the next line,
      // allowing us to uncache the previous iteration.
      messages = g.mapReduceTriplets(sendMsg, mergeMsg, Some((newVerts, activeDirection))).cache()
      // The call to count() materializes `messages`, `newVerts`, and the vertices of `g`. This
      // hides oldMessages (depended on by newVerts), newVerts (depended on by messages), and the
      // vertices of prevG (depended on by newVerts, oldMessages, and the vertices of g).
      activeMessages = messages.count()

      System.out.println("Pregel finished iteration " + i)

      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
      // count the iteration
      i += 1
    }
    val ConnectVertex:VertexRDD[Double]=g.vertices.filter(v=>v._2!=Double.PositiveInfinity && v._2!=0.0)
    val topK:RDD[(VertexId,(Double,Int))]=OutDegree.innerJoin(ConnectVertex)((v,a,b)=>(i+1-b,a)).sortBy(v=>v._2, false, OutDegree.partitions.size)
    sc.parallelize(topK.take(K),1)
  }
}