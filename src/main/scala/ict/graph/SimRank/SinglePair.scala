package ict.graph.SimRank

import org.apache.spark.graphx._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark._
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer

class SinglePair {
	  def runSimRank(graph:Graph[Int,Double],K:Int,vertexA:Long,vertexB:Long):Double=
  {
	    //将图反转
	    val out=new PrintWriter("SinglePair.txt")
	    val C:Double=0.5
	    var TotalScore:Double = 0
	    var end:Boolean = false
	    val reverseGraph=graph.reverse.cache 
	    //第一次迭代
	    var Afollows:RDD[VertexId]=reverseGraph.edges
	    			.filter(edge=>edge.srcId == vertexA)
	    			.map(edges=>edges.dstId)
	    			
	    var Aoutdegree:Long=Afollows.count
	    var Bfollows:RDD[VertexId]=reverseGraph.edges
	    			.filter(edge=>edge.srcId==vertexB)
	    			.map(edges=>edges.dstId)
	
	    var Boutdegree:Long=Bfollows.count
	    var score:Double = 0
	    if(Aoutdegree == 0 || Boutdegree == 0){
	      end = true
	    }
	    else{
	      val conumber:Double=Afollows.intersection(Bfollows).count.toDouble
	    	score +=1*conumber/(Aoutdegree*Boutdegree)
	    }
	    
	    TotalScore +=score*C
	    out.println(TotalScore)
	    
	    //第二次迭代
	    var i =1
	    var Anodes:RDD[(VertexId,Double)]=Afollows.map(vertex=>(vertex,1.0/Aoutdegree))
	    out.println("Anodes:"+Anodes.collect.mkString(","))
	    var Anumbers:Long=0
	    var Bnodes:RDD[(VertexId,Double)]=Bfollows.map(vertex=>(vertex,1.0/Boutdegree))
	    out.println("Bnodes:"+Bnodes.collect.mkString(","))
	    var Bnumbers:Long=0
	    //每个数据项纪录的是源节点，后继节点，源节点概率
	    var Afollower:RDD[(VertexId,(VertexId,Double))]=null
	    var Bfollower:RDD[(VertexId,(VertexId,Double))]=null
//	    val co:RDD[(VertexId,(Double,Double))]=Anodes.join(Bnodes)
	    val iterateA:ArrayBuffer[RDD[(VertexId,Double)]]=new ArrayBuffer()
//	    iterateA.append(co.map(v=>(v._1,v._2._1)))
	    iterateA.append(Anodes)
	    val iterateB:ArrayBuffer[RDD[(VertexId,Double)]]=new ArrayBuffer()
//	    iterateB.append(co.map(v=>(v._1,v._2._2)))
	    iterateB.append(Bnodes)
	    while(i<K && (!end)){
	      score = 0
//	      Anodes=Anodes.subtractByKey(Bnodes)
//	      Bnodes=Bnodes.subtractByKey(Anodes)
	      
	      Anumbers=Anodes.count
	      Bnumbers=Bnodes.count
	      
	      if(Anumbers == 0 ||Bnumbers == 0){
	        end = true
	      }
	      else{
	        val tempa:RDD[(Long,(Int,Double))]=Anodes.map(vertex=>(vertex._1,(1,vertex._2)))
	        out.println("tempa"+tempa.collect.mkString(","))
	        val oldAfollower = Afollower
	        Afollower=reverseGraph
	        	.outerJoinVertices(tempa)((vertex,old,newopt)=>newopt.getOrElse((0,0.0)))
	        	.triplets
	        	.filter(triplet=>triplet.srcAttr._1 == 1)
	        	.map(triplet=>(triplet.srcId,(triplet.dstId,triplet.srcAttr._2))).cache
	        //每个源节点对应的分支个数
	        val statistica:RDD[(Long,Int)]=Afollower.groupBy(elem=>elem._1).map(elem=>(elem._1,elem._2.size))
	        
	        Anodes=Afollower
	        	.join(statistica).map(elem=>(elem._2._1._1,elem._2._1._2/elem._2._2))
	        	.reduceByKey((pro1,pro2)=>pro1+pro2).cache
	        out.println("Afollower"+Afollower.collect.mkString(",")+"\n"+statistica.collect.mkString(",")+"\n"+Anodes.collect.mkString(","))
	        Aoutdegree=Anodes.count

	        val tempb:RDD[(Long,(Int,Double))]=Bnodes.map(vertex=>(vertex._1,(1,vertex._2)))
	        
	        val oldBfollower = Bfollower
	        Bfollower=reverseGraph
	        	.outerJoinVertices(tempb)((vertex,old,newopt)=>newopt.getOrElse((0,0.0)))
	        	.triplets
	        	.filter(triplet=>triplet.srcAttr._1 == 1)
	        	.map(triplet=>(triplet.srcId,(triplet.dstId,triplet.srcAttr._2))).cache
	        //每个源节点对应的分支个数
	        val statisticb:RDD[(Long,Int)]=Bfollower.groupBy(elem=>elem._1).map(elem=>(elem._1,elem._2.size))
	        
	        //每个后继节点对应的分数
	        Bnodes=Bfollower
	        	.join(statisticb).map(elem=>(elem._2._1._1,elem._2._1._2/elem._2._2))
	        	.reduceByKey((pro1,pro2)=>pro1+pro2).cache
	        
	        Boutdegree=Bnodes.count
/*	        val tempb:RDD[(Long,Int)]=Bnodes.map(vertex=>(vertex,1))
	        Bfollows=reverseGraph
	        	.outerJoinVertices(tempb)((vertex,old,newopt)=>newopt.getOrElse(0))
	        	.triplets
	        	.filter(triplet=>triplet.srcAttr == 1).map(triplet=>triplet.dstId)
	        Boutdegree=Bfollows.count
	        * 
	        */

	        if(Aoutdegree == 0 ||Boutdegree == 0){
	          end = true
	        }
	        else{
	        	val conodes=Anodes
	        			.join(Bnodes)
	        			.map(v=>(v._2._1*v._2._2)).cache
	        	out.println("Anodes:"+Anodes.collect.mkString(",")+"a ok and b:\n"+Bnodes.collect.mkString(",")+conodes.collect.mkString(","))
	        	val conumber:Double =if(conodes.count>0)conodes.sum else 0
	        	TotalScore +=conumber*doublepow(C,i+1)
	        	conodes.unpersist(blocking=false)
	        }	        
	        if(i>1){
	          oldAfollower.unpersist(blocking=false)
	          oldBfollower.unpersist(blocking=false)
	        }
	      }
	      i +=1
	    }
	    out.close()
	    TotalScore
  }
	 def doublepow(a:Double,K:Int):Double={
	   var result:Double=1
	   var i = 0
	   while(i<K){
	     result *=a
	     i +=1
	   }
	   result
	 }
}