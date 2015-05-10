package ict.graph.SimRank

import ict.graph.common._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx._

object ictSimRankTest {
	def main(args:Array[String]){
	  val conf=new SparkConf().setAppName("ictSimRankTest").setMaster("local[2]")
	  val sc = new SparkContext(conf)
	  
	  
/*	  	val parser = new scopt.OptionParser[ParaConfig]("RunSimRank") {
      head("RunSimRank", "1.0")
      opt[String]("edgesfile") required () action { (x, c) =>
        c.copy(edgesfile = x)
      } text ("edgesfile is the input file that includes the graph edges infomation")
      opt[String]("vertexfile") required () action { (x, c) =>
        c.copy(vertexfile = x)
      } text ("vertexfile is the input file that includes the vertex that needs to be kept")
      opt[String]("outpath") required () action { (x, c) =>
        c.copy(outpath = x)
      } text ("outpath is the output path")
      opt[Int]("partitions") optional () action { (x, c) =>
        c.copy(partitions = x)
      } text ("partitions is the min number of RDD's split parts,default is 2")
      opt[Int]("topk") optional () action { (x, c) =>
        c.copy(topk = x)
      } text ("topk is the number of vertex wanted to be output,default is 10")
      opt[Long]("startnode") required () action { (x, c) =>
        c.copy(startnode = x)
      } text ("startnode is the VertexId that be calculated the simrank Score with other vertices")
    }
    // parser.parse returns Option[Config]
	  val para= parser.parse(args, ParaConfig()).get
	  * 
	  */
	   val vertexfile:String="vertices3.txt"
//	   val vertexfile:String=para.vertexfile
	   
//	   val verminpartition:Int=para.partitions
	   val verminpartition:Int=10
	   val edgefile:String="edges5.txt"	     
//	   val edgefile:String=para.edgesfile
	   val edgeminpartition:Int=4
//	   val outpath:String=if(args.count(p=>true)>4) args(4).toString() else "hdfs://s1:8020/user/root/caoqi/SimRank/result"
	   val time1 = System.currentTimeMillis()
	   
	   val graph = new IctGraphLoader(sc).LoaderFile(vertexfile, verminpartition, edgefile, edgeminpartition).cache
/*(1,0.4759131852693175)
(2,0.4759131852693175)
(3,0.4759131852693175)
(4,0.7596632006466224)
(5,0.22991425205103283)

5次
(1,0.5644913886509996)
(2,0.5644913886509996)
(3,0.5644913886509996)
(4,0.7447253987158786)
(5,0.4317881924361201)

4次
(1,0.079200378654676)
(2,0.079200378654676)
(3,0.079200378654676)
(4,0.4859814763119308)
(5,0.09687298747269424)
* 
* 
10次
2 -> 0.31167432062360945
5 -> 0.24965819430103922
4 -> 0.6215123323963034
1 -> 0.25515692700121795
3 -> 0.30403327281276604
*/   
/*样本5	
 * 
 * iteration 5 result:
(1,0.7732402538535944)
(2,0.7484333200068617)
(3,0.3937492892564289)
(4,0.7800711055793201)
(5,0.5473971257385003)
* 
* 
* iteration 10 result:
(1,0.49261924342655483)
(2,0.423627683041367)
(3,0.16609349632366394)
(4,0.5541761380621524)
(5,0.1488029440048996)

iteration6
2 -> 0.47752680353726984
5 -> 0.26902528647159957
4 -> 0.5930032662282637
1 -> 0.5548277653857612
3 -> 0.2540790680171892
*/
	   val K:Int=5
	   val simrank=new SingleSourceSimRank()
/*	   val newgraph:Graph[Double,Double] = graph.mapVertices((vertex,value)=>
	     				{if(vertex == 1) 0.25515692700121795
	     				else if (vertex == 2) 0.31167432062360945
	     				else if (vertex == 3) 0.30403327281276604
	     				else if (vertex == 4) 0.6215123323963034
	     				else 0.24965819430103922})
	     				* 
	     				*/

	   val newgraph:Graph[Double,Double] = graph.mapVertices((vertex,value)=>0.2)
//	   val Source:Long=1197161814L
//	   val Source:Long=1756881315L
//	   val Source:Long=2918753171L
//	   val Source:Long=3063067615L 
//	   val Source:Long=para.startnode
	   val Source:Long=1
	   val result = simrank.runSimRank(newgraph,Source,5,0.8)
//	   val result=simrank.runSingleSource(newgraph,Source,5,0.8)
//	   val result = simrank.runSimRank(graph,5, 1197161814, 1197033464) //入度最大，500
//	   val result2 = simrank.runSimRank(graph,5, 1666693283, 1820895513)
//	   val result = simrank.runSimRank(graph,5, 1, 2)
/*	   val a=sc.parallelize(Array(((1L,3L),1),((2L,7L),2),((3L,5L),3)), 2)
	   val b= sc.parallelize(Array(((1L,3L),5),((1L,3L),7),((2L,5L),7),((4L,7L),8)), 2)
	   val c = a.leftOuterJoin(b).map(v=>(v._1,v._2._2.getOrElse(v._2._1)))
	   c.foreach(println)
*/
/*   val a = sc.parallelize(Array((4,1.2),(5,5.3),(6,2.1)), 2)
	   val b = sc.parallelize(Array((4,2.2),(5,8.3),(7,1.1)), 2)
	   a.subtract(b).foreach(println)
	   println("a ok and b:")
	   a.subtractByKey(b).foreach(println)
	   * 
	   * 
	   */
//	   simrank.preprocessing(graph, 5, 5, 1000,0.8)

/*	    val random = scala.math.random
	    for(i<-0 until(20))
	    println(scala.math.floor(scala.math.random*10).toInt)
	    * 
	    */
	   
//	   println(graph.vertices.count+":"+graph.edges.count)
	   val resultsort=result.sortBy(v=>v._2, false, 10)
	   println(resultsort.take(100).mkString("\n"))
//	   resultsort.saveAsTextFile(para.outpath)
//	   println(result.size)
/*	   val D:RDD[(VertexId,Double)]=sc.textFile("estimateD.txt", 4)
	   									.map(line=>{
	   										val parts = line.split("\t")
	   										(parts(0).toLong,parts(1).toDouble)
	   									})
	   val newgraph:Graph[Double,Double] = graph.outerJoinVertices(D)((vertex,old,newopt)=>newopt.getOrElse(0))	   	   
	   val query:RDD[(VertexId,Int)]=sc.textFile("query.txt", 1).map(line=>{
		   													val parts=line.split("\t")
		   													(parts(0).toLong,parts(1).toInt)
	   })
	   val queryResult:ArrayBuffer[String]=new ArrayBuffer()
	   for(q<-query.toArray){
	     val result:RDD[(VertexId,Double)] =simrank.runSimRank(newgraph,q._1,5,0.8)
	     val resultsort=result.sortBy(v=>v._2, false, verminpartition)
	     queryResult += (q._1+"\t"+q._2+"\n"+resultsort.take(q._2).mkString("\n")+"\n")
	   }
	   * 
	   */


//	  val queryResult = simrank.runSimRank(newgraph,2918753171L,5,0.8).sortBy(v=>v._2, false, 1).take(1000)
//	  val out=new PrintWriter("simrankResult.txt")
//	  out.println(queryResult.mkString("\n"))
//	  out.close()




	   val time2 = System.currentTimeMillis()
	   println("time:"+(time2-time1))
	   
	   
	}
}