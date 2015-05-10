package ict.graph.NMF
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import ict.graph.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.io.PrintWriter
import java.util.concurrent.TimeUnit
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext._
import java.io.File
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.{ SerializableWritable, SparkConf, SparkContext }
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
object LinkPredictionhdfs {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LinkPredictionhdfs")
    val sc = new SparkContext(conf)

    // parse arguments
    val parser = new scopt.OptionParser[ParaConfig]("LinkPredictionhdfs") {
      head("LinkPredictionhdfs", "1.0")
      opt[String]("edgesfile") required () action { (x, c) =>
        c.copy(edgesfile = x)
      } text ("edgesfile is the input file that includes the graph edges infomation")
      opt[Int]("partitions") optional () action { (x, c) =>
        c.copy(partitions = x)
      } validate { x => if (x > 0) success else failure("Option --partitions must be >0")
      } text ("partitions is the min number of RDD's split parts,default is 2")
      opt[Int]("R") optional () action { (x, c) =>
        c.copy(R = x)
      } validate { x => if (x > 0) success else failure("Option --R must be >0")
      } text ("R is the top R users in a community,default is 100")
      opt[Int]("reducedDim") required () action { (x, c) =>
        c.copy(reducedDim = x)
      } validate { x => if (x > 0) success else failure("Option --reducedDim must be >0")
      } text ("reducedDim is the reduced Dim in NMF")
      opt[Int]("maxIteration") required () action { (x, c) =>
        c.copy(maxIteration = x)
      } validate { x => if (x > 0) success else failure("Option --maxIteration must be >0")
      } text ("maxIteration is the max iteration of NMF")
      opt[Double]("theta") required () action { (x, c) =>
        c.copy(theta = x)
      } validate { x => if (x > 0.0) success else failure("Option --theta must be >0.0")
      } text ("theta is the stepsize  of NMF")
      opt[Double]("lambda") required () action { (x, c) =>
        c.copy(lambda = x)
      } validate { x => if (x > 0.0) success else failure("Option --lambda must be >0.0")
      } text ("lambda is the normalization item  of NMF")
      opt[String]("output") required () action { (x, c) =>
        c.copy(output = x)
      } text ("output is the output file")
      opt[Int]("withZeroItems") optional () action { (x, c) =>
        c.copy(withZeroItems = x)
      } validate { x => if (x == 0 || x == 1) success else failure("Option --withZeroItems must be 0 or 1")
      } text ("withZeroItems is the choice you want run NMF Graph with zero or not")
      opt[Int]("WorH") optional () action { (x, c) =>
        c.copy(WorH = x)
      } validate { x => if (x == 0 || x == 1) success else failure("Option --WorH must be 0 or 1")
      } text ("WorH is the choice you want to get the community from W or H")
    }
    // parser.parse returns Option[Config]
    val para = parser.parse(args, ParaConfig()).get

    val edgesFile: String = para.edgesfile
    val edgminPartitions: Int = para.partitions
    val reducedDim: Int = para.reducedDim
    val maxIteration: Int = para.maxIteration
    val theta: Double = para.theta
    val lambda: Double = para.lambda
    val output: String = para.output
    val R : Int = para.R
    val withZeroItems: Int = para.withZeroItems
    val WorH: Int = para.WorH
    val loader = new IctGraphLoader(sc)
    val graph: Graph[Int, Double] = loader.LoaderEdgeFile(edgesFile, edgminPartitions)
    val result = withZeroItems match {
      case 0 => ICTGraphNMF.run(graph, maxIteration, theta, lambda, reducedDim)
      case 1 => ICTGraphNMF.runWithZero(graph, maxIteration, theta, lambda, reducedDim)
    }
   val link_result=linkprediction_approach(result).take(R)
   sc.parallelize(link_result,1).saveAsTextFile(output)
   sc.stop()
   System.exit(0)
  }
     def linkprediction_approach(
	    graph:Graph[(org.apache.spark.util.Vector,org.apache.spark.util.Vector),Double])={
	  
	  //in_links relationship
     val g=graph.mapTriplets(triplet=>{
    	  			var value:Double=0
    	  			val len=triplet.srcAttr._1.length
    	  			val v1=triplet.srcAttr._1
        			val v2=triplet.dstAttr._2
    	  			var i :Int=0
    	  			while (i<len){
        				value +=v1.apply(i)*v2.apply(i)
        				i +=1    	  			  
    	  			}
    	  			value
    	  				
      }).cache

//      val average:Double=g.edges.map(edges=>edges.attr).sum/g.edges.count
//      val filteredge=g.edges.filter(edge=>edge.attr>average).cache
//      println(average)
//      : RDD[(String, Array[String])] 
	  val mapInlinks=
      g.edges
        .map(edges => (edges.dstId.toString, (edges.srcId.toString,edges.attr.toDouble)))
        .groupByKey.map(v=>{val ins:Iterable[(String, Double)]=v._2
          			val result=ins.toSeq.sortBy(vv=>vv._2).take(10)
          			(v._1,result.map(vv=>vv._1).toArray)
        })
	  
       val mapIn=mapInlinks.toArray.toMap
        
	 //out_links relationship 
	 val mapOutlinks: RDD[(String, Array[String])] =
      g.edges
        .map(edges => (edges.srcId.toString, (edges.dstId.toString,edges.attr.toDouble)))
        .groupByKey.map(v=>{val ins:Iterable[(String, Double)]=v._2
          			val result=ins.toSeq.sortBy(vv=>vv._2).take(10)
          			(v._1,result.map(vv=>vv._1).toArray)
        })        
	  
        val mapOut=mapOutlinks.toArray.toMap
       val fathers= mapInlinks
       				.map(v=>{val sortv=v._2.sortBy(key=>key)
       				    sortv.mkString(",")})
       				.distinct
       				.map(v=>v.split(","))
       	val fathers_in=fathers
       						.map(v=>{
       						  var result:Array[String]=Array()
       				  			for(i<-v){
       				  			  if(mapIn.contains(i)){
       				  			    result =result ++mapIn(i)
       				  			  }
       				  			}
       						  (v.mkString(","),result.distinct)
       				  			  })
       
       val fathers_out=fathers.map(v=>{var result :Array[String]=Array()
         							   for(i<-v){
         							     if(mapOut.contains(i)){
         							       result =result ++mapOut(i)
         							     }
         							   }
       									(v.mkString(","),result.distinct)
         							})
       val sons=mapOutlinks
       				.map(v=>{val sortv=v._2.sortBy(key=>key)
       				    sortv.mkString(",")})
       				.distinct
       				.map(v=>v.split(","))
       	val sons_in=sons
       						.map(v=>{
       						  var result:Array[String]=Array()
       				  			for(i<-v){
       				  			  if(mapIn.contains(i)){
       				  			    result =result ++mapIn(i)
       				  			  }
       				  			}
       						  (v.mkString(","),result.distinct)
       				  			  })  
       val up_vertices=mapInlinks.map(v=>(v._2.sortBy(key=>key).mkString(","),v._1))
       							.join(fathers_in)
       							.join(fathers_out)
       							.map(v=>(v._2._1._1.toLong,(v._2._1._2++v._2._2).distinct))
       							
       val down_vertices=mapOutlinks.map(v=>(v._2.sortBy(key=>key).mkString(","),v._1))
       								.join(sons_in)
       								.map(v=>(v._2._1.toLong,v._2._2))
       								
       								
       val originaledge:RDD[Edge[Double]]=graph.edges.map(e=>Edge(e.srcId,e.dstId,0.0))								
       val vertices=graph.vertices
       					.leftJoin(up_vertices)((vertexid,old,newopt)=>(newopt.getOrElse(Array())))
       					.leftJoin(down_vertices)((vertexid,old,newopt)=>(old++newopt.getOrElse(Array()).distinct))
       	val newedges=vertices.flatMap(v=>{
       			v._2.map(id=>Edge(id.toLong,v._1,0.0))
       	})
       	.subtract(originaledge)


	   val newgraph:Graph[(org.apache.spark.util.Vector,org.apache.spark.util.Vector),Double]=Graph(graph.vertices, newedges)
	   val result=newgraph.triplets.map(triplet=>{var value:Double= 0
        							val len:Int=triplet.srcAttr._1.length
        							val v1=triplet.srcAttr._1
        							val v2=triplet.dstAttr._2
        							var i = 0
        							while(i<len){
        							  value +=v1.apply(i)*v2.apply(i)
        							  i +=1
        							}
        							(triplet.srcId,triplet.dstId,value)})
        							.sortBy(v=>v._3, false)
//	   println("graph:"+result.count)

	   result.cache

	}
	def linkprediction(
	    graph:Graph[(org.apache.spark.util.Vector,org.apache.spark.util.Vector),Double])={
	  val g = graph.cache
	  val v=g.vertices.map(v=>v._1)
	  val edge:RDD[((VertexId,VertexId),Double)]=v.cartesian(v).map(edge=>((edge._1,edge._2),0))
	  
	  val originaledges=g.edges.map(edge=>((edge.srcId,edge.dstId),edge.attr) )
	  val leftedge=	edge.subtractByKey(originaledges)
	  					.map(edge=>Edge(edge._1._1,edge._1._2,edge._2))					

      val vertor:org.apache.spark.util.Vector=null
	  val ngraph=Graph(g.vertices,leftedge)
/*	    		  .outerJoinVertices(graph.vertices)((vertexid,old,newopt)=>
	    		    							newopt.getOrElse((vertor,vertor))).cache
	    		    							* 
	    		    							*/
///	   val newgraph:Graph[(org.apache.spark.util.Vector, org.apache.spark.util.Vector),Double]
	   println(ngraph.vertices.count+";"+ngraph.edges.count)
       val result=ngraph.triplets.map(triplet=>{var value:Double= 0
        							val len:Int=triplet.srcAttr._1.length
        							val v1=triplet.srcAttr._1
        							val v2=triplet.dstAttr._2
        							var i = 0
        							while(i<len){
        							  value +=v1.apply(i)*v2.apply(i)
  //      							  if(triplet.srcId == 1191262305L && triplet.dstId == 1496850204L)println("vale:"+value+";"+v1.apply(i)+";"+v2.apply(i))
        							  i +=1
        							}
 //      								if(triplet.srcId == 1191262305L && triplet.dstId == 1496850204L)println()
        							(triplet.srcId,triplet.dstId,value)})
        							.sortBy(v=>v._3, false)

        
        result
	}
}