package ict.graph.pagerank

import scala.reflect.ClassTag
import org.apache.spark.Logging
import org.apache.spark.graphx._
import java.io.PrintWriter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import ict.graph.common._
import ict.graph.common.IctGraphLoader

import iie.udps.common.hcatalog.scala.{ SerHCatInputFormat, SerHCatOutputFormat }
import org.apache.hadoop.io.NullWritable
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hive.hcatalog.data.DefaultHCatRecord
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.{ SerializableWritable, SparkConf, SparkContext }
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

class PageRank extends Logging with Serializable{
  
  def runPagerank[VD:ClassTag,ED:ClassTag](
      graph:Graph[VD,ED],numIter:Int,tol:Double=0.0001,resetProb:Double=0.15):Graph[Double,Double]=
      {
		  // Initialize the pagerankGraph with each edge attribute
          // having weight 1/outDegree and each vertex with attribute 1.0.
		  val pagerankGraph:Graph[(Double,Double),Double]=graph
		   // Associate the outdegree with each vertex
		  .outerJoinVertices(graph.outDegrees){
		    (vid,vdata,deg)=>deg.getOrElse(0)
		  }
		  // Set the weight on the edges based on the degree
		  .mapTriplets(e => 1.0/e.srcAttr)
		  // Set the vertex attributes to (initalPR, delta = 0)
		  .mapVertices((id,attr)=>(0.0,1.0))
		  .cache()
		  
		  // Define the three functions needed to implement PageRank
          // in version of Pregel
		  
		  def vertexProgram(id: VertexId, attr: (Double,Double), msgSum: Double):(Double,Double)={
		    val (oldPR,lastDelta)=attr
		    val newPR=oldPR+(1.0-resetProb)*msgSum
		    (newPR,newPR-oldPR)
		  }
		  
		  def sendMessage(edge: EdgeTriplet[(Double, Double), Double])={
		    if(edge.srcAttr._2 > tol){
		      Iterator((edge.dstId, edge.srcAttr._2 * edge.attr))
		    } else {
		      Iterator.empty
		    }
		  }
		  
		  def messageCombiner(a: Double, b: Double):Double=a+b
		  
		  // The initial message received by all vertices in PageRank
		  val initialMessage=resetProb/(1.0-resetProb)
		  
		  // Execute  Pregel.
		  pregel(pagerankGraph, initialMessage, numIter,activeDirection=EdgeDirection.Out)(
		      vertexProgram, sendMessage, messageCombiner)
		      .mapVertices((vid,attr)=>attr._1)
      }
  
   def pregel[VD: ClassTag, ED: ClassTag, A: ClassTag]
     (graph: Graph[VD, ED],
      initialMsg: A,
      maxIterations: Int = Int.MaxValue,
      activeDirection: EdgeDirection = EdgeDirection.Either)
     (vprog: (VertexId, VD, A) => VD,
      sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)],
      mergeMsg: (A, A) => A)
    : Graph[VD, ED] =
   {
  	//val desfile="/home/liuwei/workspace/times.txt"
  	//val pw=new PrintWriter(desfile)
    var g = graph.mapVertices((vid, vdata) => vprog(vid, vdata, initialMsg)).cache()
    // compute the messages
    var messages = g.mapReduceTriplets(sendMsg, mergeMsg)
    var activeMessages = messages.count()
    // Loop
    var prevG: Graph[VD, ED] = null
    var i = 0
    while (activeMessages > 0 && i < maxIterations) {
      // Receive the messages. Vertices that didn't get any messages do not appear in newVerts.
      val newVerts = g.vertices.innerJoin(messages)(vprog).cache()
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

      logInfo("Pregel finished iteration " + i)
      
    
      // Unpersist the RDDs hidden by newly-materialized RDDs
      oldMessages.unpersist(blocking=false)
      newVerts.unpersist(blocking=false)
      prevG.unpersistVertices(blocking=false)
      prevG.edges.unpersist(blocking=false)
      // count the iteration
      i += 1
    }
  	//val content="Pregel finished iteration " + i
    //pw.write(content)
  	//pw.flush()
    //pw.close()
    
    g
  } 
 
}

object PageRank{
   def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("RunPageRank")
    val sc=new SparkContext(conf)  
    
    // parse arguments  
    /*
	val parser = new scopt.OptionParser[ParaConfig]("IctPageRank") {
      head("PageRank", "1.0")
      opt[String]("c") required () action { (x, c) =>
        c.copy(c = x)
      } text ("c is the description.xml file path")
    }
    // parser.parse returns Option[Config]
    val para=parser.parse(args, ParaConfig()).get
	//val MaxnumIter=para.pagerank_maxiter
	
    //val edgesFile=para.pagerank_input
   // val path=para.pagerank_output
    */
    //val configFile: String = para.c
    
    
   /* var configFile: String=null;
    for(i<-0 to args.length-1){
      if(args.apply(i).equals("-c")){
       configFile = args.apply(i+1)
      }   
    }  
    val edgminPartitions=10
    val configReader = new ConfigReader(configFile)
    val configInfo = configReader.ReadConfig();
    val MaxnumIter=configInfo.getOperatorInfo().getParameterInfo().getIter()
    val topk=configInfo.getOperatorInfo().getParameterInfo().getTopK()
    //val db_tb1=configInfo.getDatasetsInfo().getDatasetinfo().getInport().trim().split("\\.",2)
    val db_tb1=configInfo.getOperatorInfo().getParameterInfo().getInport1().trim().split("\\.",2)
    
    val tempDatabaseName=configInfo.getContexInfo().getContex().getTempDatabaseName()
    val tempHdfsBasePath=configInfo.getContexInfo().getContex().getTempHdfsBasePath()
    
    val jobinstanceid=configInfo.getJobinstanceid()*/
    val parser = new scopt.OptionParser[ParaConfig]("PageRank") {
      head("PageRank", "1.0")
      opt[String]("edgesfile") required () action { (x, c) =>
        c.copy(edgesfile = x)
      } text ("edgesfile is the input file that includes the graph edges infomation")
      opt[Int]("topk") optional () action { (x, c) =>
        c.copy(topk = x)
      } validate { x => if (x > 0) success else failure("Option --topk must be >0")
      } text ("topk is the top k users default is 10")
      opt[Int]("maxIteration") required () action { (x, c) =>
        c.copy(maxIteration = x)
      } validate { x => if (x > 0) success else failure("Option --maxIteration must be >0")
      } text ("maxIteration is the max iteration of NMF")
      opt[String]("output") required () action { (x, c) =>
        c.copy(output = x)
      } text ("output is the output file")
    }
    // parser.parse returns Option[Config]
    val para = parser.parse(args, ParaConfig()).get

    val edgesFile: String = para.edgesfile
   // val reducedDim: Int = para.reducedDim
    val MaxnumIter: Int = para.maxIteration
    val output: String = para.output
    val topk : Int = para.topk
    
    //val db_tb2=configInfo.getDatasetsInfo().getDatasetinfo().getOutport().trim().split("\\.",2)
    
   // val MaxnumIter=20
    
    //val desfile="/home/liuwei/workspace/println.txt"
    //val pw=new PrintWriter(desfile)
    
    //val verticesFile="hdfs://s1:8020/user/root/input/v1.txt"
 
   // val verticesFile="file:///home/liuwei/workspace/v.txt"
   // val verticesFile="vertices.txt"
   // val verminPartitions=2
    
   // val edgesFile="hdfs://s1:8020/user/root/input/e1.txt"
    
   // val edgesFile="hdfs://s1:8020/user/root/input/v.txt"
    //val edgesFile="edges.txt" 
    
    //load graph from file
    val loader=new IctGraphLoader(sc)
    //val loader=new IctGraphLoaer(sc)
    val graph=loader.LoaderEdgeFile(edgesFile, 2)
    //val graph=loader.LoaderEdgeHive(db_tb1.apply(0), db_tb1.apply(1), edgminPartitions)
    //println("num edges = " + graph.numEdges);
   // println("num vertices = " + graph.numVertices); 
    
    
     //run pagerank     
    //val tol=0.0001
   // val resetProb=0.15
    //val path="hdfs://s1:8020/user/root/output/pagerank"
       
    //val path="E:/Projects/git/ictgraph/result/pagerank2"
    val pagerank=new IctPageRank() 
    val start=System.currentTimeMillis();
    val pagerankGraph=pagerank.runPagerank(graph,MaxnumIter)
    val end=System.currentTimeMillis(); //获取结束时间  
    
    
    /*
    val content="The running time of Pagerank is ： "+(end-start)+"ms"
    pw.write(content+"\n")
    pw.write("num edges = " + graph.numEdges+"\n")
    pw.write("num vertices = " + graph.numVertices+"\n")
    pw.flush()
    pw.close()
    */
    //pagerankGraph.vertices.sortBy(_._2, ascending=false, numPartitions=1).saveAsTextFile(path)
    
    /*
    val p1=pagerankGraph.vertices.sortBy(_._2, ascending=false, numPartitions=1)
    val p2=p1.map{
      case(k,v)=>"%s\t%s".format(k.toString,v.toString)
    }
    p2.saveAsTextFile(path)
    println("pagerank top 5排名如下:")
    pagerankGraph.vertices.top(5)(Ordering.by(_._2)).foreach(println)
    
    */
    val p1=pagerankGraph.vertices.sortBy(_._2, ascending=false, numPartitions=1)
    
    val p2=sc.parallelize(p1.take(topk), 1)
    
    p2.map{
        case(k,v)=>"%s\t%s".format(k.toString,v.toString())
    }.saveAsTextFile(output)
    
    
   /* val dataset2=p2.map(f=>{
    val k:NullWritable=NullWritable.get()
    //val k:NullWritable=null
    val record:HCatRecord=new DefaultHCatRecord(2)
    record.set(0, f._1.toString)
    record.set(1, f._2)
    val v=new SerializableWritable[HCatRecord](record)
    (k,v)
   }
   )
   val outputJob = new Job()
   //val outputdb="ictgraph"
   val outputtb="pagerank"
   outputJob.setOutputFormatClass(classOf[SerHCatOutputFormat])
   outputJob.setOutputKeyClass(classOf[NullWritable])
   outputJob.setOutputValueClass(classOf[SerializableWritable[HCatRecord]])
   SerHCatOutputFormat.setOutput(outputJob, OutputJobInfo.create(tempDatabaseName, outputtb, null))
   SerHCatOutputFormat.setSchema(outputJob, SerHCatOutputFormat.getTableSchema(outputJob.getConfiguration))
   new PairRDDFunctions[NullWritable, SerializableWritable[HCatRecord]](dataset2).saveAsNewAPIHadoopDataset(outputJob.getConfiguration)
  
   println("pagerank top %s 排名如下:".format(topk.toString))
   p1.take(topk).foreach(println)
   
   val xmlFile =
<response>
		<jobinstanceid>{jobinstanceid}</jobinstanceid>
		<datasets>
			<dataset name="outport1">
				<row>{tempDatabaseName}.pagerank</row>
			</dataset>
		</datasets>
</response>      
    val output=tempHdfsBasePath+"stdout.xml"
	val conf2:Configuration= new Configuration()
	val fs:FileSystem= FileSystem.get(conf2)
	val fsout:FSDataOutputStream= fs.create(new Path(output))
	fsout.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n".getBytes())
	fsout.write(xmlFile.toString.getBytes())
	fsout.close()*/
   
   //println("Pagerank run time:"+(end-start)+"ms")
   sc.stop()
   System.exit(0)
  }
  
}