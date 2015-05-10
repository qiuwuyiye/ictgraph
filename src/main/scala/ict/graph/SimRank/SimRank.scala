package ict.graph.SimRank

import ict.graph.common._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.graphx._
import iie.udps.common.hcatalog.scala.{ SerHCatInputFormat, SerHCatOutputFormat }
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.io.NullWritable
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hive.hcatalog.data.DefaultHCatRecord
import org.apache.hadoop.mapred.JobConf
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.{ SerializableWritable, SparkConf, SparkContext }
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


object SimRank {
	def main(args:Array[String]){
	  val conf=new SparkConf().setAppName("SimRank")
	  val sc = new SparkContext(conf)
	  
	  
    /*var configFile: String=null;
    for(i<-0 to args.length-1){
      if(args.apply(i).equals("-c")){
       configFile = args.apply(i+1)
      }   
    }  
    
    
       val configReader = new ConfigReader(configFile)
       val configInfo = configReader.ReadConfig();
//	   val db_tb1=configInfo.getDatasetsInfo().getDatasetinfo().getInport().trim().split("\\.",2)
       val db_tb1=configInfo.getOperatorInfo().getParameterInfo().getInport1().trim().split("\\.",2)
//       val db_tb2=configInfo.getDatasetsInfo().getDatasetinfo().getOutport().trim().split("\\.",2)
	   val tempDatabaseName=configInfo.getContexInfo().getContex().getTempDatabaseName()
       val tempHdfsBasePath=configInfo.getContexInfo().getContex().getTempHdfsBasePath()
	   val db_tb2=Array(tempDatabaseName,"matrixD")
       val jobinstanceid=configInfo.getJobinstanceid()  	   
*/
	  val parser = new scopt.OptionParser[ParaConfig]("RunFilter") {
      head("RunFilter", "1.0")
      opt[String]("edgesfile") optional () action { (x, c) =>
        c.copy(edgesfile = x)
      } text ("edgesfile is the input file that includes the graph edges infomation")
      opt[String]("vertexfile") optional () action { (x, c) =>
        c.copy(vertexfile = x)
      } text ("vertexfile is the input file that includes the vertex that needs to be kept")
      opt[String]("outpath") optional () action { (x, c) =>
        c.copy(outpath = x)
      } text ("outpath is the output path")
      opt[Int]("partitions") optional () action { (x, c) =>
        c.copy(partitions = x)
      } text ("partitions is the min number of RDD's split parts,default is 10")
      opt[Int]("k") optional () action { (x, c) =>
        c.copy(topk = x)
      } text ("k is the number of iteration's times,default is 1")
    }
	  val para= parser.parse(args, ParaConfig()).get

	  
//	   val vertexfile:String="vertices4.txt"
	  
	   val vertexfile:String=para.vertexfile
	   val verminpartition:Int=para.partitions
//	   val verminpartition:Int=10 
//	   val edgefile:String="edges4.txt"	     
	   val edgefile:String=para.edgesfile
	   val edgeminpartition:Int=1
	   val outpath:String=para.outpath
	  //参数赋值
	  // val edgeminpartition:Int=10
	   val C:Double=0.8
	   val L:Int=5
	   val R:Int=1000
	   val T:Int=5
	   val time1 = System.currentTimeMillis()	   
       val graph = new IctGraphLoader(sc).LoaderFile(vertexfile, verminpartition, edgefile, edgeminpartition).cache
	   //val graph :Graph[Int,Double]= new IctGraphLoader(sc).LoaderEdgeHive(db_tb1.apply(0), db_tb1.apply(1), edgeminpartition)
	   val simrank=new SingleSourceSimRank()
	   val result = simrank.preprocessing(graph,L,T,R,C)
	   val resultRDD=sc.parallelize(result.toSeq, edgeminpartition)
	   //resultRDD.saveAsTextFile(outpath)
	   resultRDD.map(x=>x._1.toString+'\t'+x._2.toString).saveAsTextFile(outpath)
	   
	       /*   val xmlFile = 
//         <?xml version="1.0" encoding="UTF-8" ?>
<response>
		<jobinstanceid>{jobinstanceid}</jobinstanceid>
		<datasets>
			<dataset name="outport1">
				<row>{tempDatabaseName}.matrixD </row>
			</dataset>
		</datasets>
</response> 
          
         
        /*val confi:Configuration= new Configuration()
    	val fs:FileSystem= FileSystem.get(confi)
    	val fsout:FSDataOutputStream= fs.create(new Path(tempHdfsBasePath+"/stdout.xml"))
    	fsout.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n".getBytes())
    	fsout.write(xmlFile.toString.getBytes())
    	fsout.close()*/
	   
	   
	   
	   
	   
	  //将结果输出到HcatLog表中
    	//val a:org.apache.hadoop.io.NullWritable=null
	    //val data_out=resultRDD
	    			.map(v=>{val record=new DefaultHCatRecord(2)
  								record.set(0, v._1.toString)
  								record.set(1, v._2)
  								(a,new SerializableWritable[HCatRecord](record))})  							
  	   //val outputJob = new Job();
       //outputJob.setOutputFormatClass(classOf[SerHCatOutputFormat])
       //outputJob.setOutputKeyClass(classOf[NullWritable])
       //outputJob.setOutputValueClass(classOf[SerializableWritable[HCatRecord]])
       //SerHCatOutputFormat.setOutput(outputJob, OutputJobInfo.create(db_tb2.apply(0), db_tb2.apply(1), null))
       //SerHCatOutputFormat.setSchema(outputJob, SerHCatOutputFormat.getTableSchema(outputJob.getConfiguration))
       //new PairRDDFunctions[NullWritable, SerializableWritable[HCatRecord]](data_out).saveAsNewAPIHadoopDataset(outputJob.getConfiguration)

//	   resultRDD.map(result=>result._1+"\t"+result._2).saveAsTextFile(output)
	   val time2 = System.currentTimeMillis()
	   println("time:"+(time2-time1))*/
	   
	   sc.stop
	   System.exit(0)
	}
}