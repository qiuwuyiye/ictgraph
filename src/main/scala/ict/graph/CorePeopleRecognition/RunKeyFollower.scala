package ict.graph.CorePeopleRecognition

import org.apache.spark._
import ict.graph.CorePeopleRecognition._
import ict.graph.common._

object RunKeyFollower {
	def main(args:Array[String]){
	  //创建环境变量
		val conf = new SparkConf().setAppName("RunKeyFollower")
		val sc = new SparkContext(conf)
		
	  val parser = new scopt.OptionParser[ParaConfig]("RunKeyFollower") {
      head("RunKeyFollower", "1.0")
      opt[String]("edgesfile") required () action { (x, c) =>
        c.copy(edgesfile = x)
      } text ("edgesfile is the input file that includes the graph edges infomation")
      opt[String]("outpath") required () action { (x, c) =>
        c.copy(outpath = x)
      } text ("outpath is the output path")
      opt[Int]("startnode") required () action { (x, c) =>
        c.copy(startnode = x)
      } text ("startnode is the id of vertex that we need to find its key followers")
      opt[Int]("partitions") optional () action { (x, c) =>
        c.copy(partitions = x)
      } validate { x =>if (x > 0) success else failure("Option --partitions must be >0") 
      }text ("partitions is the min number of RDD's split parts,default is 2")
      opt[Int]("topk") optional () action { (x, c) =>
        c.copy(topk = x)
      }validate { x =>if (x > 0) success else failure("Option --topk must be >0") 
      } text ("topk is the number of output Vertices,default is 10")
    }
    // parser.parse returns Option[Config]
		val para=parser.parse(args, ParaConfig()).get
    
		val edgesFile:String=para.edgesfile
		val outpath:String=para.outpath
		
		val edgminPartitions=para.partitions
		val K = para.topk
		val Node=para.startnode
		
		//将图load进内存
		val loader=new CorePeopleGraph(sc)
		loader.LoaderFile( edgesFile, edgminPartitions)
		
		loader.KeySpreadNode(K, Node).map(v=>v._1.toString+"\t"+v._2.toString).saveAsTextFile(outpath)


		
	}
}