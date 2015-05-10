package ict.graph.CorePeopleRecognition

import org.apache.spark._
import ict.graph.CorePeopleRecognition._
import ict.graph.common._


object RunFilter {
	def main(args:Array[String]){
	  //创建环境变量
		val conf = new SparkConf().setAppName("RunFilter")
		val sc = new SparkContext(conf)
		
	// parse arguments
	val parser = new scopt.OptionParser[ParaConfig]("RunFilter") {
      head("RunFilter", "1.0")
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
      }validate { x =>if (x > 0) success else failure("Option --partitions must be >0") 
      }text ("partitions is the min number of RDD's split parts,default is 2")
    }
    // parser.parse returns Option[Config]
    val para=parser.parse(args, ParaConfig()).get

	val edgesFile:String=para.edgesfile
	val vertexFile:String=para.vertexfile
	val outpath:String=para.outpath
	val edgminPartitions=para.partitions

		//将图load进内存
	val loader=new CorePeopleGraph(sc)
	loader.LoaderFile( edgesFile, edgminPartitions)
		
		
	loader.filterGraph(vertexFile,edgminPartitions).saveAsTextFile(outpath)

	}
}