package ict.graph.NMF
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import ict.graph.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.io.PrintWriter
import java.util.concurrent.TimeUnit
import java.io.File
object CommunityDetection {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CommunityDetection")
    val sc = new SparkContext(conf)

    // parse arguments
    val parser = new scopt.OptionParser[ParaConfig]("CommunityDetection") {
      head("CommunityDetection", "1.0")
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
    //return the top R vertices in the Matric W
    //val com: Array[(Long,Double,Int)]
    var community  = WorH match{
        case 0 => result.vertices.sortBy(v=>v._2._1(0), false).take(R).map(v=>(v._1,v._2._1(0),1))
        case 1 => result.vertices.sortBy(v=>v._2._2(0), false).take(R).map(v=>(v._1,v._2._2(0),1))
      }
    for(k <- 1 until reducedDim)
    {
      //sort the community
      //println("this is the "+k+"th community and its members!")
      val com  =  WorH match{
        case 0 => result.vertices.sortBy(v=>v._2._1(k), false).take(R).map(v=>(v._1,v._2._1(k),k+1))
        case 1 => result.vertices.sortBy(v=>v._2._2(k), false).take(R).map(v=>(v._1,v._2._2(k),k+1))
      }
     community = community.union(com)
      //result.vertices.sortBy(v=>v._2._1(k), false).map(v=>(v._1,v._2._1(k),k)).foreach(println)
    }
     sc.parallelize(community, 1).saveAsTextFile(output)
    sc.stop()
    System.exit(0)
}
}