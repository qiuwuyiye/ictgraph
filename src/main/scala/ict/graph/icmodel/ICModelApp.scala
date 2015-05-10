package ict.graph.icmodel

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import ict.graph.common._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import java.io.PrintWriter

object ICModelApp {

  def main(args: Array[String]) {
    var conf: SparkConf = null;
    if ("--local" == args(0)) {
      conf = new SparkConf().setAppName("ICTICModelApp").setMaster("local[4]")
    } else {
      conf = new SparkConf().setAppName("ICTICModelApp")
    }

    val sc = new SparkContext(conf)

    val parser = new scopt.OptionParser[ParaConfig]("ICTICModelApp") {
      head("ICTICModelApp", "1.0")
      opt[String]('e', "edgesfile") required () action { (x, c) =>
        c.copy(edgesfile = x)
      } text ("edgesfile is the input file that includes the graph edges and their weights")
      opt[String]('s', "seedfile") required () action { (x, c) =>
        c.copy(seedfile = x)
      } text ("seedfile is the input file that inlcudes the activation souces")
      opt[String]('o', "output") required () action { (x, c) =>
        c.copy(output = x)
      } text ("output is the output file")
      opt[Int]('r', "numrand") optional () action { (x, c) =>
        c.copy(numrand = x)
      } validate { x => if (x > 0) success else failure("Option --numrand must be >0")
      } text ("numrand is the number of simulation times for information cascades, default is 1000")
      opt[Int]("partitions") optional () action { (x, c) =>
        c.copy(partitions = x)
      } validate { x => if (x > 0) success else failure("Option --partitions must be >0")
      } text ("partitions is the min number of RDD's split parts,default is 2")
      opt[Unit]('l', "local") action { (_, c) =>
        c.copy(local = true)
      } text ("flag to indicate local running")
     opt[Unit]("seq") action { (_, c) =>
        c.copy(seq = true)
      } text("flag to indicate submitting job sequencially")
    }

    val para = parser.parse(args, ParaConfig()).get

    val edgesFile: String = para.edgesfile
    val seedFile: String = para.seedfile
    val output: String = para.output
    val numRand: Int = para.numrand
    val edgminPartitions = para.partitions
    val verminPartitions = para.partitions

    val start = System.currentTimeMillis();

    val loader = new IctGraphLoader(sc)
    val graph = loader.LoaderEdgeFile(edgesFile, edgminPartitions)

    //read seedfile for propagation
    val seedread = sc.textFile(seedFile, verminPartitions)
    val seeds: RDD[(VertexId, Boolean)] = seedread.map(
      line => {
        val verfields = line.trim().split("\\s+", 2)
        (verfields(0).toLong, true)
      })

    //    val (vSample, aveActivated): (RDD[VertexId], Long) =
    //      ICTICModel.runICModel1(sc)(graph, seeds, numRand)
    //vSample.saveAsTextFile("activatedVertices.txt")
    //    val (result, aveActivated): (RDD[(VertexId, Double)], Double) =
    //      ICTICModel.runICModel2(sc)(graph, seeds, numRand)
    var result:RDD[(VertexId, Double)] = null
    var aveActivated = 0.0
    if (para.seq == false ){
      val (r, a) = ICTICModel.runICModel3(sc)(graph, seeds, numRand)
      result=r
      aveActivated =a
    }else{
      val (r, a) = ICTICModel.runICModel2(sc)(graph, seeds, numRand)
      result=r
      aveActivated =a
    }
    val end = System.currentTimeMillis();
    

    val formResult = result.sortBy(_._2, ascending = false, numPartitions = 2).map {
      case (k, v) => "%s\t%s".format(k.toString, v.toString)
    }
    
    if(para.local){
    	val out = new PrintWriter(output)
    	out.println(formResult.collect.mkString("\n"))
    	out.close()
    }else{
    	formResult.saveAsTextFile(output)
    }
    
    println(s"The average total number of activated vertices is $aveActivated")
    println("Algorithm Run time:" + (end - start) + "ms");
    sc.stop()
  }

}