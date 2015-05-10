package ict.graph.CorePeopleRecognition

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.spark.rdd.RDD

object CorePeopleGraphTest {
	def main(args:Array[String]){
		val conf = new SparkConf().setAppName("CorePeopleGraphTest").setMaster("local[2]")
		val sc = new SparkContext(conf)
	  
//		val edgesFile="hdfs://s1:8020/user/root/caoqi/CorePeople/CorePeopleEdges.txt"
		
		val edgesFile="CorePeopleEdges.txt"
		val edgminPartitions=args.apply(0).toInt
		val sig:String =args.apply(1)
		
		val loader=new CorePeopleGraph(sc)
		
		val time1=System.currentTimeMillis()
		loader.LoaderFile( edgesFile, edgminPartitions)
		val path="hdfs://s1:8020//user/root/caoqi/CorePeople/output";
//		val path=""
		if(sig == "Source")
			println("SourceNode:"+loader.SourceNode)
		else if(sig == "OutDegree")
//			loader.MaxOutDegreeNode(10).map(v=>v._1.toString+"\t"+v._2.toString).saveAsTextFile(path+"/MaxOutDegreeNode")
		  loader.MaxOutDegreeNode(10).map(v=>v._1.toString+"\t"+v._2.toString).foreach(println)
		else if(sig == "InDegree")
//			loader.MaxInDegreeNode(10).map(v=>v._1.toString+"\t"+v._2.toString).saveAsTextFile(path+"/MaxInDegreeNode")
		  loader.MaxInDegreeNode(10).map(v=>v._1.toString+"\t"+v._2.toString).foreach(println)
		else if (sig == "TransRegional")
//			loader.TransRegionalNode(10).map(v=>v._1.toString+"\t"+v._2.toString).saveAsTextFile(path+"/TransRegionalNode")
		  loader.TransRegionalNode(10).map(v=>v._1.toString+"\t"+v._2.toString).foreach(println)
		else if(sig == "CriticalRegion")
//			loader.CriticalRegion(10).map(v=>v._1.toString+"\t"+v._2.toString).saveAsTextFile(path+"/CriticalRegion")
			loader.CriticalRegion(10).map(v=>v._1.toString+"\t"+v._2.toString).foreach(println)
		 else if(sig == "KeySpread")
//			loader.KeySpreadNode(20, 1988572314).map(v=>v._1.toString+"\t"+v._2.toString).saveAsTextFile(path+"/KeySpreadNode")
			loader.KeySpreadNode(20, 1988572314).map(v=>v._1.toString+"\t"+v._2._1.toString+":"+v._2._2.toString).foreach(println)
		else if(sig == "Filter")
		   loader.filterGraph("filterVertex.txt",2).foreach(println)
		val time2=System.currentTimeMillis()
		println("time:"+(time2-time1))
			//		loader.filterGraph("filterVertex.txt",2)
	}
}