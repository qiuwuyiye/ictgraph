package ict.graph.pagerank

import ict.graph.common._

import org.apache.spark._
import org.apache.spark.graphx._
//import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

import org.apache.spark.graphx.util.GraphGenerators

import java.io.PrintWriter

object TestPagerank {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("TestPagerankIctLoader").setMaster("local[4]")
    val sc=new SparkContext(conf)
    
    val desfile="println.txt"
    val pw=new PrintWriter(desfile)
    
    val verticesFile="vertices.txt"
    val verminPartitions=2
    val edgesFile="edges.txt"
    val edgminPartitions=2
    
    //load file
    val loader=new IctGraphLoader(sc)
    //val graph=loader.LoaderFile(verticesFile, verminPartitions, edgesFile, edgminPartitions)
    val graph=loader.LoaderEdgeFile(edgesFile, edgminPartitions)
    /*
    val numVertices=1000000
    val numEParts=10
    val graph:Graph[Long,Int]=GraphGenerators.logNormalGraph(sc,numVertices,numEParts)
    */
    
    //run pagerank 
   // val tol=0.0001
   // val resetProb=0.15
    val MaxnumIter=20
    val path="E:/Projects/git/ictgraph/res/pagerank"
    val pagerank=new IctPageRank()
    
    val start=System.currentTimeMillis();
    val pagerankGraph=pagerank.runPagerank(graph,MaxnumIter)
    val end=System.currentTimeMillis(); //获取结束时间  
    println("Pagerank运行时间： "+(end-start)+"ms");   
    val content="Pagerank运行时间： "+(end-start)+"ms"
    pw.write(content+"\n")
    pw.write("num edges = " + graph.numEdges+"\n")
    pw.write("num vertices = " + graph.numVertices+"\n")
    pw.flush()
    pw.close()
    //println("num edges = " + graph.numEdges);
    //println("num vertices = " + graph.numVertices); 
    //pagerankGraph.vertices.sortBy(_._2, ascending=false, numPartitions=1).saveAsTextFile(path)
    /*
    val p1=pagerankGraph.vertices.sortBy(_._2, ascending=false, numPartitions=1)
    val p2=p1.map{
      case(k,v)=>"%s\t%s".format(k.toString,v.toString)
    }
    p2.saveAsTextFile(path)
    * 
    */
    println("pagerank top 5排名如下:")
    pagerankGraph.vertices.top(5)(Ordering.by(_._2)).foreach(println)
    
  }

}