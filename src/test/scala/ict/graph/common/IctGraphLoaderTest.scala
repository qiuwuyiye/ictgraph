package ict.graph.common

import org.apache.spark._
import org.apache.spark.graphx._
//import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._

object IctGraphLoaderTest {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("IctGraphLoaderTest").setMaster("local[4]")
    val sc=new SparkContext(conf)
    
    val verticesFile="vertices.txt"
    val verminPartitions=2
    val edgesFile="edges.txt"
    val edgminPartitions=2
    
    val loader=new IctGraphLoader(sc)
    val graph=loader.LoaderFile(verticesFile, verminPartitions, edgesFile, edgminPartitions)
    println("num edges = " + graph.numEdges);
    println("num vertices = " + graph.numVertices);  
    /*
    val verticesfile=sc.textFile(verticesFile, verminPartitions)
    verticesfile.foreach{
      line=>{
        val verfields=line.trim().split("\\s+",2)
        println(verfields(0).toLong+" "+verfields(1).toInt)
      }
      
    }
    
    val edgesfile=sc.textFile(edgesFile, edgminPartitions)
    edgesfile.foreach{
      line=>{
        val edgesfields=line.trim().split("\\s+",3)
        println(edgesfields(0).toLong+" "+edgesfields(1).toLong+" "+edgesfields(2).toDouble)
      }
    }
    
    */
  }

}