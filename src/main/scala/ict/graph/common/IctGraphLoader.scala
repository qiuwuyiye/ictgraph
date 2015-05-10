package ict.graph.common

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

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

/**
 * Provides utilities for loading [[Graph]]s from files.
 */

class IctGraphLoader(@transient sc: SparkContext) extends Serializable {

  def LoaderFile(
    verticesFile: String,
    verminPartitions: Int = 2,
    edgesFile: String,
    edgminPartitions: Int = 2): Graph[Int, Double] =
    {
      val verticesfile = sc.textFile(verticesFile, verminPartitions)
      val vertices: RDD[(VertexId, Int)] = verticesfile.map(
        line => {
          val verfields = line.trim().split("\\s+", 2)
          (verfields(0).toLong, verfields(1).toInt)
        })
      val edgesfile = sc.textFile(edgesFile, edgminPartitions)
      val edges: RDD[Edge[Double]] = edgesfile.map(
        line => {
          val edgesfields = line.trim().split("\\s+", 3)
          Edge(edgesfields(0).toLong, edgesfields(1).toLong, edgesfields(2).toDouble)
        })
      val defaultVertexAttr = 1
      Graph(vertices, edges, defaultVertexAttr, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
    }
  def LoaderEdgeFile(
    edgesFile: String,
    edgminPartitions: Int=2): Graph[Int, Double] =
    {
      val edgesfile = sc.textFile(edgesFile, edgminPartitions)
      val edges: RDD[Edge[Double]] = edgesfile.map(
        line => {
          val edgesfields = line.trim().split("\\s+", 3)
          Edge(edgesfields(0).toLong, edgesfields(1).toLong, edgesfields(2).toDouble)
        })
      val defaultVD = 0
      val nullVert: RDD[(VertexId, Int)] = null
      Graph.fromEdges(edges, defaultVD, StorageLevel.MEMORY_AND_DISK,
        StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)
    }
   def LoaderEdgeHive(
       dbName:String,
       tbName:String,
       edgminPartitions: Int=2):Graph[Int, Double]=
       {
		  val inputJob = new Job()
		  SerHCatInputFormat.setInput(inputJob.getConfiguration, dbName, tbName)
		  val dataset = sc.newAPIHadoopRDD(inputJob.getConfiguration, classOf[SerHCatInputFormat], classOf[NullWritable], classOf[SerializableWritable[HCatRecord]])
		  val fields=dataset.map{case(k,v)=>{
			  val field1=v.value.get(0).toString()
					  val field2=v.value.get(1).toString()
					  val field3=v.value.get(2).toString().toDouble
					  (field1,field2,field3)
		  }}
		  
		  val edges: RDD[Edge[Double]] = fields.map(
				  f => {
				Edge(f._1.toLong, f._2.toLong, f._3)
				  })
		  val defaultVD = 0
	      Graph.fromEdges(edges, defaultVD, StorageLevel.MEMORY_AND_DISK,
						  StorageLevel.MEMORY_AND_DISK).partitionBy(PartitionStrategy.RandomVertexCut)	  
       }

}