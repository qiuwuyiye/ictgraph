package ict.graph.pagerank

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

object ReadWriteTest {

  def main(args: Array[String]): Unit = {
  val dbName = "ictgraph";
  val inputTbl = "edges";
  val outputTbl = "edges3";

  val sc: SparkContext = new SparkContext(new SparkConf().setAppName("ReadWriteTest"))

  val inputJob = new Job()
  SerHCatInputFormat.setInput(inputJob.getConfiguration, dbName, inputTbl)

  val dataset = sc.newAPIHadoopRDD(inputJob.getConfiguration, classOf[SerHCatInputFormat], classOf[NullWritable], classOf[SerializableWritable[HCatRecord]])
  
  val fields=dataset.map{case(k,v)=>{
    val field1=v.value.get(0).toString()
    val field2=v.value.get(1).toString()
    val field3=v.value.get(2).toString().toDouble
    (field1,field2,field3)
  }}
  
  //fields.foreach(println)
  
  val dataset2=fields.map(f=>{
    val k:NullWritable=NullWritable.get()
    //val k:NullWritable=null
    val record:HCatRecord=new DefaultHCatRecord(3)
    record.set(0, f._1)
    record.set(1, f._2)
    record.set(2,f._3)
    val v=new SerializableWritable[HCatRecord](record)
    (k,v)
  }
  )
  val outputJob = new Job()
  outputJob.setOutputFormatClass(classOf[SerHCatOutputFormat])
  outputJob.setOutputKeyClass(classOf[NullWritable])
  outputJob.setOutputValueClass(classOf[SerializableWritable[HCatRecord]])
  SerHCatOutputFormat.setOutput(outputJob, OutputJobInfo.create(dbName, outputTbl, null))
  SerHCatOutputFormat.setSchema(outputJob, SerHCatOutputFormat.getTableSchema(outputJob.getConfiguration))
  new PairRDDFunctions[NullWritable, SerializableWritable[HCatRecord]](dataset2).saveAsNewAPIHadoopDataset(outputJob.getConfiguration)
  println("done")
  
  }

}