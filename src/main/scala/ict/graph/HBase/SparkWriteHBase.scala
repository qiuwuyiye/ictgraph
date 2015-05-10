package ict.graph.HBase


import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put, Get}
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor, TableName }
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.io.Source

import java.util.Random;

class SparkWriteHBase extends  Serializable{
  
 
  def saveHTable(rdd:RDD[String],conf:Configuration,tableName:String)={
    
    // Initialize hBase table if necessary
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      print("Creating HTable_test Table")
      val tableDesc = new HTableDescriptor(tableName)
      tableDesc.addFamily(new HColumnDescriptor("E".getBytes()))
      tableDesc.addFamily(new HColumnDescriptor("V".getBytes()))
      admin.createTable(tableDesc)
    }   
    val myTable = new HTable(conf, tableName);
  	conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
  	
  	//put data into table
  	  
  	val jobConf = new JobConf(conf, getClass)
  	jobConf.setOutputFormat(classOf[TableOutputFormat])
  	  
  	val tuple=rdd.map(line=>{
    val fields=line.trim().split(" ",3)
    (fields(0), fields(2))
    }
  			)
    tuple.cache()
    tuple.flatMap{
       case (k, v) => convert(k, v) 
      }.saveAsHadoopDataset(jobConf)  
  	}

    def convert(rowId:String,desIds:String) = {
      val cf1="E"
      val cf2="V"
      val t=10
      val one=1
      try{
      val put = new Put(Bytes.toBytes(rowId))
      desIds.split(" ").foreach{ desId =>
        val r=new Random()
      	val a=r.nextDouble()*10+1.0
      	val b=r.nextDouble()*10+1.0
      	val tmp=t/a
      	val f=(b/a)*Math.pow(tmp, b-1)*Math.exp((Math.pow(tmp, b)*(-1)))
	    put.add(Bytes.toBytes(cf1), Bytes.toBytes(desId), Bytes.toBytes(f.toDouble))
	    println(rowId+" "+cf1+" "+desId+" "+f)
	    	}
	    put.add(Bytes.toBytes(cf2), Bytes.toBytes("attr"), Bytes.toBytes(one.toDouble))
	     println(rowId+" "+cf2+" "+"attr"+" "+one.toDouble)
	    //println(rowId+" "+desIds)
	   Some (new ImmutableBytesWritable, put)
	   }catch {
      	case _:Throwable => {println("[ERR] Bad line:" + rowId+" "+desIds);None}    	
      }
  	}
}