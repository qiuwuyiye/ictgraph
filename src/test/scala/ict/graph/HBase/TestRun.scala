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
import org.apache.hadoop.hbase.client.Scan;

import scala.io.Source

object TestRun {

  def main(args: Array[String]): Unit = {
    
    val logFile = "file:///E:/Projects/spark/Test/test.txt" // Should be some file on your system   
    val sparkConf = new SparkConf().setAppName("RunTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile(logFile)
    
    //val tableName = "test"
    //
    val tableName = "test"
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "s1")
	conf.set("hbase.zookeeper.property.clientPort", "2181")
	val writer=new SparkWriteHBase()
    writer.saveHTable(file, conf, tableName)
    
    //scan HBase
    
    val conf2 = HBaseConfiguration.create()
    conf2.set("hbase.zookeeper.quorum", "s1")
	conf2.set("hbase.zookeeper.property.clientPort", "2181")
    
    val scan = new Scan();
    scan.addFamily(Bytes.toBytes("E"));
   //scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("airName"));
   /*
    scan.setStartRow(Bytes.toBytes("195861-1035177490"));
    scan.setStopRow(Bytes.toBytes("195861-1072173147"));
    scan.addFamily(Bytes.toBytes("info"));
    scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("levelCode"));
    */
   // Other options for configuring scan behavior are available. More information available at
   // http://hbase.apache.org/apidocs/org/apache/hadoop/hbase/mapreduce/TableInputFormat.html
 
   val scanner=new SparkScanHBase(sc)
   val scanner_res=scanner.ScanHTable(conf2, tableName, scan)
   
   
   scanner_res.foreach{
	    case (rowId,res) => {
	       res.map {
	        case (cf, clos) => {
	          clos.map{
	            case(k,v)=>{              
	            println(rowId+" "+cf+" "+k+" "+v)
             }
           }
         }
       }
     }
   }
  
   sc.stop()
  }

}