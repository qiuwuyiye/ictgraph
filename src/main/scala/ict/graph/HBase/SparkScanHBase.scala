package ict.graph.HBase

import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor, HColumnDescriptor, TableName }
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable, Put, Get}
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark._

import scala.collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;

class SparkScanHBase (@transient sc: SparkContext) extends Serializable{
  
  def ScanHTable(conf: Configuration, tb_name: String,scan:Scan): RDD[(String, Map[String, Map[String, Double]])] = {
	conf.set(TableInputFormat.INPUT_TABLE, tb_name)
    val proto = ProtobufUtil.toScan(scan)
    val ScanToString = Base64.encodeBytes(proto.toByteArray())
    conf.set(TableInputFormat.SCAN, ScanToString)
    conf.setInt("hbase.client.scanner.caching", 10000)
   
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tb_name)) {
      print("Table Not Exists! Create Table") 
      val tableDesc = new HTableDescriptor(tb_name)    
      tableDesc.addFamily(new HColumnDescriptor("E".getBytes()))
      tableDesc.addFamily(new HColumnDescriptor("V".getBytes()))
      admin.createTable(tableDesc)
     }  	  
     sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable], classOf[Result]) map {
          case (rowId, res) =>
            (Bytes.toString(rowId.get), extractRow(res))
        }         
  }
  
   /**
   * Transform a HBase Result object to a scala Map
   * 
   * Returns `Map[String, Map[String, String]]` with format
   * `Map[column_family, Map[column, value]`
   */
   def extractRow(res: Result): Map[String, Map[String, Double]] = {
      res.getNoVersionMap().map {
        case (cf, cols) => (Bytes.toString(cf), cols.map{
          case (k, v) => (Bytes.toString(k), Bytes.toDouble(v))
        }.toMap        
      )}.toMap
    }

}