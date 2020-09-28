package com.dapeng.hbase

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Test{
  def main(args: Array[String]): Unit = {

    var aa="k0d9ris7ri"
    var bb="jcqzf9dc"

    println(math.abs(aa.hashCode^bb.hashCode)%9)
    test("k8109xzthg","jcqzf9dc")

  }



  def test(userId: String, courseId: String): Unit = {
//    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("yarn").set("deploy-mode", "cluster")
    val conf = new SparkConf().setAppName("HBaseReadTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 设置HBase集群的配置
    val hbaseConf: Configuration = HBaseConfiguration.create
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    val scan = new Scan()
    // rowkey格式为：时间戳+xxxx
    // 在这里设置你想对HBase表扫描的起始和结束行
    val regionNum=math.abs(userId.hashCode^courseId.hashCode)%9

    val startRow=regionNum+"_"+userId+"_"+courseId
    val endRow=startRow+"|"
    scan.setStartRow(String.valueOf(startRow).getBytes)
    scan.setStopRow(String.valueOf(endRow).getBytes)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "dwd_student_stage_work_detail_new")
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    val ScanToString: String = Base64.encodeBytes(proto.toByteArray)
    hbaseConf.set(TableInputFormat.SCAN, ScanToString)
    val rddValue: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
//    id	course_id	stage_id	user_id	lmss_achievements_end	job_submit_end	job_submit_time	curriculum_id	excelserverrcid	excelserverrn	chapter_node	handin	score	handin_num	last_modified_date
    rddValue.map(_._2).foreach(result=>{
      val rowkey = Bytes.toString(result.getRow)
      val id=Bytes.toString(result.getValue("info".getBytes(),"id".getBytes()))
      val course_id=Bytes.toString(result.getValue("info".getBytes(),"course_id".getBytes()))
      val stage_id=Bytes.toString(result.getValue("info".getBytes(),"stage_id".getBytes()))
      val user_id=Bytes.toString(result.getValue("info".getBytes(),"user_id".getBytes()))
      val lmss_achievements_end=Bytes.toString(result.getValue("info".getBytes(),"lmss_achievements_end".getBytes()))
      val job_submit_end=Bytes.toString(result.getValue("info".getBytes(),"job_submit_end".getBytes()))
      val job_submit_time=Bytes.toString(result.getValue("info".getBytes(),"job_submit_time".getBytes()))
      val curriculum_id=Bytes.toString(result.getValue("info".getBytes(),"curriculum_id".getBytes()))
      val excelserverrcid=Bytes.toString(result.getValue("info".getBytes(),"excelserverrcid".getBytes()))
      val excelserverrn=Bytes.toString(result.getValue("info".getBytes(),"excelserverrn".getBytes()))
      val chapter_node=Bytes.toString(result.getValue("info".getBytes(),"chapter_node".getBytes()))
      val handin=Bytes.toString(result.getValue("info".getBytes(),"handin".getBytes()))
      val score=Bytes.toString(result.getValue("info".getBytes(),"score".getBytes()))
      val handin_num=Bytes.toString(result.getValue("info".getBytes(),"handin_num".getBytes()))
      val last_modified_date=Bytes.toString(result.getValue("info".getBytes(),"last_modified_date".getBytes()))
      println(rowkey,id,course_id,stage_id,user_id,lmss_achievements_end,job_submit_end,job_submit_time,curriculum_id,
        excelserverrcid,excelserverrn,chapter_node,handin,score,handin,handin_num,last_modified_date)
    })
    System.out.println("成功从HBase读取数据")
  }
}
