package com.dapeng.hbase

import com.dapeng.hbase.bean.XF
import com.dapeng.hbase.util.{HBaseUtils, JsonToBean, MyKafkaUtil, PropertiesUtil}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

import scala.util.Try


object KafkaHBase {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealTimeApp")
    sparkConf.set("spark.streaming.backpressure.enabled", "true")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
    //控制每秒读取Kafka每个Partition最大消息数/second，若Streaming批次为3秒，topic最大分区为3，则每3s批次最大接收消息数为3*3*1000=9000/bach
    //             .set("spark.streaming.kafka.maxRatePerPartition", "1000")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    //2.创建StreamingContext
    val ssc = new StreamingContext(sc, Seconds(3))
//    ssc.sparkContext.setCheckpointDir("eventCheck")

    //3.读取Kafka数据
    val topic: String = PropertiesUtil.load("config.properties").getProperty("kafka.topic")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)

    val tableName = "dwd_student_stage_work_detail_new"
    val quorum = "hadoop102,hadoop103,hadoop104"
    val port = "2181"

    kafkaDStream.map(ds=>JsonToBean.gson(ds.value())).foreachRDD(rdd=>{

//      rdd.foreach(println(_))
      try{
        rdd.foreachPartition(rddp=>{
          val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
          conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
          val htable = HBaseUtils.getTable(conf,tableName)
          rddp.foreach(xf=>{

            val region_num=math.abs(xf.user_id.hashCode()^xf.course_id.hashCode)%9

            val rowkey=region_num+"_"+xf.user_id+"_"+xf.course_id+"_"+xf.stage_id+"_"+xf.curriculum_id

            val put = new Put(Bytes.toBytes(rowkey))
//            id	course_id	stage_id	user_id	lmss_achievements_end	job_submit_end	job_submit_time	curriculum_id	excelserverrcid	excelserverrn	chapter_node	handin	score	handin_num	last_modified_date
            put.add(Bytes.toBytes("info"),Bytes.toBytes("id"),Bytes.toBytes(xf.id))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("course_id"),Bytes.toBytes(xf.course_id))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("stage_id"),Bytes.toBytes(xf.stage_id))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("user_id"),Bytes.toBytes(xf.user_id))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("lmss_achievements_end"),Bytes.toBytes(xf.lmss_achievements_end))
            if (xf.job_submit_end!=null) {
              put.add(Bytes.toBytes("info"), Bytes.toBytes("job_submit_end"), Bytes.toBytes(xf.job_submit_end))
            }
            if (xf.job_submit_time!=null) {
              put.add(Bytes.toBytes("info"),Bytes.toBytes("job_submit_time"),Bytes.toBytes(xf.job_submit_time))
            }
            put.add(Bytes.toBytes("info"),Bytes.toBytes("curriculum_id"),Bytes.toBytes(xf.curriculum_id))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("excelserverrcid"),Bytes.toBytes(xf.excelserverrcid))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("excelserverrn"),Bytes.toBytes(xf.excelserverrn))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("chapter_node"),Bytes.toBytes(xf.chapter_node))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("handin"),Bytes.toBytes(xf.handin))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("score"),Bytes.toBytes(xf.score))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("handin_num"),Bytes.toBytes(xf.handin_num))
            put.add(Bytes.toBytes("info"),Bytes.toBytes("last_modified_date"),Bytes.toBytes(xf.last_modified_date))
//            Try(htable.put(put)).getOrElse(htable.close())//将数据写入HBase，若出错关闭table
            htable.put(put)


          })
          htable.close()
        })

      }catch {
        case e:Exception =>{print(e)}
      }finally {

      }


    })


    ssc.start()
    ssc.awaitTermination()



  }
}
