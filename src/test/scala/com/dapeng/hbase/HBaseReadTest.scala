package com.dapeng.hbase

import com.dapeng.hbase.util.HBaseUtils
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by guoxingyu on 2018/8/18.
 * 从HBase读取数据
 */

object HBaseReadTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseReadTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    sc.setLogLevel("WARN")
    val tableName = "dwd_student_stage_work_detail_new"
    val quorum = "hadoop102,hadoop103,hadoop104"
    val port = "2181"

    // 配置相关信息
    val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    // HBase数据转成RDD
    val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).cache()

    // RDD数据操作
    val data = hBaseRDD.map(x => {
      val result = x._2
      val key = Bytes.toString(result.getRow)
      val value = Bytes.toString(result.getValue("info".getBytes,"course_id".getBytes))

      (value,1)
    })

    data.reduceByKey(_+_).foreach(println(_))
    println(data.map(_._2).reduce(_ + _))

    sc.stop()
  }
}
