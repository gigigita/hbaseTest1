package com.dapeng.hbase

import com.dapeng.hbase.util.HBaseUtils
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by guoxingyu on 2018/8/17.
 * 通过HTable中的Put向HBase写数据
 */

object HBaseWriteTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseWriteTest").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val tableName = "imooc_course_clickcount"
    val quorum = "hadoop102"
    val port = "2181"

    // 配置相关信息

    val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)


    val indataRDD = sc.makeRDD(Array("002,10","003,10","004,50"))

    indataRDD.foreachPartition(x=> {
      val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
      conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

      val htable = HBaseUtils.getTable(conf,tableName)

      x.foreach(y => {
        val arr = y.split(",")
        val key = arr(0)
        val value = arr(1)

        val put = new Put(Bytes.toBytes(key))
        put.add(Bytes.toBytes("info"),Bytes.toBytes("clict_count"),Bytes.toBytes(value))
        htable.put(put)
      })
    })

    sc.stop()

  }
}
