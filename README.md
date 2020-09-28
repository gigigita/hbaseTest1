1.sparkstreaming 读取kafka数据，并将结果写入hbase分区表中
2.通过rowkey范围扫描，读取hbase中数据

重点：hbase预分区，9个分区，rowkey设计，唯一，并将userid^course_id的哈希值% 分区数9，将数据存入相应的分区表中。
读取数据时也是一样，拿到用户的userid^course_id，算出数据所在分区，拼接，范围扫描rowkey，查询出结果集。
