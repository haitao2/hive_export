package com.yjp.export

import java.util.{Date, Properties}
import com.yjp.export.util.Predef._
import com.alibaba.fastjson.JSONObject
import com.yjp.export.common.Constant
import com.yjp.export.util.CommonUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object RunApplication {

  case class PushDetail(batchNumber: Int, tablename: String, queryStartPartition: Int, queryEndPartition: Int, hiveStartPartition: Int, hiveEndPartition: Int, batchCount: Long, lastBatchTime: Date)

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    // 获取hive数据,需要按照天分区来获取，具体获取哪天需要通过外部传入。
    // val conf = new SparkConf().setAppName("export_hive")
    val conf = new SparkConf().setAppName("export_hive").setMaster("local[2]")
    //val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    // 获取mysql数据，确定推送批次
    val etlMeta = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://172.16.4.14:3306/test")
      .option("user", "root")
      .option("password", "Root#123")
      .option("query", "select * from test.etl_info")
      .option("driver", "com.mysql.jdbc.Driver")
      .load().sort($"batchNumber".asc).cache()
    val databaseTableName = CommonUtil.getQueryTableName(etlMeta.collect())
    val queryStartPartition = etlMeta.where("tableName='" + databaseTableName + "'").select("queryStartPartition").collect()(0).getAs[Int](0)
    etlMeta.where("tableName='" + databaseTableName + "'").save2Mysql()
    println(queryStartPartition)
   //val partitionsRow: Array[Row] = spark.sql("show partitions " + databaseTableName).collect()
   //val hiveStartPartition = partitionsRow(0).getAs[Int](0)
   //val hiveEndPartitions = partitionsRow(partitionsRow.length - 1).getAs[Int](0)
   //val timeHorizon = CommonUtil.getTimeHorizon(partitionsRow, queryStartPartition)
   //val queryStartPartition_ = timeHorizon._2
   //val queryEndPartition_ = timeHorizon._3
   //val queryConditionSql = CommonUtil.getQuerySql(databaseTableName, timeHorizon)
   //val batchCount = spark.sql(queryConditionSql._2).collect()(0).getAs[Int](0)
   //  spark.sql(queryConditionSql._1).foreachPartition(iterator => {
   //  val prop = new Properties()
   //  prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BOOTSTRAP_SERVERS)
   //  // key、value的序列化器
   //  prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
   //  prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
   //  prop.put("security.protocol", "SASL_PLAINTEXT")
   //  prop.put("sasl.mechanism", "GSSAPI")
   //  prop.put("sasl.kerberos.service.name", "kafka")
   //  val kafkaProducer = new KafkaProducer[String, String](prop)
   //  iterator.foreach(row => {
   //    // 获取schema
   //    val structFields: Array[StructField] = row.schema.toArray
   //    // 构建json
   //    val json = new JSONObject()
   //    json.put("table_name", databaseTableName)
   //    for (i <- 0 until row.length) {
   //      if (row.get(i) != null) {
   //        val columnValue = row.get(i)
   //        val columnName = structFields(i).name
   //        json.put(columnName, columnValue)
   //      }
   //    }
   //    println(Constant.KAFKA_BOOTSTRAP_SERVERS)
   //    kafkaProducer.send(new ProducerRecord[String, String](Constant.KAFKA_TOPIC, json.toJSONString))
   //  })

   //})


  }

}


