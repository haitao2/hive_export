package com.yjp.export

import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.yjp.export.common.Constant
import com.yjp.export.util.CommonUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object RunApplication {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    // 获取hive数据,需要按照天分区来获取，具体获取哪天需要通过外部传入。
    val databaseTableName = CommonUtil.getTableName(Constant.TABLE_NAME)._1 + "." + CommonUtil.getTableName(Constant.TABLE_NAME)._2
    val conf = new SparkConf().setAppName("hive_export")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._
    val partitionsRow: Array[Row] = spark.sql("show partitions " + databaseTableName).collect()
    val timeHorizon = CommonUtil.getTimeHorizon(partitionsRow, Constant.QUERY_CONDITION.toInt)
    val sql = CommonUtil.getQuerySql(databaseTableName, timeHorizon)
    val hiveDF: DataFrame = spark.sql(sql)
    hiveDF.foreachPartition(iterator => {
      val prop = new Properties()
      prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BOOTSTRAP_SERVERS)
      // key、value的序列化器
      prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      prop.put("security.protocol", "SASL_PLAINTEXT")
      prop.put("sasl.mechanism", "GSSAPI")
      prop.put("sasl.kerberos.service.name", "kafka")
      val kafkaProducer = new KafkaProducer[String, String](prop)
      iterator.foreach(row => {
        // 获取schema
        val structFields: Array[StructField] = row.schema.toArray
        // 构建json
        val json = new JSONObject()
        json.put("table_name", CommonUtil.getTableName(Constant.TABLE_NAME)._2)
        json.put("database_name", CommonUtil.getTableName(Constant.TABLE_NAME)._1)
        for (i <- 0 until row.length) {
          if (row.get(i) != null) {
            val columnValue = row.get(i)
            val columnName = structFields(i).name
            json.put(columnName, columnValue)
          }
        }
        println(Constant.KAFKA_BOOTSTRAP_SERVERS)
        kafkaProducer.send(new ProducerRecord[String, String](Constant.KAFKA_TOPIC, json.toJSONString))
      })

    })
  }
}
