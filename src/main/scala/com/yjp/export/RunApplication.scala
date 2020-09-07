package com.yjp.export

import java.sql.Timestamp
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

  case class DataExportConfig(tablename: String, var batchstart_ts: Timestamp, var batchend_ts: Timestamp, typeid: Int, typename: String,
                              checkcolumn: String, var intoffset: Int, var longoffset: Long, columntypeid: Int,
                              var cloumntypename: String, var currentbatchcount: Long, ordered: Boolean)

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)
    // 获取hive数据,需要按照天分区来获取，具体获取哪天需要通过外部传入。
    // val conf = new SparkConf().setAppName("export_hive")
    val conf = new SparkConf().setAppName("export_hive").setMaster("local[2]")
    //val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val dataExport = spark.read
      .options(Map("kudu.master" -> Constant.KUDU_MASTER, "kudu.table" -> Constant.KUDU_TABLE))
      .format("kudu").load
    println("读取kudu数据成功，数据行数为：" + dataExport.collect().length)
    val typeId = args(0)
    // 获取到指定批次的所有表
    val exportDS = dataExport.filter(s"typeid=$typeId")
    exportDS.collect().foreach(row => {
      // 获取每一张需要推送到kafka的表，然后获取数据写入kafka
      // tablename,typeid,typename,checkcolumn,longoffset,columntypeid,cloumntypename,ordered
      val dataExportConfig = DataExportConfig(row.getAs[String](0), row.getAs[Timestamp](1), row.getAs[Timestamp](2), row.getAs[Int](3), row.getAs[String](4),
        row.getAs[String](5), row.getAs[Int](6), row.getAs[Long](7), row.getAs[Int](8),
        row.getAs[String](9), row.getAs[Long](10), row.getAs[Boolean](11))
      dataExportConfig.batchstart_ts = CommonUtil.getKuduTimestampFromLong(System.currentTimeMillis())
      dataExportConfig.cloumntypename = "spark sql 测试写数据是否正常"
      val checkColumn = row.getAs[String](5)
      val columnTypeId = row.getAs[Int](8)
      val typeOffset = if (columnTypeId == 0) {
        row.getAs[Date](1)
      } else if (columnTypeId == 1) {
        row.getAs[Int](6)
      } else {
        row.getAs[Long](7)
      }
      dataExportConfig.currentbatchcount = spark.sql("select count(*) from " + dataExportConfig.tablename + "_hive where " + checkColumn + ">=" + typeOffset).collect()(0).getAs[Long](0)
      spark.sql("select * from " + dataExportConfig.tablename + "_hive where " + checkColumn + ">=" + typeOffset)
        .foreachPartition(iterator => {
          // 发送数据到kafka。
          val prop = new Properties()
          prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BOOTSTRAP_SERVERS)
          // key、value的序列化器
          prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          // prop.put("security.protocol", "SASL_PLAINTEXT")
          // prop.put("sasl.mechanism", "GSSAPI")
          prop.put("sasl.kerberos.service.name", "kafka")
          val kafkaProducer = new KafkaProducer[String, String](prop)
          iterator.foreach(row => {
            // 获取schema
            val structFields: Array[StructField] = row.schema.toArray
            // 构建json
            val json = new JSONObject()
            json.put("table_name", dataExportConfig.tablename)
            for (i <- 0 until row.length) {
              if (row.get(i) != null) {
                val columnValue = row.get(i)
                val columnName = structFields(i).name
                json.put(columnName, columnValue)
              }
            }
            //kafkaProducer.send(new ProducerRecord[String, String](Constant.KAFKA_TOPIC, json.toJSONString))
          })
        })
      dataExportConfig.batchend_ts = CommonUtil.getKuduTimestampFromLong(System.currentTimeMillis())
      println("最终输出信息为" + dataExportConfig.toString)
      spark
        .sparkContext
        .makeRDD(Seq[DataExportConfig](dataExportConfig), 1)
        .toDF()
        .write
        .options(Map("kudu.master" -> Constant.KUDU_MASTER, "kudu.table" -> Constant.KUDU_TABLE))
        .mode(SaveMode.Append).format("kudu").save
    })
  }

}


