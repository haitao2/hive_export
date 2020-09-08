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
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

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

    val typeId = args(0)
    // 获取到指定批次的所有表
    val exportDS = dataExport.filter(s"typeid=$typeId")
    exportDS.collect().foreach(row => {
      val dataExportConfig = DataExportConfig(row.getAs[String](0), row.getAs[Timestamp](1), row.getAs[Timestamp](2), row.getAs[Int](3), row.getAs[String](4),
        row.getAs[String](5), row.getAs[Int](6), row.getAs[Long](7), row.getAs[Int](8),
        row.getAs[String](9), row.getAs[Long](10), row.getAs[Boolean](11))
      dataExportConfig.batchstart_ts = CommonUtil.getKuduTimestampFromLong(System.currentTimeMillis())
      val checkColumn = row.getAs[String](5)
      val columnTypeId = row.getAs[Int](8)
      val data: DataFrame = spark.read.options(Map("kudu.master" -> Constant.KUDU_MASTER, "kudu.table" -> dataExportConfig.tablename))
        .format("kudu").load()
      data.createOrReplaceTempView(dataExportConfig.tablename)
      val maxOffset = spark.sql("select max(" + dataExportConfig.checkcolumn + ") from " + dataExportConfig.tablename).collect()(0)
      val typeOffset = if (columnTypeId == 0) {
        dataExportConfig.batchstart_ts = maxOffset.getAs[Timestamp](0)
        row.getAs[Date](1)
      } else if (columnTypeId == 1) {
        dataExportConfig.intoffset = maxOffset.getAs[Int](0)
        row.getAs[Int](6)
      } else {
        dataExportConfig.longoffset = maxOffset.getAs[Long](0)
        row.getAs[Long](7)
      }
      dataExportConfig.currentbatchcount = spark.sql("select count(*) from " + dataExportConfig.tablename + " where " + checkColumn + ">=" + typeOffset).collect()(0).getAs[Long](0)
      // 获取hive数据发送到kafka。
      spark.sql("select * from " + dataExportConfig.tablename + " where " + checkColumn + ">=" + typeOffset).toKafkaDF(dataExportConfig.tablename)
        .write
        .option("kafka.bootstrap.servers", Constant.KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", Constant.KAFKA_TOPIC)
        .format("kafka").mode("append").save()
      dataExportConfig.batchend_ts = CommonUtil.getKuduTimestampFromLong(System.currentTimeMillis())
      // etl记录写入kudu表
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


