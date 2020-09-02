package com.yjp.export

import java.util.Properties

import com.yjp.export.common.Constant
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducerUtil {
  def sendMessage(line: String): Unit = {
    val brokerList = Constant.KAFKA_BOOTSTRAP_SERVERS
    println("======================================")
    println("kafkaBrokerList" + brokerList)
    val topic = Constant.KAFKA_TOPIC
    // 创建kafka消费者
    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    // key、value的序列化器
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("kafka.security.protocol", "SASL_PLAINTEXT")
    prop.put("sasl.mechanism", "GSSAPI")
    prop.put("sasl.kerberos.service.name", "kafka")
    val kafkaProducer = new KafkaProducer[String, String](prop)
    println("生产者创建成功")
    kafkaProducer.send(new ProducerRecord[String, String](topic, line))
    println("数据发送成功")
  }

}
