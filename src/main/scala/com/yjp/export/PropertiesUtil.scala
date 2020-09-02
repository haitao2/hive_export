package com.yjp.export

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def apply(file: String = "default.properties") = {
    val pro = new Properties()
    pro.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(file), "UTF-8"))
    pro
  }

  def getValue(key: String): String = {
    apply().getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    println(PropertiesUtil.getValue("kafka_topic"))
  }
}