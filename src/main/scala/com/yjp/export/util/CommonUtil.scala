package com.yjp.export.util

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.JSON
import com.yjp.export.RunApplication.DataExportConfig
import org.apache.commons.lang.time.FastDateFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types._

import scala.util.control.Breaks.{break, breakable}

/**
  * @author dingjingbo
  * @email jingbo.ding@foxmail.com
  * @version 1.0.0
  * @Description 通用工具类
  * @createTime 2020/4/20 13:52
  */
object CommonUtil {

  /**
    * 判断是否是JSON字符串
    *
    * @param str
    * @return
    */
  /*def isJSON(str: String) = {
    try {
      JSON.parseObject(str)
      true
    } catch {
      case _ => false
    }
  }*/

  /**
    * 将驼峰式命名的字符串转换为下划线大写方式。如果转换前的驼峰式命名的字符串为空，则返回空字符串。</br>
    * 例如：helloWorld->hello_world
    *
    * @param name 转换前的驼峰式命名的字符串
    * @return 转换后下划线大写方式命名的字符串
    */
  def underscoreName(name: String): String = {
    val result = new StringBuilder
    if (name != null && name.length > 0) {
      // 将第一个字符处理成大写
      result.append(name.substring(0, 1).toUpperCase)
      // 循环处理其余字符
      for (i <- 1 until name.length) {
        val s = name.substring(i, i + 1)
        // 在大写字母前添加下划线
        if (s == s.toUpperCase && !Character.isDigit(s.charAt(0)) && !("_" == s)) result.append("_")
        // 其他字符直接转成大写
        result.append(s.toUpperCase)
      }
    }
    result.toString.toLowerCase
  }

  /**
    * 将下划线大写方式命名的字符串转换为驼峰式。如果转换前的下划线大写方式命名的字符串为空，则返回空字符串。</br>
    * 例如：hello_world->helloWorld
    *
    * @param name 转换前的下划线大写方式命名的字符串
    * @return 转换后的驼峰式命名的字符串
    */
  def camelName(name: String, firstUpperCase: Boolean): String = {
    val result = new StringBuilder
    // 快速检查
    if (name == null || name.isEmpty) { // 没必要转换
      return ""
    } else if (!name.contains("_")) { // 不含下划线，仅将首字母小写
      if (!firstUpperCase) return name.substring(0, 1).toLowerCase + name.substring(1)
      else return name.substring(0, 1).toUpperCase + name.substring(1)
    }
    // 用下划线将原始字符串分割
    val camels = name.split("_")
    for (camel <- camels) {
      // 跳过原始字符串中开头、结尾的下换线或双重下划线
      breakable(if (camel.isEmpty) break)
      // 处理真正的驼峰片段
      if (result.length == 0 && !firstUpperCase) { // 第一个驼峰片段，全部字母都小写
        result.append(camel.toLowerCase)
      }
      else { // 其他的驼峰片段，首字母大写
        result.append(camel.substring(0, 1).toUpperCase)
        result.append(camel.substring(1).toLowerCase)
      }
    }
    result.toString
  }

  /**
    * 获取类对应的 StructType
    *
    * @param clazz
    * @return
    */
  def getStructType(clazz: Class[_]) = {
    val structFields = clazz.getDeclaredFields.map(x => {
      val name = x.getName
      val dataType = if (x.getType == classOf[String]) {
        StringType
      } else if (x.getType == classOf[Double]) {
        DoubleType
      } else if (x.getType == classOf[java.lang.Double]) {
        DoubleType
      } else if (x.getType == classOf[Int]) {
        IntegerType
      } else if (x.getType == classOf[Integer]) {
        IntegerType
      } else {
        StringType
      }
      StructField(name, dataType, false)
    })
    StructType(structFields)
  }

  /**
    * 获取hive库名表名
    *
    * @param name
    * @return
    */
  def getTableName(name: String) = {
    val default_database = "yjp_dm_tag"
    if (name.contains(".")) {
      (default_database, name)
    } else {
      val names = name.split(".")
      (names(0), names(1))
    }
  }

  def getYesterDay(format: String = "yyyy-MM-dd"): String = {
    val dateFormat = FastDateFormat.getInstance(format)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def main(args: Array[String]): Unit = {
    println(System.currentTimeMillis())
    println(getTimestamp4Long(System.currentTimeMillis()))
  }


  /**
    * 返回查询时间范围，根据传入查询的起始时间，和分区中获取的最大时间来确定查询范围
    * 增量查询条件:传入时间(startDay)>=分区中最大值，则增量查询
    *
    * @param partitionsRow 所需查询表所有分区的row。
    * @param startDay      需要查询的开始时间。
    * @return (是否增量查询，起始时间，结束时间)
    */
  def getTimeHorizon(partitionsRow: Array[Row], startDay: Int) = {

    val list = List[Int]()
    for (row <- partitionsRow) {
      val partition: Int = row.get(0).asInstanceOf[Int]
      list.+:(partition)
    }
    // 获取当前时间的年月日
    val yesterDay = getYesterDay("yyyyMMdd").toInt
    val maxPartition = list.max
    val minPartition = list.min
    // 查询的上限
    val endTime: Int = if (maxPartition >= yesterDay) yesterDay else maxPartition
    // 判断 startDay
    val result = if (endTime >= startDay && startDay >= minPartition) {
      (false, startDay, endTime)
    } else if (startDay < minPartition) {
      (false, minPartition, endTime)
    } else {
      (true, endTime, endTime)
    }
    result
  }

  /**
    * 时间戳转换为Int
    */
  def dataTimeFormat[T](date: Date, format: String = "yyyyMMdd"): T = {
    val simpleDateFormat = new SimpleDateFormat(format)
    if (date != null) {
      simpleDateFormat.format(date).asInstanceOf[T]
    } else {
      "0".asInstanceOf[T]
    }
  }

  def getTimestamp4Long(time: Long): Timestamp = {
    val date = new Date(time)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    Timestamp.valueOf(sdf.format(date))
  }


  def getQueryTableName(etlMetas: Array[Row]): String = {
    val yesterDay = getYesterDay("yyyyMMdd").toInt
    var tableName: String = ""
    var flag = true
    for (i <- etlMetas.indices if flag) {
      for (j <- 0 until etlMetas(i).length if flag) {
        var lastBatch: Int = 0
        if (etlMetas(i).getAs[Date](etlMetas(i).length - 1) == null) {
          lastBatch = yesterDay
        } else {
          lastBatch = dataTimeFormat(etlMetas(i).getDate(etlMetas(i).length - 1))
        }
        // 获取到最新的一个今天没有调度的表
        if (lastBatch == yesterDay || lastBatch == 0) {
          tableName = etlMetas(i).getAs[String](1)
          flag = false
        } else {
          break()
        }
      }
    }
    tableName
  }

  /**
    * 需要确定如何查询，需要根据telMeta中的信息进行判断查询哪一条
    *
    * @param databaseTableName
    * @param queryCondition
    * @return
    */
  def getQuerySql(databaseTableName: String, queryCondition: (Boolean, Int, Int)): (String, String) = {
    val querySB = new StringBuilder
    val conditionSB = new StringBuilder
    val countSB = new StringBuilder
    querySB.append("select * from ").append(databaseTableName)
    countSB.append("select count(1) from ").append(databaseTableName)
    if (queryCondition._1) {
      // 增量查询
      conditionSB.append(" where day=").append("'").append(queryCondition._2).append("'")
      countSB.append(conditionSB.toString())
      querySB.append(conditionSB.toString())
    } else {
      // 批量查询
      conditionSB.append(" where day >=").append("'").append(queryCondition._2).append("' and day <=").append("'").append(queryCondition._3).append("'")
      countSB.append(conditionSB.toString())
      querySB.append(conditionSB.toString())
    }
    (querySB.toString(), countSB.toString())
  }


  def getKuduTimestampFromLong(time: Long): Timestamp = {
    var date = new Date(time)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val instance = Calendar.getInstance
    instance.setTime(date)
    instance.add(Calendar.HOUR, 8)
    date = instance.getTime
    Timestamp.valueOf(sdf.format(date))
  }


}
