package com.yjp.export.util

import com.alibaba.fastjson.JSON
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
   * @param str
   * @return
   */
  def isJSON(str: String)={
    try {
      JSON.parseObject(str)
      true
    }catch {
      case _ => false
    }
  }

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
      for(i <- 1 until name.length){
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
   * @param clazz
   * @return
   */
  def getStructType(clazz:Class[_])={
    val structFields = clazz.getDeclaredFields.map(x=>{
      val name = x.getName
      val dataType = if(x.getType==classOf[String]){
        StringType
      }else if(x.getType==classOf[Double]){
        DoubleType
      }else if(x.getType==classOf[java.lang.Double]){
        DoubleType
      }else if(x.getType==classOf[Int]){
        IntegerType
      }else if(x.getType==classOf[Integer]){
        IntegerType
      }else{
        StringType
      }
      StructField(name,dataType,false)
    })
    StructType(structFields)
  }

}
