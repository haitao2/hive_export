package com.yjp.export.util


import org.apache.spark.sql.Dataset
import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Predef {

  implicit class DatasetWapper[T](ds: Dataset[T]) {

    import ds.sparkSession.implicits._

    /**
     * 生成kafka适配的df
     */
    def toKafkaDF(keyField: String) = {
      ds.toDF().map(x => {
        // val key = x.getAs[String](keyField)
        val valuesMap = x.getValuesMap[Object](x.schema.fieldNames)
        val json = new JSONObject()
        for (fieldName <- x.schema.fieldNames) {
          json.put(fieldName, x.getAs(fieldName))
        }
        (keyField, json.toJSONString)
      }).toDF("key", "value")
    }
  }

  def main(args: Array[String]): Unit = {
    val res = StructType(Seq(StructField("age", StringType, false)))
    println(res.names(0))


  }
}
