package com.yjp.export.util

import org.apache.spark.sql.Dataset

object Predef {

  implicit class DatasetWapper[T](ds: Dataset[T]) {

    import ds.sparkSession.implicits._

    /**
     * 将数据更新到mysql中，
     */
    def save2Mysql() = {
    }
  }

}
