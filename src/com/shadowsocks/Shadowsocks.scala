package com.shadowsocks

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.TimestampType

object Shadowsocks {
  def main(args: Array[String]): Unit = {
    val Array(targetDay, registerTable, logTable, url) = args
    val spark = SparkSession.builder().appName("shadowsocks").enableHiveSupport().getOrCreate()
    val regTableDF = ReadTableCommon.readShadowSocks(spark)(logTable, targetDay)
    val logTableDF = ReadTableCommon.readHistory(spark)(registerTable, targetDay)
    val retainDF   = ReadTableCommon.retainAddData(spark)(targetDay, regTableDF, logTableDF)
    retainDF.schema
    .add("id",LongType,false)
    .add("target_day",DateType,true)
    .add("retained_day",DateType,true)
    .add("retained_day_num",IntegerType,true)
    .add("retained_person_num",IntegerType,true)
    .add("create_time",TimestampType,false)
    ReadTableCommon.outJDBCData(spark)(url, retainDF, "shadowsocks_retain")
    spark.close()
  }
}