package com.shadowsocks

import org.apache.spark.sql.SparkSession
import com.util.DateUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Dataset, Row, SparkSession }
import org.apache.spark.sql.functions.{ when, _ }
import java.util.Properties

object ReadTableCommon {
  /**
   * 读取当日登陆用户数
   */
  def readShadowSocks(spark: SparkSession)(logTable: String, targetDay: String) = {
    import spark.implicits._
    spark.read.table(logTable).where($"login_time" === targetDay)
      .select($"login_time", $"ip_send")
      .distinct()
  }

  /**
   * 读取留存数据
   */
  def readHistory(spark: SparkSession)(registerTable: String, targetDay: String) = {
    val (seven_day, fourteen_day, thirty_day) = (DateUtil.getDateByDay(targetDay, -7), DateUtil.getDateByDay(targetDay, -14), DateUtil.getDateByDay(targetDay, -30))
    import spark.implicits._
    spark.read.table(registerTable)
      .filter(($"register_time" <= targetDay && $"register_time" >= seven_day) || $"register_time" === fourteen_day || $"register_time" === thirty_day)
      .select($"id", $"ip_send", $"ip_end", $"register_time")
      .distinct
  }

  /**
   * 留存统计公共模块
   */
  def retainAddData(spark: SparkSession)(targetDay: String, regTableDF: DataFrame, logTableDF: DataFrame) = {
    import spark.implicits._
    regTableDF.join(logTableDF, Seq("ip_send"))
      .groupBy("register_time").agg(count("ip_send").as("retained_person_num"))
      .withColumn("target_day", when($"retained_person_num".isNotNull, targetDay))
      .select($"target_day", $"register_time".as("retained_day"), datediff($"target_day", $"register_time").as("retained_day_num"), $"retained_person_num")
  }

  /**
   * 将DataFrame的数据输入到mysql数据库
   */
def outJDBCData(spark: SparkSession)(url: String, regTableDF: DataFrame, tableName: String) = {
//    val url = "jdbc:mysql://192.168.131.155:3306/hadoop?characterEncoding=UTF-8"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "root");// 设置用户名
    connectionProperties.setProperty("password", "root");// 设置密码
//    regTableDF.write.jdbc(url, "shadowsocks_retain", connectionProperties)//新建数据库表，并添加数据（数据库中必须没有数据表格）
    regTableDF.write.mode("append").jdbc(url, tableName, connectionProperties)//在数据表中追加数据
  }
  
  
}