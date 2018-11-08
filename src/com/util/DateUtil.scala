package com.util

import java.util.Calendar
import java.text.SimpleDateFormat

object DateUtil {
  private val YYYY_MM_DD = "yyyy-MM-dd"

  /**
   * 获取指定日期前或者后index日的指定日期
   * @param target_day
   * @param index
   */
  def getDateByDay(target_day: String, index: Int) = {
    val smp = new SimpleDateFormat(YYYY_MM_DD)
    val ca = Calendar.getInstance()
    ca.setTime(smp.parse(target_day))
    ca.add(Calendar.DATE, index)
    smp.format(ca.getTime)
  }

  /**
   * 计算两个日期之间的天数
   * @param start_day 开始日期
   * @param end_day 结束日期
   * @return 返回两个日期之间的天数
   */
  def getDayByDate(start_day: String, end_day: String) = {
    val smp = new SimpleDateFormat(YYYY_MM_DD)
    val start_date = smp.parse(start_day)
    val end_date = smp.parse(end_day)
    ((end_date.getTime - start_date.getTime) / (1000 * 24 * 3600)).toInt
  }

  def main(args: Array[String]): Unit = {
    println(getDayByDate("2018-03-25", "2018-06-25"))
  }
}