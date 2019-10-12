package scala.yunce9.utils

import java.text.SimpleDateFormat

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime

object TimesUtil {


  val ONE_HOUR_MILLISECONDS = 60 * 60 * 1000

  val SECOND_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"

  val DAY_DATE_FORMAT_ONE = "yyyy-MM-dd"

  val DAY_DATE_FORMAT_TWO = "yyyyMMdd"

  //时间字符串=>时间戳
  def convertDateStr2TimeStamp(dateStr: String, pattern: String): Long = {
    new SimpleDateFormat(pattern).parse(dateStr).getTime
  }


  //时间字符串+天数=>时间戳
  def dateStrAddDays2TimeStamp(dateStr: String, pattern: String, days: Int): Long = {
    convertDateStr2Date(dateStr, pattern).plusDays(days).date.getTime
  }

  //时间字符串+小时数=>时间戳
  def dateStrAddHours2TimeStamp(dateStr: String, pattern: String, hours: Int): Long = {
    convertDateStr2Date(dateStr, pattern).plusHours(hours).date.getTime
  }
  //时间字符串=>日期
  def convertDateStr2Date(dateStr: String, pattern: String): DateTime = {
    new DateTime(new SimpleDateFormat(pattern).parse(dateStr))
  }


  //时间戳=>日期
  def convertTimeStamp2Date(timestamp: Long): DateTime = {
    new DateTime(timestamp)
  }

  //时间戳=>字符串
  def convertTimeStamp2DateStr(timestamp: Long, pattern: String): String = {
    new DateTime(timestamp).toString(pattern)
  }

  //时间戳=>小时数
  def convertTimeStamp2Hour(timestamp: Long): Long = {
    new DateTime(timestamp).hourOfDay().getAsString().toLong
  }


  //时间戳=>分钟数
  def convertTimeStamp2Minute(timestamp: Long): Long = {
    new DateTime(timestamp).minuteOfHour().getAsString().toLong
  }

  //时间戳=>秒数
  def convertTimeStamp2Sec(timestamp: Long): Long = {
    new DateTime(timestamp).secondOfMinute().getAsString.toLong
  }


  def addZero(hourOrMin: String): String = {
    if (hourOrMin.toInt <= 9)
      "0" + hourOrMin
    else
      hourOrMin

  }

  def delZero(hourOrMin: String): String = {
    var res = hourOrMin
    if (!hourOrMin.equals("0") && hourOrMin.startsWith("0"))
      res = res.replaceAll("^0", "")
    res
  }
}