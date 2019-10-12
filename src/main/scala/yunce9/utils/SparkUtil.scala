package scala.yunce9.utils

import java.text.SimpleDateFormat
import java.util.{Date, Locale}



object SparkUtil {
  /**
    * 过滤一个字符串中是否包含list中的某个字符串,返回值为boolern型
    *
    * @param list
    * @param string
    * @return Boolean
    */
  def filterStr(list: List[String], string: String): Boolean = {
    var exist = false
    list.map(p => {
      if (string.contains(p))
        exist = true
    })
    exist
  }

  /**
    * 统计文章中某单词出现的次数
    *
    * @return
    */
  def countString(content: String, keyword: String): Int = {
    val result = content.toString
    var count = 0
    var index = 0
    var fi = true
    while (fi) {
      index = result.indexOf(keyword, index + 1)
      if (index > 0) count += 1
      else fi = false //todo: break is not supported
    }
    count
  }

  /**
    * 统计关键词在文章中出现的次数
    *
    * @param content
    * @param word
    * @return
    */
  def countKeys(content: String, word: String): (String, Int) = {
    val war = word.split(";")
    val wordcount = {
      if (war.size > 1) {
        val ar1 = SparkUtil.countString(content, war(0))
        val ar2 = SparkUtil.countString(content, war(1))
        if (ar1 > 0 && ar2 > 0 && (ar1 - ar2) > 0) word -> ar2
        else if (ar1 > 0 && ar2 > 0 && (ar1 - ar2) >= 0) word -> ar1
        else word -> 0
      } else word -> SparkUtil.countString(content, word)
    }
    wordcount
  }


  def stringToLong(string: String) = {
    val loc = new Locale("en")
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", loc)
    val dt2 = fm.parse(string);
    dt2.getTime
  }

  /**
    * long型时间戳转字符串时间
    *
    * @param time
    * @return
    */
  def longToString(time: Long): String = {
    TimesUtil.convertTimeStamp2DateStr(time, TimesUtil.SECOND_DATE_FORMAT)
  }

  //获取当前时间字符串
  def NowDate(tord: Boolean = true): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = dateFormat.format(now)
    if (tord) date else date.split(" ")(0)
  }

  //获取n天以后的时间
  //SECOND_DATE_FORMAT
  def daysAgo(days: Int, tord: Boolean = true): String = {
    val now: String = NowDate()
    var daysAgoLong = TimesUtil.dateStrAddDays2TimeStamp(now, TimesUtil.DAY_DATE_FORMAT_ONE, days)
    val daysAddString = longToString(daysAgoLong)
    if (tord) daysAddString else daysAddString.split(" ")(0)
  }

  //获取n天以后的时间
  //SECOND_DATE_FORMAT
  def hoursAgo(hours: Int, tord: Boolean = true): String = {
    val now: String = NowDate()
    var daysAgoLong = TimesUtil.dateStrAddHours2TimeStamp(now, TimesUtil.SECOND_DATE_FORMAT, hours)
    val daysAddString = longToString(daysAgoLong)
    if (tord) daysAddString else daysAddString.split(" ")(0)
  }

  def  days_ago_pre(days: Int): String = {
    val now: String = NowDate()
    var daysAgoLong = TimesUtil.dateStrAddDays2TimeStamp(now, TimesUtil.SECOND_DATE_FORMAT, -days)
    val daysAddString = longToString(daysAgoLong)
    daysAddString
  }


  //某日期一天后的日期
  def oneDayAgo(startday: String): String = {
    val start: String = startday + " 00:00:00"
    val daysAgoLong = TimesUtil.dateStrAddDays2TimeStamp(start, TimesUtil.DAY_DATE_FORMAT_ONE, 1)
    val daysAddString = longToString(daysAgoLong).split(" ")(0)
    daysAddString
  }


  /**
    * 计算两个时间的天数差
    *
    * @param time1
    * @param time2
    * @return
    */
  def diffTime(time1: String, time2: String): Int = {
    val diffLong = SparkUtil.stringToLong(time1) - SparkUtil.stringToLong(time2)
    val diffLongs = diffLong / (1000 * 3600 * 24)
    diffLongs.toInt
  }


}

