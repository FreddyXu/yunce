package scala.yunce9.structured


import com.google.gson.Gson
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{explode, split, to_json, udf}

import math._
import scala.yunce9._
import scala.yunce9.streaming.KafkaStreaming._
import MySpark._
import scala.collection.mutable.ArrayBuffer
import scala.yunce9.utils.{MysqlConn, SparkUtil, TimesUtil}
import spark.implicits._


object RematchData {

  def main(args: Array[String]): Unit = {
    getAllMatchData()
  }

  /**
    * 获取业务所需数据
    *
    * @param real_search 搜索关键词id
    * @return
    */
  def getAllMatchData() = {
    val getweight = udf((a: Double) => a * 0 + 1.0)
    val negative_words = sqlreader.option("dbtable", "t_negative_words").load().withColumn("word", explode(split($"cal_words", ";"))).withColumn("weight", getweight($"cal_num")).drop("cal_words", "id").cache()
    val positive_words = sqlreader.option("dbtable", "t_positive_words").load().withColumn("word", explode(split($"key_words", ";"))).drop("key_words", "key", "limit_weight", "id").cache()
    val present_words = sqlreader.option("dbtable", "t_present_words").load().withColumn("word", explode(split($"words", ";"))).drop("words").cache()
    val neg_pos_words = negative_words.join(positive_words, Seq("word", "first_level", "second_level"), "full")
    val all = present_words.join(neg_pos_words, "word").drop("id").as[Present].collect()
    //    val map_present = all.map(m => m.word -> m).toMap
    val neg_words = all.filter(_.appraise == "负面")
    val pos_words = all.filter(_.appraise == "正面")
    val source_influence = sqlreader.option("dbtable", "t_source_influence").load().map(a => (a.getAs[String]("source_name"), a.getAs[Double]("source_weight"))).collect()

    val matchdf: Dataset[MatchPresent] = getInfoData(30, 37)
      //      .where(s"present_json!='null'")
      .withColumnRenamed("search_id", "searchId")
      .withColumnRenamed("search_name", "nickName")
      .withColumnRenamed("main_body_json", "mainBody_json")
      .as[PresentSql]
      .map(m => {
      val gson = new Gson()
      val presents = if (m.present_json != "null") gson.fromJson(m.present_json, classOf[Array[Present]]) else null
      val mainBody = if (m.mainBody_json != "null") m.mainBody_json.replaceAll("[\\[\\]]", "").split(" ") else null
      MatchPresent(m.searchID, m.nickName, m.id, m.author,m.content_words,m.platform, m.publish_date, m.risk_value, m.label, m.appraise, m.heat, m.title, m.topic_words, mainBody, presents)
    })
    val matchDataDf = matchdf.map(df => {
//      val contentwords =
      val matchData: MatchPresent = MatchPresent(df.searchID, df.nickName, df.id, df.author,df.content_words,df.platform, df.publish_date, 100.0, "", df.appraise, df.heat, df.title, df.topic_words, df.mainBody, null)
      val words = df.mainBody.toArray
      if (df.appraise == "负面") {
        //计算负面数据的来源权重*时间衰减权重
        val fu_presents: ArrayBuffer[Present] = new ArrayBuffer[Present]()
        val mainBody = MainBody(df.searchID, df.nickName, df.id, df.title, df.topic_words.replaceAll("[^\\u0000-\\uFFFF]", ""), df.author, df.content_words,df.platform, df.publish_date, df.appraise, df.heat, SparkUtil.NowDate(), df.mainBody.toArray)
        val weight = time_source_processing(mainBody, source_influence)
        neg_words.foreach(n => {
          if (n.word.contains("\\+")) {
            val p2 = n.word.split("\\+")
            n.weight = weight.toString
            df.mainBody.foreach(w => if (w.contains(p2(0)) && w.contains(p2(1))) fu_presents.append(n))
          } else {
            df.mainBody.foreach(w => if (w.contains(n.word)) fu_presents.append(n))
          }
        })
        //负面数据匹配不为空
        if (fu_presents.nonEmpty) {
          val result = getEarlyWarn(mainBody, fu_presents) //预警数据写入!
          matchData.label = result.type_label
          matchData.presents = fu_presents
          matchData.risk_value = result.risk_value
        }
      }
      else {
        val zm_match: ArrayBuffer[Present] = new ArrayBuffer[Present]()
        pos_words.foreach(p => {
          //  val word_set = p.word.split("\\+").toSet
          //  if (word_set.intersect(words) == word_set) zm_match.append(p)
          if (p.word.contains("\\+")) {
            val p2 = p.word.split("\\+")
            words.foreach(w => if (w.contains(p2(0)) && w.contains(p2(1))) zm_match.append(p))
          } else {
            words.foreach(w => if (w.contains(p.word)) zm_match.append(p))
          }
        })
        if (!(matchData.presents == null)) println(s"${SparkUtil.NowDate()} 有效数据 : $matchData")
        else println(s"${SparkUtil.NowDate()} 无效数据 : $matchData")
      }
      matchData
    }).withColumn("mainBody_json", to_json($"mainBody"))
      .withColumn("present_json", to_json($"presents"))
      .drop("presents", "mainBody").as[PresentSql]

    matchDataDf.foreachPartition(f => saveToMacthcs(f.toSeq))

    println(matchDataDf.count())
  }

  def time_source_processing(df: MainBody, source_influence: Array[(String, Double)]): Double = {
    //计算负面数据的来源权重
    val source_weight: Double = try {
      source_influence.find(_._1 == df.sourceName).head._2
    } catch {
      case e: Exception => 1.0
    }

    val hour2ms = 3600000.0
    //2013-02-27 11:39:00
    if (df.publishDate.length == 19) {
      val t_publish = TimesUtil.convertDateStr2TimeStamp(df.publishDate, pattern = TimesUtil.SECOND_DATE_FORMAT)
      val create_time = TimesUtil.convertDateStr2TimeStamp(df.createTime, pattern = TimesUtil.SECOND_DATE_FORMAT)
      val t_sub = Math.abs(t_publish - create_time) / hour2ms //时间戳相减后换算成小时
      //匹配对应的权值
      val t_sub_time = t_sub match {
        case s if s < 0.5 => 2.5 //  小于30分钟  2.5
        case s if s < 1 && s >= 0.5 => 2 //  小于1小时且大于等于30分钟  2
        case s if s < 168 && s >= 1 => 1.5 // 小于1周且大于等于1小时 1.5
        case s if s < 5040 && s >= 168 => 1.25 // 小于1个月且大于等于1周 1.25
        case s if s < 30240 && s >= 5040 => 1.0 //  小于半年且大于等于1个月  1
        case s if s < 30240 && s >= 30240 => 2 //  小于1年且大于等于半年 0.75
        case _ => 0.5 //  大于等于1年 0.5
      }
      t_sub_time * source_weight
    }
    else 1.0 * source_weight //当publist为空值时 时间衰减权重为1
  }

  def getEarlyWarn(info: MainBody, matchWords: Seq[Present]): UpDateInfo = {
    val first_value = oneDataProcess(matchWords)
    val risk_value: Double = first_value._3 //搜索任务基础风险值
    val heat = info.heat
    val warn_level = risk_value match {
      case v if v < 50 => "红色"
      case v if v >= 50 && v <= 77 => "橙色"
      case v if v > 77 && v <= 88 => "黄色"
      case _ => ""
    }
    //红：【0,53.52】 紧急  黄：【53.53,89.34】 重要  绿：【89.35,100】一般
    val import_level = risk_value match {
      case v if v <= 53.52 => "红色"
      case v if v >= 53.53 && v <= 89.34 => "黄色"
      case _ => "绿色"
    }
    var upData: UpDateInfo = null
    upData = if (risk_value <= 88) UpDateInfo(info.id, first_value._1, risk_value, warn_level, import_level, 1, heat) else UpDateInfo(info.id, first_value._1, risk_value, warn_level, import_level, 0, heat)
    SqlSave.updateWarnSql(upData)
    println(upData)
    upData
  }


  def oneDataProcess(matchWords: Seq[Present]): (String, String, Double) = {
    matchWords.map(a => (a.first_level, a.second_level) -> a).groupBy(_._1)
    val second_level_score = matchWords.groupBy(f => (f.first_level, f.second_level))
      .mapValues(v => 100.0 - getCalValue(v)).toArray
    //    second_level_score.foreach(println)
    val max_label = second_level_score.minBy(_._2)
    val power_size = 11 - second_level_score.length.toDouble
    val product = second_level_score.map(_._2).product * pow(100, power_size)
    val ESQR = pow(product, 1.0 / 11).formatted("%.2f").toDouble
    (max_label._1._1, max_label._1._2, ESQR)
  }


  /**
    * 获取二级指标的扣分值
    *
    * @param fumians fumians
    * @return
    */
  def getCalValue(fumians: Seq[Present]): Double = {
    val head = fumians.head
    val num = fumians.size
    val fx_value = fumians.map(f => f.cal_score.toDouble * f.weight.toDouble).sum // * f.weight
    val second_score = if (num > head.cal_num.toInt) {
      val value = fx_value + head.cal_num_limit.toDouble
      if (value < head.cal_limit.toDouble) value else head.cal_limit.toDouble
    } else {
      val value = fx_value
      if (fx_value < head.cal_limit.toDouble) value else head.cal_limit.toDouble
    }
    second_score
  }

  def saveToMacthcs(value: Seq[PresentSql]): Unit = {
    val conn = MysqlConn.connectMysql()
    val sql = "replace into t_matchs_data(search_id,search_name,id,author,platform,publish_date,risk_value,label,appraise,heat,title,topic_words,main_body_json,present_json,content_words) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val statement = conn.prepareStatement(sql)
//    conn.setAutoCommit(false)
    value.foreach(t => {
      val title = t.title.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", "")
      statement.setLong(1, t.searchID)
      statement.setString(2, t.nickName)
      statement.setLong(3, t.id)
      statement.setString(4, t.author)
      statement.setString(5, t.platform)
      statement.setString(6, t.publish_date)
      statement.setDouble(7, t.risk_value)
      statement.setString(8, t.label)
      statement.setString(9, t.appraise)
      statement.setDouble(10, t.heat)
      statement.setString(11, title)
      statement.setString(12, t.topic_words)
      statement.setString(13, t.mainBody_json)
      statement.setString(14, t.present_json)
      statement.setString(15, t.content_words)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
//      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
      conn.close()
    }
  }
}
