package scala.yunce9.structured

import com.google.gson.Gson
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.summary.TextRankKeyword
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.math.pow
import scala.yunce9._
import scala.yunce9.structured.MySpark._
import spark.implicits._
import scala.yunce9.utils.{SparkUtil, TimesUtil}


object DataMatchAnalize {
  //  HanLP.Config.IOAdapter = new HadoopFileIoAdapter()
  val segment: Segment = HanLP.newSegment().enableCustomDictionaryForcing(true).enableAllNamedEntityRecognize(true)
  val kafkaServer = "10.253.100.30:9092,10.253.100.31:9092,10.253.100.32:9092"
  val getRank = new TextRankKeyword()
  val kafka_topic = "info_formal_data"

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")
    //    val dates_ago = SparkUtil.days_ago_pre(30)
    val getweight = udf((a: Double) => a * 0 + 1.0)
    val negative_words = sqlreader.option("dbtable", "t_negative_words").load().withColumn("word", explode(split($"cal_words", ";"))).withColumn("weight", getweight($"cal_num")).drop("cal_words", "id").cache()
    val positive_words = sqlreader.option("dbtable", "t_positive_words").load().withColumn("word", explode(split($"key_words", ";"))).drop("key_words", "key", "limit_weight", "id").cache()
    val present_words = sqlreader.option("dbtable", "t_present_words").load().withColumn("word", explode(split($"words", ";"))).drop("words").cache()
    val neg_pos_words = negative_words.join(positive_words, Seq("word", "first_level", "second_level"), "full")
    val all_presents = present_words.join(neg_pos_words, "word").drop("id").as[Present].collect()
    //    val map_present = all.map(m => m.word -> m).toMap
    val neg_words = all_presents.filter(_.appraise == "负面")
    val pos_words = all_presents.filter(_.appraise == "正面")
    val source_influence: Map[String, Double] = sqlreader.option("dbtable", "t_source_influence").load().map(a => (a.getAs[String]("source_name"), a.getAs[Double]("source_weight"))).collect().toMap
    // 获取kafka数据
    val kafkaDS = spark.readStream
      .format("kafka")
      .option("failOnDataLoss", "false")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", kafka_topic)
      .load().selectExpr("CAST(value AS STRING)")
      .as[String]
    val dataInfo: Dataset[MainBody] = kafkaDS.filter(a => a.contains("}")).map(m => {
      val gs = new Gson()
      val json = m.replace("type", "types")
      var mainBody: MainBody = null
      try {
        val df = gs.fromJson(json, classOf[MainBodyKafka])
        if (df.createTime == null || df.createTime == "") df.createTime = SparkUtil.NowDate()
        val body: Array[String] = if (df.mainBody == null) Array() else df.mainBody.replaceAll("[\\[\\]\'\"]|[^\\u0000-\\uFFFF]", "").split(",")
        mainBody = MainBody(df.searchId, df.nickName, df.id, df.title, df.contentWords.replaceAll("[^\\u0000-\\uFFFF]|[\'\"]", ""), df.author, df.platform, df.sourceName, df.publishDate, df.appraise, df.heat, df.createTime, body)
      } catch {
        case _: Exception =>
          val df = gs.fromJson(json, classOf[MainBody])
          if (df.createTime == null || df.createTime == "") df.createTime = SparkUtil.NowDate()
          val body: Array[String] = if (df.mainBody != null) df.mainBody else Array()
          mainBody = MainBody(df.searchId, df.nickName, df.id, df.title, df.contentWords, df.author, df.platform, df.sourceName, df.publishDate, df.appraise, df.heat, df.createTime, body)
      }
      mainBody
    }).withColumn("timestamp", to_timestamp($"createTime", "yyyy-MM-dd HH:mm:ss"))
      .withWatermark("timestamp", "5 minutes")
      .as[MainBody].where("appraise in ('负面','正面')")

    //匹配与测试主体相关的业务正负面关键词
    val matchDF: Dataset[MatchPresent] = dataInfo.map(df => {
      val heat = df.heat
      val words = df.mainBody.map(m => m.replaceAll(s"${df.nickName}|[<>p/]| ", "")).toSet.filter(_.length > 1)
      val topic_words: String = if (df.contentWords == null || df.contentWords == "null") "" else participle(df.contentWords, rank = true)._2.mkString(" ")
      val matchData: MatchPresent = MatchPresent(df.searchId, df.nickName, df.id, df.author, df.contentWords, df.platform, df.publishDate, 100.0, "", df.appraise, heat, df.title, topic_words, words.toSeq, null)
      if (df.appraise == "负面") {
        //计算负面数据的来源权重*时间衰减权重
        val source_weight = source_processing(df, source_influence)
        val fu_presents: ArrayBuffer[Present] = new ArrayBuffer[Present]()
        neg_words.foreach(n => {
          if (n.word.contains("\\+")) {
            n.weight = source_weight.toString
            val p2 = n.word.split("\\+")
            words.foreach(w => if (w.contains(p2(0)) && w.contains(p2(1))) fu_presents.append(n))
          } else {
            words.foreach(w => if (w.contains(n.word)) fu_presents.append(n))
          }
        })
        //负面数据匹配不为空
        if (fu_presents.nonEmpty) {
          val result = getEarlyWarn(df, fu_presents) //预警数据写入!
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
        //正面数据匹配不为空
        if (zm_match.nonEmpty) matchData.presents = zm_match
      }

      if (!(matchData.presents == null)) println(s"${SparkUtil.NowDate()} 有效数据 : $matchData")
      else println(s"${SparkUtil.NowDate()} 无效数据 : $matchData")
      matchData
    })
    // .filter(r => !(r.presents == null))
    //构造入库的数据json,方便保存
    val saveSqlDF: Dataset[PresentSql] = matchDF
      .withColumn("mainBody_json", to_json($"mainBody"))
      .withColumn("present_json", to_json($"presents"))
      .drop("presents", "mainBody").as[PresentSql]
    //    val writer = new MysqlSink
    val writer = new MysqlSink
    val query = saveSqlDF.writeStream
      //.format("console")
      .foreach(writer)
      //      .option("checkpointLocation", "file:///D:/gitProject/yunce9/src/datas/checkpoint")
      .option("checkpointLocation", "hdfs://calcluster/yunce/checkpoint")
      //      .option("checkpointLocation","hdfs://hadoop1:9000/yunce/checkpoint")
      .start()

    query.awaitTermination()
  }

  /**
    * 预警数据写入
    *
    * @param info       主体数据
    * @param matchWords 匹配到的关键词呈现集合
    * @return
    */
  def getEarlyWarn(info: MainBody, matchWords: Seq[Present]): UpDateInfo = {
    //整合数据影响权重
    matchWords.foreach(m => m.weight = (m.weight.toDouble * time_processing(info)).toString)
    val first_value: (String, String, Double) = getRiskValue(matchWords)
    val risk_value: Double = first_value._3
    //搜索任务基础风险值
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
    //    if (risk_value <= 88)
    SqlSave.updateWarnSql(upData)
    upData
  }

  /**
    * 获取数据风险值
    *
    * @param fus Fumian数据迭代器
    * @return
    */
  def getRiskValue(fus: Iterable[Present]): (String, String, Double) = {
    val second_level_score = fus.groupBy(f => (f.first_level, f.second_level))
      .mapValues(v => 100 - getCalValue(v)).toArray
    //    second_level_score.foreach(println)
    val max_label: ((String, String), Double) = second_level_score.minBy(_._2)
    val power_size = 11 - second_level_score.length.toDouble
    val product = second_level_score.map(m => m._2).product * pow(100.0, power_size)
    val ESQR = pow(product, 1.0 / 11).formatted("%.2f").toDouble
    (max_label._1._1, max_label._1._2, ESQR)
  }

  /**
    * 获取二级指标的扣分值
    *
    * @param fumians 负面数据
    * @return
    */
  def getCalValue(fumians: Iterable[Present]): Double = {
    val head = fumians.head
    val num = fumians.size
    val fx_value = fumians.map(f => f.cal_score.toDouble * f.weight.toDouble).sum //
    val second_score = if (num > head.cal_num.toInt) {
      val value = fx_value + head.cal_num_limit.toDouble
      if (value < head.cal_limit.toDouble) value else head.cal_limit.toDouble
    } else {
      val value = fx_value
      if (fx_value < head.cal_limit.toDouble) value else head.cal_limit.toDouble
    }
    second_score
  }

  /**
    * 封装分词方法
    *
    * @param sentence   文本内容
    * @param stop_words 是否启用停用词处理
    * @return
    */
  def participle(sentence: String, stop_words: Boolean = true, rank: Boolean = false): (Seq[String], Seq[String]) = {
    var str = sentence.replaceAll("尊敬的读者[.]+|网页链接}[!@#$%&*()]-|<p>|</p>|", "")
    if (sentence == null) str = ""
    var result: List[String] = null
    var topic_words: List[String] = null
    if (sentence.nonEmpty) {
      val segs = segment.seg(str)
      if (stop_words) CoreStopWordDictionary.apply(segs)
      result = segs.map(_.word).toList
      if (rank) {
        //是否只提取主题关键词
        val keys = getRank.getKeywords(segs, 20)
        val words = keys.map(m => segs.find(t => t.word == m).head).filter(a => a.nature.startsWith("v") || (a.nature.startsWith("n") && a.word.length > 1))
        topic_words = words.map(m => if (m.nature.startsWith("nr")) m.word + "/nr" else m.word).toList
      }
    }
    else {
      result = List()
      topic_words = List()
    }
    (result, topic_words)
  }

  def source_processing(df: MainBody, source_influence: Map[String, Double]): Double = {
    //计算负面数据的来源权重
    val source_weight: Double = try {
      source_influence(df.sourceName)
    } catch {
      case e: Exception => 1.0
    }
    source_weight
  }

  def time_processing(df: MainBody): Double = {
    val hour2ms = 3600000.0
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
        case s if s < 30240 && s >= 30240 => 0.75 //  小于1年且大于等于半年 0.75
        case _ => 0.5 //  大于等于1年 0.5
      }
      t_sub_time
    }
    else 1.0
  }
}

