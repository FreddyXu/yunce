package scala.yunce9.streaming

import java.sql.Connection
import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.pow
import scala.util.matching.Regex
import scala.yunce9._
import scala.yunce9.streaming.MySpark._
import scala.yunce9.streaming.MySpark.spark.implicits._
import scala.yunce9.streaming.SaveOrUpdataSql.queryAndDelete
import scala.yunce9.utils.SparkUtil.NowDate
import scala.yunce9.utils.{KafkaSink, MysqlConn, SparkUtil}

object IntervalAnalyzeTest {
  val url = "jdbc:mysql://ycmysql.web.zz:3306/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
  val regex: Regex = "[“《#][\\u4e00-\\u9fa5\\w\\,， ]{5,}[#》”]".r
  val send_kafka = "yunce_out"
  val empty_desc = s"${" " * 8}您本次的网信健康综合评测结果为：健康。\n${" " * 8}网信健康管理是一个长期、持续的过程，关注您的家人、朋友及同事的关联风险也是管理的重要一环。请您务必保持定期评测的良好习惯，防患未然。"

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    //    val dates_ago = SparkUtil.days_ago_pre(30)
    sqlreader.option("dbtable", "t_search_task").load.createOrReplaceTempView("search_task")
    sqlreader.option("dbtable", "t_task_data").load.repartition(10).createOrReplaceTempView("t_task_data")
    //KafkaStreaming.getTaskData(200,600,10).createOrReplaceTempView("t_task_data")
    //    sqlreader.option("dbtable", "t_real_task").load.withColumnRenamed("id", "real_id").createOrReplaceTempView("t_real_task")
    //3568,3443,3450,3295,3564,3567,3299,3509,3317,3360,3363,3520,3408  3133
    val source = Source.fromFile("D:/gitProject/yunce9/src/datas/real_ids", "UTF-8")
    val lines: Array[String] = source.getLines.toArray
    //    val str = """{"realId":40,"searchId":40}""" //{"searchId":3260,"realId":180}
    val sourRdd = sc.makeRDD(lines) //417 220 1133 365 902
    val start = System.currentTimeMillis()
    analyze(sourRdd)
    println(s"用时：${(System.currentTimeMillis() - start) / 1000.0} s ")
    spark.close()

  }


  /**
    * 云测核心分析入口
    *
    * @param rdd kafka收到的任务id
    */
  def analyze(rdd: RDD[String]): Unit = {
    //解析kafka收到的数据
    val real_search = rdd.filter(_.contains("}")).map(a => {
      val gs = new Gson()
      println(a)
      gs.fromJson(a, classOf[ReceiveKafka])
    }).collect().filter(_ != null)
    val real_ids: Array[String] = real_search.map(m => m.realId).distinct
    val ids = real_ids.mkString(",")
    //打印收到的任务ID,用于对比查看是否有遗漏的任务未做处理
    println($"${NowDate()} INFO 当批待分析的所有任务id：$ids")
    sqlreader.option("dbtable", "t_real_task").load.withColumnRenamed("id", "real_id").createOrReplaceTempView("t_real_task")
    val personalDF = sqlreader.option("dbtable", "t_personal_risk_result").load
    personalDF.createOrReplaceTempView("t_personal_risk_result")
    val task_map: Map[Long, String] = personalDF.where(s"real_id in ($ids) ").rdd.map(r => r.getAs[Long]("real_id") -> SparkUtil.longToString(r.getAs[Long]("p_create_time"))).groupByKey.mapValues(_.max).collect().toMap
    println(NowDate())
    val allData: (Dataset[TaskInfo], Array[SearchData]) = getAllData(real_search)
    //    println(allData._1.count())
    val result = batchClusters(allData._1, allData._2, task_map)
    try {
      allData._1.unpersist()
      result.show()
    } catch {
      case _: NullPointerException =>
        println(s"${NowDate()} ERROR 批数据$ids 任务异常，无分析结果！")
    }
  }

  /**
    * 获取业务所需数据
    *
    * @param real_search 搜索关键词 id
    * @return
    */
  def getAllData(real_search: Array[ReceiveKafka]): (Dataset[TaskInfo], Array[SearchData]) = {
    val real_ids = real_search.filter(_ != null).map(_.realId)
    val real_string = real_ids.mkString(",")
    val search_ids = real_search.map(_.searchId)
    val real_str = if (real_ids.length == 1) s"=$real_string" else s"in ($real_string)"
    val search_str = if (search_ids.length == 1) s"= ${search_ids.mkString(",")}" else s"in (${search_ids.mkString(",")})"
    //    println(matchdf.rdd.getNumPartitions)
    val get_test_time = udf((create_time: Long) => {
      val now_time = System.currentTimeMillis()
      val time = now_time - create_time
      if (time > 3600000) now_time else create_time
    })
    spark.udf.register("get_test_time", get_test_time)
    val getSearch = s"SELECT real_id,search_id,get_test_time(rt.create_time) as test_time,search_keys nick_name,is_vip " +
      s"FROM search_task st " +
      s"JOIN t_real_task rt ON rt.search_id=st.id " +
      s"where rt.real_id $real_str"
    val search_data = spark.sql(getSearch)
    //    search_data.show()
    val searchArr = search_data.as[SearchData].collect()

    val matchdf: Dataset[TaskInfo] = getInfoData(search_str, 10, 37)
      .where(s"present_json !='' and present_json!='null' and present_json is not null")
      .withColumn("sourceName", $"platform")
      .withColumnRenamed("search_id", "searchID")
      .withColumnRenamed("search_name", "nickName")
      .withColumnRenamed("main_body_json", "mainBody_json")
      .as[PresentSql]
      .flatMap(m => {
        val gson = new Gson()
        val presents = if (m.present_json != "null") gson.fromJson(m.present_json, classOf[Array[Present]]) else null
        searchArr.filter(f => f.search_id == m.searchID).map(p => TaskInfo(p.real_id, p.is_vip,p.test_time, p.nick_name, p.search_id, m.id, m.publish_date, m.author, m.platform, m.appraise, m.risk_value, m.label, m.heat, getInfoTitle(m.title, m.nickName), m.topic_words, m.content_words, presents))
      })

    val infoDF: Dataset[TaskInfo] = matchdf.persist() //getInfo.as[TaskInfo]
    (infoDF, searchArr)
  }

  /**
    * 分区读取t_data_nfo表数据
    *
    * @param parts 分区数
    * @param range 每个分区的查询天数
    * @return
    */
  def getInfoData(search_str: String, parts: Int = 6, range: Int = 5): DataFrame = {
    val predicates = 1.to(parts).toArray.map(a => {
      if (a == 1) SparkUtil.NowDate() -> SparkUtil.days_ago_pre(a * range)
      else SparkUtil.days_ago_pre((a - 1) * range) -> SparkUtil.days_ago_pre(a * range)
    }).map {
      case (start, end) => s"publish_date < '$start' AND publish_date >= '$end' AND search_id $search_str"
    }
    val prop = new java.util.Properties
    prop.setProperty("user", "zzyq")
    prop.setProperty("password", "1qaz2WSX!@")
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    //    val url = ConfigFactory.load("config.conf").getString("ycmysql.web.zz")
    spark.read.jdbc(url, "t_matchs_data", predicates, prop)
  }

  /**
    * 批处理分析接口
    *
    * @param ds       所有有效数据
    * @param task_map 任务map
    * @return
    */
  def batchClusters(ds: Dataset[TaskInfo], searchArr: Array[SearchData], task_map: Map[Long, String]): Dataset[SeRiskResult] = {
    //   real_id,search_id,rt.create_time test_time,search_keys nick_name,is_vip
    var returnDS: Dataset[SeRiskResult] = null
    val use_ids = ds.map(_.real_id).collect()
    //    val searchArr: Array[SearchData] = spark.sql("select * from t_search_task").as[SearchData].collect()
    val diff = searchArr.map(_.real_id).diff(use_ids)
    //如果有测试任务数据完全为空
    if (diff.nonEmpty) {
      println(s"${NowDate()} INFO 有空数据!")
      playEmptyData(searchArr, diff).show()
    }
    //如果测试任务的数据不为空
    if (ds != null && !ds.isEmpty) {
      //      println("非空数据")
      val tfRdd: RDD[(Long, TaskInfo)] = ds.rdd.map(a => a.real_id -> a)
      //      val searchDF = spark.sql("select real_id,nick_name,test_time,is_vip from t_search_task").persist()
      val searchSet = searchArr.map(m => m.real_id -> m).toMap
      val historys: mutable.Seq[(Long, Double, Long, Int)] = getHistory(real_id = task_map.keys.mkString(","))
      returnDS = tfRdd.filter(_._2.content_words != null).groupByKey.flatMap(m => {
        val test_time = searchSet(m._1).test_time
        val nick_name = searchSet(m._1).nick_name.split(" ")(1)
        val events: (Seq[SeEventDataNew], Seq[SeEventDataNew], Seq[SeEventDataNew]) = getClusters(m._2.toSeq, task_map)
        val yearEvent = events._1
        val queraseEvent = events._2
        val recentEvent = events._3
        val yearData: Seq[TaskInfo] = m._2.filter(_.publish_date >= SparkUtil.days_ago_pre(365)).toSeq
        val queraseData = m._2.filter(_.publish_date >= SparkUtil.days_ago_pre(90)).toSeq
        val recentData = m._2.filter(_.publish_date >= SparkUtil.days_ago_pre(30)).toSeq
        val empty_desc = s"${" " * 8}您本次的网信健康综合评测结果为：健康。\n${" " * 8}网信健康管理是一个长期、持续的过程，关注您的家人、朋友及同事的关联风险也是管理的重要一环。请您务必保持定期评测的良好习惯，防患未然。"
        val yearResult: SeRiskResult = if (yearData.nonEmpty) getDescrible(getMatchs(yearData), getEventMap(yearEvent), historys, 3) else SeRiskResult(99.0, empty_desc, test_time, nick_name, m._1, m._1, "", 3, "", "")
        val queraseResult: SeRiskResult = if (queraseData.nonEmpty) getDescrible(getMatchs(queraseData), getEventMap(queraseEvent), historys, 2) else SeRiskResult(99.0, empty_desc, test_time, nick_name, m._1, m._1, "", 2, "", "")
        val recentResult: SeRiskResult = if (recentData.nonEmpty) getDescrible(getMatchs(recentData), getEventMap(recentEvent), historys, 1) else SeRiskResult(99.0, empty_desc, test_time, nick_name, m._1, m._1, "", 1, "", "")
        //组合不同数据区间分析结果用于批量入库
        //      queryAndDelete(Seq(m._1))
        val realResult: Array[SeRiskResult] = Array(yearResult, queraseResult, recentResult)
        //      realResult.foreach(println)
        saveToRiskResult(realResult) //写入最终结果表数据！
        try {
          getkafka.send(send_kafka, s"${m._1}",s"""{"real_id":"${m._1}"}""")
          println(s"${NowDate()} INFO 任务${m._1} $nick_name kafka发送成功！")
        } catch {
          case _: Exception =>
            println(s"${NowDate()} ERROR 任务${m._1} $nick_name kafka发送失败！")
        }
        realResult
        //          .foreach(println)
      }).toDS()
    }
    returnDS
  }

  /**
    * 获取数据历史测评结果数据
    *
    * @param real_id 任务id
    * @return
    */
  def getHistory(real_id: String): mutable.ArrayBuffer[(Long, Double, Long, Int)] = {
    val conn: Connection = MysqlConn.connectMysql()
    val querySql = conn.createStatement()
    //    val real_id = "2547,2567"
    val realBuff = new ArrayBuffer[(Long, Double, Long, Int)]()
    try {
      val query = querySql.executeQuery(s"select real_id,risk_value,p_create_time,result_type from t_personal_risk_result where real_id in ($real_id)")
      while (query.next()) {
        val real_id = query.getLong("real_id")
        val risk_value = query.getDouble("risk_value")
        val p_create_time = query.getLong("p_create_time")
        val result_type = query.getInt("result_type")
        realBuff.append((real_id, risk_value, p_create_time, result_type))
      }
    } catch {
      case _: Exception => println(s"${NowDate()} INFO ${real_id}任务为全新任务！")
      //        e.printStackTrace()
    }
    realBuff
  }

  /**
    * 匹配关键词
    *
    * @param infoDF 用于解析匹配业务关键词的数据
    * @return
    */
  def getMatchs(infoDF: Seq[TaskInfo]): Seq[Match] = {
    val matchData = infoDF.flatMap(all => {
      val name = try {
        all.nick_name.split(" ")(1)
      } catch {
        case _: ArrayIndexOutOfBoundsException => all.nick_name
      }
      val matchs: Seq[Match] = all.presents.map(m => {
        if (m.weight != null && m.weight != "null") Match(all.real_id, name, all.test_time, all.is_vip, all.search_id, all.id, all.publish_date, m.weight.toDouble, all.heat, all.title, all.topic_words, all.appraise, all.author, m.present, m.word, m.use, m)
        else Match(all.real_id, name, all.test_time, all.is_vip, all.search_id, all.id, all.publish_date, 1.0, all.heat, all.title, all.topic_words, all.appraise, all.author, m.present, m.word, m.use, m)
      })
      matchs
    })
    matchData
  }

  /**
    * 聚类函数，调用聚类算法对数据进行聚类
    *
    * @param dataArr 参与聚类数据
    */
  def getClusters(dataArr: Seq[TaskInfo], task_map: Map[Long, String]):
  (Seq[SeEventDataNew], Seq[SeEventDataNew], Seq[SeEventDataNew]) = {

    ClusterAnalyze.taskClusters(dataArr.toArray, task_map)

  }

  /**
    * 获取事件描述
    *
    * @param clustersDatas 事件数据
    * @return
    */
  def getEventMap(clustersDatas: Seq[SeEventDataNew]): String = {
    val M2 = clustersDatas.filter(_.event_appraise == "负面")
    var xg = ""
    if (M2.size < 1) {
      val M1 = clustersDatas.size
      xg = s"与您相关的事件共${M1}个"
    } else {
      xg = s"与您相关的风险事件共${M2.size}个"
    }
    var M3 = 0
    //    var Mlabel = ""
    var new_risk_label = ""
    val M3S = clustersDatas.filter(f => f.is_new == 1 && f.risk_value <= 88 && f.label != "").map(_.label -> 1)
    if (M3S.nonEmpty) {
      val max_labes = M3S.groupBy(_._1).mapValues(_.size).maxBy(_._2)
      M3 = max_labes._2
      val Mlabel = max_labes._1
      new_risk_label = s"，新增${M3}个${Mlabel}类高风险事件"
    }
    xg + "|" + new_risk_label
  }


  /**
    * 获取周季年所有区间数据的最终返回结果
    *
    * @param matchs        匹配到关键词的数据
    * @param eventMap      事件描述数据
    * @param historyValues 历史测评数据
    * @param section       数据区间
    * @return
    */
  def getDescrible(matchs: Seq[Match], eventMap: String, historyValues: Seq[(Long, Double, Long, Int)], section: Int): SeRiskResult = {
    val head = matchs.head
    val test_time = head.test_time
    val real_id = head.real_id
    val search_id = head.search_id
    val nick_name = head.nick_name // r.getAs[String]("nick_name")
    //负面分析结果
    val neg_analyze = try {
      negAnalyze(matchs.filter(_.appraise == "负面")).head
    } catch {
      case _: NoSuchElementException => null
    }
    val base_value = if (neg_analyze == null) 100 else neg_analyze.base_value
    val influence_weight = if (neg_analyze == null) 1.0 else neg_analyze.influenceWeight
    val negs_presents: Seq[PresentShows] = if (neg_analyze != null) neg_analyze.words_size else null
    //正面分析结果
    val pos_analyze = try {
      posAnalyze(matchs.filter(_.appraise == "正面")).head
    } catch {
      case _: NoSuchElementException => null
    }
    val pos_weight = if (pos_analyze == null) 1.0 else pos_analyze.pos_weight
    val poss_presents: Seq[PresentShows] = if (pos_analyze != null) pos_analyze.show else null
    val value = if (base_value * pos_weight > 100) 100 * influence_weight else base_value * influence_weight * pos_weight
    val lastValue = if (value >= 99) 99.0 else value.intValue()
    var risk_level = "健康"
    var fame = ""
    lastValue match {
      case b if b >= 96.0 =>
        risk_level = "健康"
        fame = "网信健康管理需整体统筹，自身、家人、朋友及同事均是管理的重要一环，请勿忽视。"
      case c if c >= 81.0 && c < 96.0 =>
        risk_level = "良好"
        fame = "网信健康管理需整体统筹，自身、家人、朋友及同事均是管理的重要一环，请勿忽视。"
      case d if d >= 62 && d < 81 =>
        risk_level = "亚健康"
        fame = "网信健康风险的变化是动态但有迹可循的，请保持定期评测的良好习惯，防患未然。"
      case e if e >= 44 && e < 62 =>
        risk_level = "微恙"
        fame = "网信健康管理是一个长期的、与己有益的过程，请务必坚持。"
      case f if f < 44 =>
        risk_level = "危重"
        fame = "网信健康管理是一个长期的、与己有益的过程，请务必坚持。"
    }
    val presents: Seq[PresentShows] = if (negs_presents == null && poss_presents != null) {
      poss_presents
    } else if (negs_presents != null && poss_presents == null) {
      negs_presents
    } else if (negs_presents != null && poss_presents != null) {
      negs_presents.union(poss_presents)
    } else {
      Seq()
    }

    val his_values = try {
      val value = historyValues.filter(f => f._1 == real_id && f._4 == section).maxBy(_._3)._2.intValue()
      val Z = lastValue.intValue() - value.intValue()
      if (Z > 0) "，健康度环比上升" + (Z * 100 / value.toDouble).intValue() + "%。"
      else if (Z < 0) "，健康度环比下降" + (-Z * 100 / value.toDouble).intValue() + "%。"
      else "，健康度环比无变化。"
    } catch {
      case e: Exception => ""
    }
    val presentShows = if (presents.nonEmpty) {
      presents.groupBy(_.key).map(v => {
        val ids = v._2.sortBy(_.publish_data).map(_.id).distinct
        val key_size = ids.length
        val key_ids = ids.mkString(",")
        s"${v._1}:$key_size|$key_ids" -> key_size
      }).toList.sortBy(-_._2).map(_._1).mkString(";")
    } else ""
    val event_influence = eventMap.split("\\|")(0) + "new_event_desc。【云测】将密切关注相关舆情动向，助您及时发现舆情异常状态。"
    val kg = " " * 8
    val event_describe = s"${kg}您本次的网信健康综合评测结果为：${risk_level}result_ratio\n${kg}现阶段，$event_influence"
    var describe = ""
    val new_event_desc = try {
      eventMap.split("\\|")(1)
    } catch {
      case _: Exception => ""
    }
    val result_ratio = his_values
    //  if (total_au > 0) describe = s"$kg$event_describe\n$kg，$fame。$post_part$neg_part\n$kg"
    describe = s"$event_describe\n$kg$fame"
    SeRiskResult(lastValue, describe, test_time, nick_name, real_id, search_id, presentShows, section, result_ratio, new_event_desc)
  }


  /**
    * 空数据分析入库操作
    *
    * @param searchArr 分析任务的简单数据
    * @param diff      分析任务和有相关数据的任务id差集
    */
  def playEmptyData(searchArr: Array[SearchData], diff: Array[Long]): Dataset[SeRiskResult] = {
    val emptyArr: Array[SeRiskResult] = searchArr.filter(f => diff.contains(f.real_id)).flatMap(p => {
      val nick_name = p.nick_name.split(" ")(1)
      val yearResult: SeRiskResult = SeRiskResult(99.0, empty_desc, p.test_time, nick_name, p.real_id, p.search_id, "", 3, "", "")
      val queraseResult: SeRiskResult = SeRiskResult(99.0, empty_desc, p.test_time, nick_name, p.real_id, p.search_id, "", 2, "", "")
      val recentResult: SeRiskResult = SeRiskResult(99.0, empty_desc, p.test_time, nick_name, p.real_id, p.search_id, "", 1, "", "")
      val realResult: Array[SeRiskResult] = Array(yearResult, queraseResult, recentResult)
      //realResult.foreach(println)
      saveToRiskResult(realResult) //写入最终结果表数据！
      try {
        getkafka.send(send_kafka, s"${p.real_id}",s"""{"real_id":"${p.real_id}"}""")
        println(s"${NowDate()} INFO 空数据任务${p.real_id}:$nick_name kafka发送完毕！")
      } catch {
        case _: Exception =>
          println(s"${NowDate()} ERROR 空数据任务${p.real_id}:$nick_name kafka发送完毕！")
      }

      realResult
    })
    spark.createDataset(emptyArr)
  }


  /**
    * 负面数据批处理分析
    *
    * @param negaDS 已匹配的负面数据
    * @return
    */
  def negAnalyze(negaDS: Seq[Match]): Seq[NegAnalyze] = {
    val analyze = negaDS.map(md => (md.real_id, md.nick_name, md.search_id) -> md).groupBy(_._1).map(mats => {
      val fus: Seq[Present] = mats._2.map(_._2).map(_.present) //负面扣分对象
      val first_value: (String, String, Double) = getRiskValue(fus)
      val test_time = mats._2.map(_._2).map(_.test_time).max
      //      first_value.foreach(println)
      val baseValue: Double = first_value._3 //搜索任务基础风险值
      //      val lables: Seq[(String, Double)] = first_value._1
      val label: Seq[String] = Seq(first_value._1)
      //fus.map(p => result_influence_weight(p.second_level)).minBy(_._2)
      val result_influences = fus.map(m => m.result_influence_weight.toDouble).toList.distinct
      val influenceWeight = if (result_influences.size < 2) result_influences.head else result_influences.sortBy(s => s).take(2).sum / 2.0 //结果影响权重
      val author_size: Int = mats._2.map(_._2).map(_.author).filter(f => f.length > 1).toSet.size
      val words_size: Seq[PresentShows] = mats._2.map(_._2).filter(_.use == "描述词").map(a => PresentShows(a.key, a.id, SparkUtil.stringToLong(a.publish_date))).map(a => {
        a.key = a.key + "-"
        a
      })
      NegAnalyze(mats._1._1, mats._1._2, mats._1._3, test_time, baseValue, influenceWeight, words_size, author_size, label)
    })
    analyze.toSeq
  }

  /**
    * 获取一级风险值
    *
    * @param fus Fumian数据迭代器
    * @return
    */
  def getRiskValue(fus: Seq[Present], is_risk: Int = 0): (String, String, Double) = {
    val second_level_score = fus.groupBy(f => (f.first_level, f.second_level))
      .mapValues(v => 100 - getCalValue(v)).toArray
    //    second_level_score.foreach(println)
    val max_label: ((String, String), Double) = second_level_score.minBy(_._2)
    val power_size = 11 - second_level_score.length.toDouble
    val product = second_level_score.map(m => m._2).product * pow(100.0, power_size)
    var ESQR = 0.0
    if (is_risk == 0) ESQR = pow(product, 1.0 / 11).formatted("%.2f").toDouble
    else ESQR = (100 - pow(product, 1.0 / 11)).formatted("%.2f").toDouble
    (max_label._1._1, max_label._1._2, ESQR)
  }

  /**
    * 清洗标题
    *
    * @param title    聚类划分的簇
    * @param nickname 云测任务的主体
    * @return
    */
  def getInfoTitle(title: String, nickname: String): String = {
    val wb = regex.findFirstIn(title)
    var new_title: String = ""
    //    《杨紫被指责抢陈钰琪的戏...
    if (wb.nonEmpty) new_title = wb.get.replaceAll("[“#”]", "")
    else {
      val sp = title.split("[,；，。【】?？!！—\\-\\_]|[\\./]{2,}|发布了头条文章：")
      val zh = if (sp.length == 1) {
        val kong = title.split(" ").filter(_.length > 4)
        if (kong.isEmpty) Array(title) else kong
      } else sp.flatMap(_.split(" "))
      if (zh.isEmpty) new_title = title else {
        val nick = zh.filter(f => nickname.exists(n => f.contains(n))).filter(_.length > 4)
        new_title = if (nick.isEmpty) zh.maxBy(m => m.replaceAll("[\\w]+", "").length)
        else nick.maxBy(m => m.replaceAll("[\\w]+", "").length)
      }
    }
    new_title.replaceAll("&quot;|\\[转帖\\]|转载", "")
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
    val fx_value = fumians.map(f => f.cal_score.toDouble * f.weight.toDouble).sum
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
    * 正面数据分析
    */
  def posAnalyze(matchDS: Seq[Match]): Seq[PosAnalyze] = {
    val result = matchDS.groupBy(g => (g.real_id, g.nick_name, g.search_id)).map(kv => {
      val md = kv._2
      val test_time = md.map(_.test_time).max
      //计算单个搜索任务的正面影响权重
      val posWeight = md.map(a => {
        if (a.present.single_weight == null) (a.real_id, 1.0) -> a.word
        else (a.real_id, a.present.single_weight.toDouble) -> a.word
      }).groupBy(_._1)
        .map(g => (g._1, g._1._2 * g._2.size)).maxBy(_._2)._1._2
      val posKeys = md.groupBy(_.key).map(a => {
        val ids = a._2.map(_.id).toSet
        (a._1, ids.size, ids.mkString(","))
      }).toList.sortBy(-_._2)
      val key_count: Seq[PresentShows] = md.map(a => PresentShows(a.key, a.id, SparkUtil.stringToLong(a.publish_date)))
      val post_keys: String = {
        if (posKeys.size > 3) posKeys.map(_._1).take(3).mkString("、")
        else if (posKeys.isEmpty) ""
        else posKeys.map(_._1).mkString(" ")
      }
      //获取作者相关信息
      val sort_au = md.map(_.author).filter(f => f.length > 1).groupBy(p => p).toList.sortBy(-_._2.size)
      val posAuthor = {
        if (sort_au.size >= 3) sort_au.take(3).map(_._1).mkString("、")
        else if (sort_au.isEmpty) ""
        else sort_au.map(_._1).mkString("、")
      }
      PosAnalyze(kv._1._1, kv._1._2, kv._1._3, test_time, posWeight + 1.0, key_count, posAuthor, sort_au.size, post_keys)
    })
    result.toSeq
  }

  /**
    * 获取kafka发送
    *
    * @return
    */
  def getkafka: KafkaSink[String, String] = {
    //kafka生产者，用以发送更新的数据
    val kafkaProducer: KafkaSink[String, String] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "10.253.100.30:9092,10.253.100.31:9092,10.253.100.32:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      KafkaSink[String, String](kafkaProducerConfig)
    }
    kafkaProducer
  }

  /**
    * 写入结果表
    *
    * @param iter iter
    */
  def saveToRiskResult(iter: Seq[SeRiskResult]): Int = {
    val conn: Connection = MysqlConn.connectMysql()
    var status = 0
    val real_ids = iter.map(_.real_id)
    if (real_ids.isEmpty) println("查询数据为空!") else queryAndDelete(real_ids)
    //删除完毕，开始插入数据
    conn.setAutoCommit(false)
    val create_time = System.currentTimeMillis()
    val sql = "replace into t_personal_risk_result(risk_value,risk_desc,nick_name,real_id,present_words,p_create_time,test_time,result_type,result_ratio,new_event_desc) values (?,?,?,?,?,?,?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    iter.foreach(t => {
      //      println(t.nick_name,t.se)
      statement.setFloat(1, t.risk_value.toFloat)
      statement.setString(2, t.risk_desc)
      statement.setString(3, t.nick_name)
      statement.setLong(4, t.real_id)
      statement.setString(5, t.present_words)
      statement.setLong(6, create_time)
      statement.setLong(7, t.test_time)
      statement.setInt(8, t.se)
      statement.setString(9, t.result_ratio)
      statement.setString(10, t.new_event_desc)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
      status = 1
    }
    status
  }

}