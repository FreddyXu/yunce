package scala.yunce9.streaming

import com.hankcs.hanlp.mining.word2vec.{DocVectorModel, WordVectorModel}
import scala.yunce9.utils.{MysqlConn, SparkUtil}
import scala.collection.mutable.ArrayBuffer
import com.hankcs.hanlp.mining.word2vec
import java.sql.{Connection, ResultSet}
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.HanLP
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.yunce9._
import IntervalAnalyzeTest._


object ClusterAnalyze {
  val segment: Segment = HanLP.newSegment().enableCustomDictionaryForcing(true).enableAllNamedEntityRecognize(true)
  //  val docVectorModel = new DocVectorModel(new WordVectorModel("D:/HanLP/hanlp-wiki-vec-zh.txt"))
  val docVectorModel = new DocVectorModel(new WordVectorModel("/home/hadoop/Hanlp/hanlp-wiki-vec-zh.txt"))

  def taskClusters(datas: Array[TaskInfo], task_map: Map[Long, String]):
  (Seq[SeEventDataNew], Seq[SeEventDataNew], Seq[SeEventDataNew]) = {
    val real_id = datas.head.real_id
    var i = 0
    val histroyEvents: ArrayBuffer[HistoryEvent] = queryHistoryEvent(real_id)
    val incrementDatas = new ArrayBuffer[IncrementData]()
    for (i <- datas.indices) {
      val info = datas(i)
      info.title = clearTitle(info)
      //      println(info.title)
      var vec = docVectorModel.addDocument(i, info.title)
      if (vec == null) vec = docVectorModel.addDocument(i, info.title.split("").mkString(" "))
      incrementDatas.append(IncrementData(i, info, vec))
    }
    //    val dataMap = incrementDatas.map(m => m.input.id -> m).toMap
    var centers: ArrayBuffer[EventInfo] = new ArrayBuffer[EventInfo]()
    if (histroyEvents.nonEmpty) {
      centers = histroyEvents.map(m => {
        val increms = new ArrayBuffer[IncrementData]()
        incrementDatas.foreach(a => {
          if (a.processed == 0 && m.ids.contains(a.input.id)) {
            a.processed = 1
            increms.append(a)
          }
        })
        //获取聚类数据的均值向量
        val length = increms.size
        val mean: Array[Float] = if (length > 0) {
          increms.map(m => if (m.vector == null || m.vector.getElementArray.isEmpty) new word2vec.Vector(300) else m.vector).reduce((a, b) => a.add(b)).getElementArray.map(m => m / length)
        } else new word2vec.Vector(300).getElementArray

        val event_vector: word2vec.Vector = new word2vec.Vector(mean)
        EventInfo(increms, event_vector, m.event_title, m.event_summary, 1)
      }).filter(_.datas.nonEmpty)
    }
    val in_clusters: ArrayBuffer[EventInfo] = incrementClusters(centers, incrementDatas.toArray.filter(_.processed == 0).sortWith((a, b) => a.input.content_words.length > b.input.content_words.length))

    val events_section = getEvents(in_clusters, task_map)
    val event_result: Seq[SeEventDataNew] = events_section._1.union(events_section._2).union(events_section._3)
    //    println("写入事件数据!")
    saveToEvent(event_result)
    events_section
  }


  /**
    * 根据聚类结果获取的详细输出
    *
    * @param clusters 聚类划分的簇
    * @param task_map 单个云测任务对应的结果表入库时间
    */
  def getEvents(clusters: ArrayBuffer[EventInfo], task_map: Map[Long, String]):
  (Seq[SeEventDataNew], Seq[SeEventDataNew], Seq[SeEventDataNew]) = {
    val sectionEvens = clusters.filter(f => f != null && f.datas.nonEmpty).map(c => {
      //      val real_id = c.head.real_id
      val inputs = c.datas.map(_.input).toArray
      //      val nickname = inputs.head.nick_name.split(" ")
      val event_id = inputs.maxBy(_.publish_date).id
      val recent_hcData = inputs.filter(_.publish_date >= SparkUtil.days_ago_pre(30))
      val querase_hcData = inputs.filter(_.publish_date >= SparkUtil.days_ago_pre(90))
      val yearsEvents: SeEventDataNew = getSectionEvents(event_id, c.e_title, c.e_summary, inputs, task_map, 3)
      var queraseEvents: SeEventDataNew = null
      var recentEvents: SeEventDataNew = null
      if (querase_hcData.nonEmpty) {
        queraseEvents = getSectionEvents(event_id, c.e_title, c.e_summary, querase_hcData, task_map, 2)
        if (recent_hcData.nonEmpty) {
          recentEvents = getSectionEvents(event_id, c.e_title, c.e_summary, recent_hcData, task_map, 1)
        }
      }
      (yearsEvents, queraseEvents, recentEvents)
    })
    (sectionEvens.map(_._1), sectionEvens.map(_._2).filter(_ != null), sectionEvens.map(_._3).filter(_ != null))
  }


  /**
    * 增量算法核心逻辑
    *
    * @param centers  历史聚类数据
    * @param dv       待归簇聚类
    * @param distinct 余弦距离
    * @return
    */
  def incrementClusters(centers: ArrayBuffer[EventInfo], dv: Array[IncrementData], distinct: Double = 0.551):
  ArrayBuffer[EventInfo] = {
    if (centers.isEmpty) {
      val head = dv.head
      //      val content = head.input.content_words.split(" ").mkString("").replaceAll("<p>|</p>|\\-", " ")
      val title = head.input.title
      //      val summary = getSingleSummary(content)
      centers.append(EventInfo(ArrayBuffer(head), head.vector, title, ""))
      dv.head.processed = 1
      centers ++ incrementClusters(centers, dv.filter(_.processed == 0))
    }
    else {
      dv.foreach(f => {
        if (f.processed == 0) {
          if (f.vector == null) {
            println(s"${f.input.title}:有空向量！")
            f.vector = new word2vec.Vector(300)
          }
          centers.foreach(c => {
            if (c.vector == null) c.vector = new word2vec.Vector(300)
            //            println(c.vector)
            if (f.processed == 0) {
              if (c.vector.cosineForUnitVector(f.vector) >= distinct) {
                //     println(c.e_title + "<------>" + f.input.title)
                c.datas.append(f)
                f.processed = 1
              }
            }
          })
          if (f.processed == 0) {
            centers.append(EventInfo(ArrayBuffer(f), f.vector, f.input.title, ""))
            f.processed = 1
          }
        }
      })
    }
    centers.map(m => {
      val nickname = m.datas.head.input.nick_name
      if (m.is_history == 0) {
        val titles = m.datas.map(_.input.title).toArray
        val contents = m.datas.map(_.input.content_words.replaceAll(" ", "")).filter(_.length > 5).mkString("。")
        m.e_title = getEventTitle(titles, nickname)
        m.e_summary = getSingleSummary(contents)
        m
      } else m
    })
    //    centers
  }


  /**
    * 获取不同区间内的事件数据
    *
    * @param event_id    事件id
    * @param event_title 任务标题
    * @param c           事件数据
    * @param task_map    任务对应标签
    * @param se          数据区间标签
    * @return
    */
  def getSectionEvents(event_id: Long, event_title: String, event_summery: String, c: Array[TaskInfo], task_map: Map[Long, String], se: Int): SeEventDataNew = {
    // HanLP.convertToSimplifiedChinese(event_title)
    // real_id,test_time,event_title,risk_value,ids,event_heat,event_appraise,is_new,new_ids,event_words
    val ids_and_news = getEventIdsAndNew(c, task_map)
    val real_id = c.head.real_id
    val event_ids = ids_and_news._1
    val is_new = ids_and_news._2
    val new_ids = ids_and_news._3
    val event_heat = c.map(a => a.heat).sum
    val test_time = c.head.test_time
    var event_nature = ""
    var risk_value = 0.0
    if (c.exists(_.appraise == "负面")) {
      val risk_events = getRiskValue(c.filter(_.appraise == "负面").flatMap(_.presents), 1)
      risk_value = risk_events._3
      event_nature = risk_events._1
    }

    val event_words = getEventWrods(c)
    //    val event_risk = getRiskValue(c.flatMap(_.presents), 1)
    //    val risk_value = event_risk._3
    val event_appraise = if (c.map(_.appraise).contains("负面")) "负面" else "正面"
    //平台数据分布
    val plat_all_distribute = c.groupBy(_.platform).map(m => s"${m._1}:${m._2.length}").mkString(",")
    //负面数据平台分布
    val plat_neg_distribute = c.filter(_.appraise == "负面").groupBy(_.platform).map(m => s"${m._1}:${m._2.length}").mkString(",")
    //日期数量分布
    val date_counts = c.groupBy(_.publish_date.split(" ")(0)).toList.sortBy(_._1).map(m => s"${m._1}:${m._2.length}").mkString(",")

    //日期风险度数据
    val date_risks = c.groupBy(_.publish_date.split(" ")(0)).toList.sortBy(_._1).map(m => {
      var d_risk = 0.0
      if (m._2.exists(_.appraise == "负面")) {
        val risk_date = getRiskValue(m._2.filter(_.appraise == "负面").flatMap(_.presents), 1)
        d_risk = risk_date._3
      }
      s"${m._1}:$d_risk"
    }).mkString(",")
    //  real_id,test_time,event_title,risk_value,ids,event_heat,event_appraise,is_new,new_ids,event_words
    val sectionEvents: SeEventDataNew = SeEventDataNew(event_id, real_id, test_time, event_title, event_summery, risk_value, event_nature, event_ids.mkString(","), event_heat, event_appraise, is_new, new_ids.mkString(","), event_words, se, plat_all_distribute, plat_neg_distribute, date_counts, date_risks)
    sectionEvents
  }


  /**
    * 查询结果表并删除满足条件的历史数据
    *
    * @param real_ids 任务id
    */
  def queryHistoryEvent(real_ids: Long): ArrayBuffer[HistoryEvent] = {
    val conn: Connection = MysqlConn.connectMysql()
    val querySql = conn.createStatement()
    //    val real_ids = results
    //real_id: Long, event_title: String, event_summary: String, ids: ArrayBuffer[Long], event_vector: word2vec.Vector
    val query = querySql.executeQuery(s"select real_id,event_title,event_summary,ids,event_vector,risk_value from t_event_cluster where real_id = $real_ids and result_type=3")
    val realBuff = new ArrayBuffer[HistoryEvent]()
    var a = 0
    while (query.next()) {
      try {
        val ids = new ArrayBuffer[Long]()
        val real_id = query.getLong("real_id")
        val event_title = query.getString("event_title")
        val event_summary = query.getString("event_summary")
        val sql_ids = query.getString("ids").split(",")
        val sql_event_vector = query.getString("event_vector")
        val risk_value = query.getDouble("risk_value")
        sql_ids.foreach(id => ids.append(id.toLong))
        val event_vector = try {
          new word2vec.Vector(sql_event_vector.split(",").map(_.toFloat))
        } catch {
          case _: Exception =>
            a += 1
            docVectorModel.addDocument(a, event_title)
        }
        realBuff.append(HistoryEvent(real_id, risk_value, event_title, event_summary, ids, event_vector))
      }
      catch {
        case e: Exception => println(s"${SparkUtil.NowDate()} ERROR 历史事件数据查询异常！")
          e.printStackTrace()
      }
    }
    querySql.close()
    conn.close()
    //    println("查询事件完毕")
    realBuff
  }

  /**
    * 获取事件所含的文章id,是否为新事件，新事件标记的文章id
    *
    * @param arr      聚类划分的簇
    * @param task_map 单个云测任务对应的结果表入库时间
    * @return
    */
  def getEventIdsAndNew(arr: Array[TaskInfo], task_map: Map[Long, String]): (Array[Long], Int, Array[Long]) = {
    val real_id = arr.head.real_id
    val event_ids = arr.map(_.id).sortBy(p => p)
    var is_tested = ""
    try {
      is_tested = task_map(real_id)
    } catch {
      case _: Exception => is_tested = "2100-12-12 00:00:00"
    }
    val new_ids: Array[Long] = if (arr.exists(f => f.publish_date > is_tested)) arr.filter(f => f.publish_date > is_tested).map(_.id) else Array()
    val is_new: Int = if (event_ids.length == new_ids.length) 1 else 0

    (event_ids, is_new, new_ids)
  }


  /**
    * 获取单个事件的标题
    *
    * @param arr      聚类划分的簇
    * @param nickname 云测任务的主体
    * @return
    */
  def getEventTitle(arr: Array[String], nickname: String): String = {
    val titles = arr.map(str => {
      val wb = regex.findFirstIn(str)
      var title: String = ""
      //    《杨紫被指责抢陈钰琪的戏...
      if (wb.nonEmpty) title = wb.get.replaceAll("[“#”]", "")
      else {
        val sp = str.split("[,；，。【】?？!！—\\-\\_]|[\\./]{2,}|发布了头条文章：")
        val zh = if (sp.length == 1) {
          val kong = str.split(" ").filter(_.length > 4)
          if (kong.isEmpty) Array(str) else kong
        } else sp.flatMap(_.split(" "))
        if (zh.isEmpty) title = str else {
          val nick = zh.filter(f => nickname.exists(n => f.contains(n))).filter(_.length > 4)
          title = if (nick.isEmpty) zh.maxBy(m => m.replaceAll("[\\w]+", "").length)
          else nick.maxBy(m => m.replaceAll("[\\w]+", "").length)
        }
      }
      title
    })
    val nickTitles = titles.filter(f => f.contains(nickname))
    var event_title: String = if (nickTitles.isEmpty) titles.maxBy(m => m.replaceAll("[\\w]+", "").length) else nickTitles.maxBy(m => m.replaceAll("[\\w]+", "").length)
    event_title = if (event_title.contains("《") && event_title.contains("》")) event_title.replaceAll("[\\[@\\]\\|]", "") else event_title.replaceAll("[\\[@\\]\\|《》]", "")
    event_title
  }


  /**
    * 获取单个事件的标题
    *
    * @param df 聚类划分的簇
    * @return
    */
  def clearTitle(df: TaskInfo): String = {
    val wb = regex.findFirstIn(df.title)
    var title: String = ""
    //    《杨紫被指责抢陈钰琪的戏...
    if (wb.nonEmpty) title = wb.get.replaceAll("[“#”]|…{3,}.+", "")
    else {
      val sp = df.title.split("[,；，。【】?？!！—\\-\\_]|[\\./]{2,}|发布了头条文章：")
      val zh = if (sp.length == 1) {
        val kong = df.title.split(" ").filter(_.length > 4)
        if (kong.isEmpty) Array(df.title) else kong
      } else sp.flatMap(_.split(" "))
      if (zh.isEmpty) title = df.title
      else {
        val nick = zh.filter(f => f.contains(df.nick_name)).filter(_.length >= 2)
        title = if (nick.isEmpty) zh.maxBy(m => m.replaceAll("[\\w]+", "").length)
        else nick.maxBy(m => m.replaceAll("[\\w]+", "").length)
      }
    }
    title
  }

  /**
    * 获取摘要
    *
    * @param content 原文内容
    * @return
    */
  def getSingleSummary(content: String): String = {
    //    println(content)
    val con = content.replaceAll("网页链接|我发表了头条文章:", "")
    var summary = ""
    if (content != null) {
      if (content.length > 1) {
        summary = HanLP.extractSummary(con, 2, "[。？?！!；;▪]|</p>|<p>").mkString(";")
        if (summary.length > 200) summary = summary.substring(0, 100)
      }
    }
    summary
  }


  /**
    * 获取事件描述词
    *
    * @param arr 聚类划分的簇
    * @return
    */
  def getEventWrods(arr: Array[TaskInfo]): String = {
    val useWords: Set[(String, Double)] = arr.flatMap(p => p.presents.filter(_.use == "事件词").map(x => x.present -> p.risk_value)).toSet
    val event_words = try {
      useWords.groupBy(_._1).mapValues(v => {
        val values: Double = if (v.isEmpty) 1000.0 else v.minBy(_._2)._2
        values
      }).minBy(_._2)._1
    } catch {
      case e: Exception => ""
    }
    event_words
  }

  /**
    * 获取事件描述
    *
    * @param clustersDatas 事件数据
    * @return
    */

  def getEventMap(clustersDatas: Seq[SeEventData]): String = {
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
    * 写入事件表
    *
    * @param iter iter
    */

  def saveToEvent(iter: Seq[SeEventDataNew]): Unit = {
    val conn = MysqlConn.connectMysql()
    val real_ids = iter.head.real_id
    //    println("聚类个数： " + iter.size)
    //    val query = MysqlConn.connectMysql()
    val query_sql = s"select id,risk_value from t_event_cluster where real_id = $real_ids and result_type= 1"
    val data: ResultSet = conn.prepareStatement(query_sql).executeQuery()
    val risk_Map = mutable.Map[Long, Double]()
    while (data.next()) {
      try {
        val event_id = data.getLong("id")
        val risk_value = data.getDouble("risk_value")
        risk_Map.put(event_id, risk_value)
      }
      catch {
        case e: Exception => println("数据异常")
      }
    }

    iter.foreach(f = f => {
      val history_value: Double = try {
        risk_Map(f.id)
      } catch {
        case _: Exception =>
          //          println(s"新增事件：${f.event_title}")
          if (f.section == 1) {
            f.is_new = 1
            f.new_ids = f.ids
          }
          100
      }
      if (f.risk_value > 12 && history_value < 12 && f.section == 1) {
        println(s"$history_value<===>${f.risk_value}")
        f.new_warn = 1
      }
      if (f.risk_value > 12 && f.is_new == 1 && f.section == 1) {
        //        println(s"$history_value<===>${f.risk_value}")
        f.new_warn = 1
      }
    })

    //    val deleteConn = MysqlConn.connectMysql()

    val del_sql = s"delete from t_event_cluster where real_id = $real_ids "
    val statement_del = conn.prepareStatement(del_sql)
    try {
      statement_del.executeUpdate()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    //    val conn = MysqlConn.connectMysql()
    //real_id,test_time,event_title,risk_value,ids,event_heat,event_appraise,is_new,new_ids,event_words
    val sql = "replace into t_event_cluster(id,real_id,test_time,event_title,risk_value,ids,event_heat,event_appraise,is_new,new_ids,event_words,result_type,all_types,neg_types,nums,risks,event_nature,event_summary,new_warn) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    //    val statement_del = connEvent.createStatement()
    val statement = conn.prepareStatement(sql)
    //    conn.setAutoCommit(false)
    iter.foreach(t => {
      statement.setLong(1, t.id)
      statement.setLong(2, t.real_id)
      statement.setLong(3, t.test_time)
      statement.setString(4, t.event_title)
      statement.setDouble(5, t.risk_value)
      statement.setString(6, t.ids)
      statement.setDouble(7, t.event_heat)
      statement.setString(8, t.event_appraise)
      statement.setInt(9, t.is_new)
      statement.setString(10, t.new_ids)
      statement.setString(11, t.event_words)
      statement.setInt(12, t.section)
      statement.setString(13, t.plat_all_distribute)
      statement.setString(14, t.plat_neg_distribute)
      statement.setString(15, t.date_counts)
      statement.setString(16, t.date_risks)
      statement.setString(17, t.label)
      statement.setString(18, t.event_summary)
      statement.setInt(19, t.new_warn)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      //      conn.commit()
    } catch {
      case e: Exception =>
        //        println(del_sql)
        e.printStackTrace()
    } finally {
      //      statement_del.close()

      statement.close()
      statement_del.close()
      conn.close()
      //      deleteConn.close()
    }
  }


}



