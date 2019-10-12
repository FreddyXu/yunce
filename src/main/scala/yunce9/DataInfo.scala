package scala.yunce9

import scala.collection.mutable.ArrayBuffer
import com.hankcs.hanlp.mining.word2vec

case class KafkaJson(searchId: String, nickName: String, id: Long, author: String, publishDate: String, title: String, contentWords: String, sourceName: String, platform: String, appraise: String, heat: Double, mainBody: String, var createTime: String)

case class AnalizeData(imie: String, real_id: Long, is_vip: Int, task_time: Long, id: Long, title: String, content: String, author: String, publishDate: String, types: String, url: String, sourceName: String, appraise: String, click: Int, zanTotal: Int, repeatTotal: Int, forward: Int, var createTime: String)

case class UpDateInfo(id: Long, type_label: String, risk_value: Double, warn_level: String, warn_label: String, warn: Int, heat: Double)

case class TestJson(id: Long, real_id: Long, data_id: Long)

case class MatchsSql(id: Long, author: String, publish_date: String, appraise: String, heat: Double, title: String, topic_words: String, var fumian: String, var zhengmian: String)

case class HcData(real_id: Long, nick_name: String, id: Long, publish_date: String, test_time: Long, platform: String, risk_value: Double, label: String, heat: Double, title: String, topic_words: String, appraise: String, presents: Seq[Present])

case class Present(word: String, present: String, use: String, appraise: String, first_level: String, second_level: String, cal_level: String, cal_score: String, cal_limit: String, var cal_num: String, cal_num_limit: String, var result_influence_weight: String, var weight: String, var single_weight: String)

case class MatchPresent(searchID: Long, nickName: String, id: Long, author: String, content_words: String, platform: String, publish_date: String, var risk_value: Double = 100, var label: String, appraise: String, heat: Double, title: String, topic_words: String, mainBody: Seq[String], var presents: Seq[Present])

case class PresentSql(searchID: Long, nickName: String, id: Long, author: String, content_words: String, platform: String, publish_date: String, var risk_value: Double = 100, var label: String, appraise: String, heat: Double, title: String, topic_words: String, mainBody_json: String, present_json: String)

case class PresentShows(var key: String, id: Long, publish_data: Long)

case class RiskResult(risk_value: Double, risk_desc: String, test_time: Long, nick_name: String, real_id: Long, search_id: Long, present_words: String)

case class ReceiveKafka(realId: String, searchId: String)

case class Match(real_id: Long, nick_name: String, test_time: Long, is_vip: Int, search_id: Long, id: Long, publish_date: String, weight: Double, heat: Double, title: String, topic_words: String, appraise: String, author: String, key: String, word: String, use: String, present: Present) extends Serializable

case class TaskInfo(real_id: Long, is_vip: Int, test_time: Long, nick_name: String, search_id: Long, id: Long, publish_date: String, author: String, platform: String, appraise: String, risk_value: Double, label: String, heat: Double, var title: String, topic_words: String, content_words: String, presents: Seq[Present])

//real_id,test_time,event_title,risk_value,ids,event_heat,event_appraise,is_new,new_ids,event_words
case class EventData(id: Long, real_id: Long, test_time: Long, event_title: String, stage: String, risk_value: Double, label: String, ids: String, event_heat: Double, event_appraise: String, is_new: Int, new_ids: String, event_words: String)

case class IncrementData(id: Long, input: TaskInfo, var vector: word2vec.Vector, var processed: Int = 0)

case class EventInfo(datas: ArrayBuffer[IncrementData], var vector: word2vec.Vector, var e_title: String, var e_summary: String, var is_history: Int = 0)

case class HistoryEvent(real_id: Long, risk_value: Double, event_title: String, event_summary: String, ids: ArrayBuffer[Long], event_vector: word2vec.Vector)

case class SeEventDataNew(id: Long, real_id: Long, test_time: Long, event_title: String, event_summary: String, risk_value: Double, label: String, ids: String, event_heat: Double, event_appraise: String, var is_new: Int = 0, var new_ids: String = "", event_words: String, section: Int, plat_all_distribute: String, plat_neg_distribute: String, date_counts: String, date_risks: String, var new_warn: Int = 0)

case class SearchData(real_id: Long, search_id: Long, test_time: Long, nick_name: String, is_vip: Int)

case class SeEventData(id: Long, real_id: Long, test_time: Long, event_title: String, stage: String, risk_value: Double, label: String, ids: String, event_heat: Double, event_appraise: String, is_new: Int, new_ids: String, event_words: String, section: Int)

case class SeRiskResult(risk_value: Double, risk_desc: String, test_time: Long, nick_name: String, real_id: Long, search_id: Long, present_words: String, se: Int, result_ratio: String, new_event_desc: String)

case class
NegAnalyze(real_id: Long, nick_name: String, search_id: Long, test_time: Long, base_value: Double, influenceWeight: Double, words_size: Seq[PresentShows], author_size: Int, label: Seq[String])

case class PosAnalyze(real_id: Long, nick_name: String, search_id: Long, test_time: Long, pos_weight: Double, show: Seq[PresentShows], authors: String, pos_au_cout: Int, post_keys: String)


case class MainBodyKafka(searchId: Long, nickName: String, id: Long, title: String, contentWords: String, author: String, platform: String, sourceName: String, publishDate: String, appraise: String, heat: Double, var createTime: String, mainBody: String)

case class MainBody(searchId: Long, nickName: String, id: Long, title: String, contentWords: String, author: String, platform: String, sourceName: String, publishDate: String, appraise: String, heat: Double, var createTime: String, mainBody: Array[String])
