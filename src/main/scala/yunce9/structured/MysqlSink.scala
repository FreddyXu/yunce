package scala.yunce9.structured

import java.sql.{Connection, Statement}

import org.apache.spark.sql.ForeachWriter

import scala.yunce9.PresentSql
import scala.yunce9.utils.MysqlConn._

class MysqlSink extends ForeachWriter[PresentSql] {

  var conn: Connection = _
  var statement: Statement = _

  override def open(partitionId: Long, epochId: Long): Boolean = {
    conn = connectMysql()
    statement = conn.createStatement
    true
  }

  override def process(value: PresentSql): Unit = {
    val info = value
    val title = value.title.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]|[\'\"]", "")
    val sql = s"replace into t_matchs_data(search_id,search_name,id,author,platform,publish_date,risk_value,label,appraise,heat,title,topic_words,main_body_json,present_json,content_words) " +
      s"values (${info.searchID},'${info.nickName}',${info.id},'${info.author}','${info.platform}','${info.publish_date}',${info.risk_value},'${info.label}','${info.appraise}',${info.heat},'$title','${info.topic_words}','${info.mainBody_json}','${info.present_json}','${info.content_words}')"
    try {
      statement.executeUpdate(sql)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(sql)
    }
  }

  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}