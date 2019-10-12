package scala.yunce9.structured

import scala.yunce9.{PresentSql, UpDateInfo}
import scala.yunce9.utils.MysqlConn

object SqlSave {
  def updateWarnSql(t: UpDateInfo): Unit = {
    // create database connection
    val conn = MysqlConn.connectMysql()
    try {
      val ps = conn.prepareStatement(s"UPDATE t_data_info SET type_label ='${t.type_label}',risk_value = ${t.risk_value},warn_level = '${t.warn_level}',warn_label = '${t.warn_label}',warn = ${t.warn},heat = ${t.heat} where id=${t.id} ")
      ps.executeUpdate()
      ps.close()
      println(s"预警写入完成：${t.id}")
    } catch {
      case e: Throwable => e.printStackTrace()
    } finally conn.close()
  }

  def saveToMacthcs(value: Seq[PresentSql]): Unit = {
    val conn = MysqlConn.connectMysql()
    //    val sql = "replace into t_negative_relation(data_id,type_label,risk_value,key_words,second_label) values (?,?,?,?,?)"
    //    val sql = s"replace into t_matchs_data(id,author,publish_date,appraise,heat,title,topic_words,fumian,zhengmian) values (?,?,?,?,?,?,?,?,?)"
    val sql = "replace into t_matchs_data(search_id,search_name,id,author,publish_date,risk_value,appraise,heat,title,topic_words,main_body_json,present_json) values (?,?,?,?,?,?,?,?,?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
    value.foreach(t => {
      val title = t.title.replaceAll("[\\ud800\\udc00-\\udbff\\udfff\\ud800-\\udfff]", "")
      statement.setLong(1, t.searchID)
      statement.setString(2, t.nickName)
      statement.setLong(3, t.id)
      statement.setString(4, t.author)
      statement.setString(5, t.publish_date)
      statement.setDouble(6, t.risk_value)
      statement.setString(7, t.appraise)
      statement.setDouble(8, t.heat)
      statement.setString(9, title)
      statement.setString(10, t.topic_words)
      statement.setString(11, t.mainBody_json)
      statement.setString(12, t.present_json)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
      conn.close()
    }
  }
}
