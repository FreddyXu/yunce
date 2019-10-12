package scala.yunce9.streaming

import java.sql.{Connection, SQLException}

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer
import scala.yunce9.SeRiskResult
import scala.yunce9.utils.SparkUtil
import scala.yunce9.utils.MysqlConn

/**
  * @ Author     ：许富友
  * @ Date       ：Created on 上午 10:45 2018/12/28 0028
  * @ Description：
  */
object SaveOrUpdataSql extends Serializable {


  /**
    * 分类的数据写入数据库
    *
    * @param iter iter
    */
  def saveToWarns(iter: Iterator[(Long, String, Double, String, String)], tablename: String): Unit = {
    val conn = MysqlConn.connectMysql()
    val sql = "replace into t_warns_data(data_id,imie,risk_value,warn_level,warn_label) values (?,?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
    iter.foreach(t => {
      statement.setLong(1, t._1)
      statement.setString(2, t._2)
      statement.setDouble(3, t._3)
      statement.setString(4, t._4)
      statement.setString(5, t._5)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
    }
  }

  /**
    * 写入结果表
    *
    * @param iter iter
    */
  def saveToRiskResult(iter: Iterable[SeRiskResult]): Int = {
    val conn: Connection = MysqlConn.connectMysql()
    val results = iter.toList
    var status = 0
    val real_ids = results.map(_.real_id)
    if (real_ids.isEmpty) println("查询数据为空") else queryAndDelete(real_ids)
    //删除完毕，开始插入数据
    val create_time = System.currentTimeMillis()
    val sql = "replace into t_personal_risk_result(risk_value,risk_desc,nick_name,real_id,present_words,p_create_time,test_time) values (?,?,?,?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
      iter.foreach(t => {
      statement.setInt(1, t.risk_value.formatted("%.0f").toInt)
      statement.setString(2, t.risk_desc)
      statement.setString(3, t.nick_name)
      statement.setLong(4, t.real_id)
      statement.setString(5, t.present_words)
      statement.setLong(6, create_time)
      statement.setLong(7,t.test_time)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      statement.close()
      status = 1
    }
    status
  }


  /**
    * 查询结果表并删除满足条件的历史数据
    *
    * @param results 需要查询的任务id
    */
  def queryAndDelete(results: Seq[Long]): Unit = {
    val conn1: Connection = MysqlConn.connectMysql()
    val querySql = conn1.createStatement()
    val real_ids = results.mkString(",")
    val query = querySql.executeQuery(s"select real_id,result_type,p_create_time from t_personal_risk_result where real_id in ($real_ids)")
    val realBuff = new ArrayBuffer[(Long, Long,Int)]()
    while (query.next()) {
      try{
        val rea_id = query.getLong("real_id")
        val p_create_time = query.getLong("p_create_time")
        val result_type = query.getInt("result_type")
        realBuff.append((rea_id, p_create_time,result_type))
      }
      catch {
        case e:Exception=>println("数据异常")
      }
    }
    val real_map = realBuff.groupBy(g=>(g._1,g._3)).mapValues(v => (v.size, v.minBy(_._2)._2)).filter(_._2._1 > 1).map(m => m._1._1 -> m._2._2)
    if (real_map.isEmpty) println("无需要被删除的历史测试结果数据")
    else {
      val deleteConn = conn1.createStatement()
      real_map.foreach(f => {
        val deleteSql = s"delete from t_personal_risk_result where real_id = ${f._1} and p_create_time = ${f._2}"
        try {
          deleteConn.execute(deleteSql)
        } catch {
            case e: Exception =>
            e.printStackTrace()
            println(deleteSql)
        }
      })
      querySql.close()
      deleteConn.close()
      conn1.close()
    }
  }
  /**
    * 写入负面数据分值表
    *
    * @param iter iter
    */
  def saveToNegative(iter: Iterator[(Long, String, Double, String, String)]): Unit = {

    val conn = MysqlConn.connectMysql()

    val sql = "replace into t_negative_relation(data_id,type_label,risk_value,key_words,second_label) values (?,?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
    iter.foreach(t => {
      statement.setLong(1, t._1)
      statement.setString(2, t._2)
      statement.setDouble(3, t._3)
      statement.setString(4, t._4)
      statement.setString(5, t._5)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
     
    }
  }


  /**
    * 更新任务状态，写入数据库
    *
    * @param iter iter
    */
  def updateStatus(iter: Iterator[(Long, Int, Int)]): Unit = {
    val conn = MysqlConn.connectMysql()
    iter.foreach(t => {
      try {
        val st = conn.createStatement() // t_data_info
        st.execute(s"UPDATE t_real_task SET status =${t._2}" +
          s" where id=${t._1} ")
      }
      catch {
        case e: SQLException =>
          println(s"${SparkUtil.NowDate()} 更新异常! $e ")
          e.getNextException

      }
    })
  }

  /**
    * 对vip用户更新任务状态
    *
    * @param iter iter
    */
  def updateStatusVip(iter: Iterator[(Long, Int, Int)]): Unit = {
    val conn = MysqlConn.connectMysql()
    iter.foreach(t => {
      try {
        val st = conn.createStatement() // t_data_info
        st.execute(s"UPDATE t_user_task SET is_update = ${t._3}" +
          s" where real_id = ${t._1} ")
      }
      catch {
        case e: SQLException =>
          println(s"${SparkUtil.NowDate()} 更新异常! $e ")
          e.getNextException
      }
    })
  }

  /**
    *
    * @param tuple tuple
    * @return
    */
  def updateVipStatus(tuple: (Long, Int)): Int = {
    var is_update = 0
    val conn = MysqlConn.connectMysql()
    try {
      val st = conn.createStatement() // t_data_info
      st.execute(s"UPDATE t_user_task SET is_update =${tuple._2}" +
        s" where real_id=${tuple._1} ")
    }
    catch {
      case e: SQLException =>
        println(s"${SparkUtil.NowDate()} 更新异常! $e ")
        e.getNextException
    }
    if (!conn.isClosed) {
      //操作完成,关闭数据库
      is_update = 1
    }
    is_update
  }


  /**
    * 分类的数据写入数据库（批量写入）
    *
    * @param iter iter
    */
  def save_negative_risk_result(iter: Iterator[(String, String, Long, String, Int, String, String, Double, Double)]): Unit = {
    val conn = MysqlConn.connectMysql()
    val sql = "replace into t_negative_risk_result(title,similar_num,ids,import_level,source_names,authors,heat,influence,search_id) values (?,?,?,?,?,?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
    iter.foreach(t => {
      val search_id = t._1.split("#")(0).toLong
      statement.setString(1, t._2)
      statement.setLong(2, t._3)
      statement.setString(3, t._4)
      statement.setInt(4, t._5)
      statement.setString(5, t._6)
      statement.setString(6, t._7)
      statement.setDouble(7, t._8)
      statement.setDouble(8, t._9)
      statement.setLong(9, search_id)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
     
    }
  }

  /**
    * 删除上一次测评的负面结果数 下一步新增当前测评结果
    *
    * @param search_ids search_ids
    */
  def delete_negative_risk_result_on_search_id(search_ids: String): Unit = {
    val conn = MysqlConn.connectMysql()
    val stmt = conn.createStatement()
    try {
      //      println(s"delete from t_negative_risk_result where search_id in (${search_ids})")
      stmt.execute(s"delete from t_negative_risk_result where search_id in ($search_ids)")
    } catch {
      case e: Exception => println("上次测评无相关负面数据!")
        e.printStackTrace()
    } finally {
      stmt.close()
     
    }
  }


  /**
    * 写入细节分析数据表
    *
    * @param iter iter
    */
  def saveToDetail(iter: Iterable[(Long, String, String, String)]): Unit = {
    val conn = MysqlConn.connectMysql()
    val sql = "replace into t_detail_result(real_id,word,appraise,ids) values (?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
    iter.foreach(t => {
      statement.setLong(1, t._1)
      statement.setString(2, t._2)
      statement.setString(3, t._3)
      statement.setString(4, t._4)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
     
    }
  }

  /**
    * 写入细节分析数据表
    *
    * @param iter iter
    */
  def updataToDetail(iter: Iterator[(Long, String, String, String)]): Unit = {
    val conn = MysqlConn.connectMysql()
    val sql = "replace into t_detail_result(real_id,word,appraise,ids) values (?,?,?,?)"
    val statement = conn.prepareStatement(sql)
    conn.setAutoCommit(false)
    iter.foreach(t => {
      statement.setLong(1, t._1)
      statement.setString(2, t._2)
      statement.setString(3, t._3)
      statement.setString(4, t._4)
      statement.addBatch()
    })
    try {
      statement.executeBatch()
      conn.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      statement.close()
     
    }
  }

  /**
    * 删除上一次测评的结果数 下一步新增当前测评结果
    *
    * @param search_ids search_ids
    */
  def delete_detail_result(search_ids: Long): Unit = {
    val conn = MysqlConn.connectMysql()
    val stmt = conn.createStatement()
    try {
      val sql = s"delete from t_detail_result where real_id =$search_ids "
      //      println(sql)
      stmt.execute(sql)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      stmt.close()
     
    }
  }

  /**
    * 正面分析结果入库
    *
    * @param df df
    *
    */
  def saveOrUpdate_positive_result(df: DataFrame, table_name: String): Unit = {
    df.foreachPartition(r => {
      val conn = MysqlConn.connectMysql()
      conn.setAutoCommit(false)
      val sql = s"replace into $table_name(search_id,word,ids,source_names,authors) values (?,?,?,?,?)"
      val pstmt = conn.prepareStatement(sql)
      while (r.hasNext) {
        val row = r.next()
        val search_id = row.getAs[Long]("search_id")
        val word = row.getAs[String]("word")
        val ids = row.getAs[String]("ids")
        val source_names = row.getAs[String]("source_names")
        val authors = row.getAs[String]("authors")
        pstmt.setLong(1, search_id)
        pstmt.setString(2, word)
        pstmt.setString(3, ids)
        pstmt.setString(4, source_names)
        pstmt.setString(5, authors)
        //split_hash id_hash_sim
        pstmt.addBatch()
      }
      try {
        pstmt.executeBatch()
        conn.commit()
      } catch {
        case e: SQLException =>
          println(s"${SparkUtil.NowDate()} 写入异常! $e ")
          e.getNextException
      } finally {
        pstmt.close()
       
      }
    })
  }


}
