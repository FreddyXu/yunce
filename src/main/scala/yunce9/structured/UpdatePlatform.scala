package scala.yunce9.structured

import java.sql.SQLException

import org.apache.spark.sql.DataFrame

import MySpark._
import yunce9.utils.{MysqlConn, SparkUtil}
import org.apache.spark.sql.functions._

import spark.implicits._
import scala.yunce9.streaming.KafkaStreaming.url
import scala.yunce9.streaming.MySpark.spark
import DataMatchAnalize.participle

object UpdatePlatform {
  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")
    //    val matchDF = sqlreader.option("dbtable", "t_matchs_data").load().where("content_words is NULL or content_words = ''").select("id")
    //    val participle = udf((str: String) => DataMatchAnalize.participle(str, stop_words = false)._1.mkString(" ").replaceAll("[\'\"]", "“"))
    //    val dataInfo = sqlreader.option("dbtable", "t_data_info").load().select($"id", participle($"content"))
    //    matchDF.join(dataInfo, "id").map(m => m.getLong(0) -> m.getString(1)).foreachPartition(f => save(f))
    val matchDF = sqlreader.option("dbtable", "t_matchs_data").load().where("publish_date>='2018-09-28 00:00:00' AND length(content_words)<10").select("id")
    println(matchDF.count())
    val ids = matchDF.rdd.map(r=>r.getAs[Long]("id")).collect.mkString(",")
    val dataInfo = sqlreader.option("dbtable", "t_data_info").load().where(s"id in (${ids})")
      .repartition(10)
      .select($"id", $"type", $"content").persist()
    val combyData = dataInfo //matchDF.join(dataInfo, "id").persist()
    dataInfo.show()
    combyData.map(m => {
      val id = m.getAs[Long]("id")
      val platform = m.getAs[String]("type")
      val content = m.getAs[String]("content")
      (id, platform, participle(content)._1.mkString(" ").replaceAll("[\'\"]", "“"))
    }).rdd.foreachPartition(f => save(f))
    println("更新完成")
  }

  def save(iterable: Iterator[(Long, String, String)]): Unit = {
    val conection = MysqlConn.connectMysql()
    iterable.foreach(t => {
      try {
        val st = conection.createStatement() // t_data_info
        st.execute(s"UPDATE t_matchs_data SET platform ='${t._2}',content_words = '${t._3}' where id=${t._1} ")
      }
      catch {
        case e: SQLException =>
          println(s"${SparkUtil.NowDate()} 更新异常! $e ")
          e.getNextException
      }
    })
  }


  def save1(iterable: Iterator[(Long, String)]): Unit = {
    val conection = MysqlConn.connectMysql()
    iterable.foreach(t => {
      try {
        val st = conection.createStatement() // t_data_info
        st.execute(s"UPDATE t_matchs_data SET platform ='${t._2}' where id=${t._1} ")
      }
      catch {
        case e: SQLException =>
          println(s"${SparkUtil.NowDate()} 更新异常! $e ")
          e.getNextException
      }
    })
  }


  /**
    * 分区读取t_data_nfo表数据
    *
    * @param parts
    * @param range
    * @return
    */
  def getInfoData(table: String, parts: Int = 20, range: Int = 37): DataFrame = {
    val predicates = 1.to(parts).toArray.map(a => {
      if (a == 1) SparkUtil.NowDate() -> SparkUtil.days_ago_pre(a * range)
      else SparkUtil.days_ago_pre((a - 1) * range) -> SparkUtil.days_ago_pre(a * range)
    }).map {
      case (start, end) => s"publish_date < '$start' AND publish_date >= '$end'"
    }
    val prop = new java.util.Properties
    prop.setProperty("user", "zzyq")
    prop.setProperty("password", "1qaz2WSX!@")
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    //    val url = ConfigFactory.load("config.conf").getString("ycmysql.web.zz")
    spark.read.jdbc(url, s"$table", predicates, prop)
  }


}

