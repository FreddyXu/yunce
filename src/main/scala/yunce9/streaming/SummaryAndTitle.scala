package scala.yunce9.streaming

import java.sql.SQLException
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.mining.word2vec.{DocVectorModel, WordVectorModel}
import com.hankcs.hanlp.seg.Segment
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import MySpark._
import spark.implicits._
import scala.yunce9.utils.{MysqlConn, SparkUtil}

object SummaryAndTitle {
  val segment: Segment = HanLP.newSegment().enableCustomDictionaryForcing(true).enableAllNamedEntityRecognize(true)
  val docVectorModel = new DocVectorModel(new WordVectorModel("D:/HanLP/hanlp-wiki-vec-zh.txt"))

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("ERROR")
    //    val clear_content = udf((content: String) => content.replaceAll("[\\#\n\\/]|网页链接", ""))
    //    val data = spark.read.csv("file:///D:/gitProject/yunce9/src/main/scala/yunce9/datas/sabeining.csv")
    //      .toDF("title", "content0").filter("content0 is not null")
    //      .withColumn("content", clear_content(col("content0")))
    //      .drop("content0")
    val get_summary = udf((content: String) => getSummary(content))
    //    data.withColumn("summary", get_summary(col("content")))
    //      .repartition(1).write.mode("overwrite")
    //      .csv("file:///D:/gitProject/yunce9/src/main/scala/yunce9/datas/summary")
    val data_event = sqlreader.option("dbtable", "t_event_cluster").load()
      .where("event_summary = '' or event_summary IS null")
      .rdd.map(r => r.getAs[Long]("id") -> r.getAs[String]("ids").split(",").max.toLong).toDF("event_id", "id").persist()
    val isd= data_event.select("id").rdd.map(m=>m.getAs[Long]("id")).collect.mkString(",")
    val data_info = sqlreader.option("dbtable", "t_data_info").load().where(s"id in ($isd)").select("id", "content").persist()
    val summary_data = data_info.join(data_event, "id")
      .withColumn("event_summary", get_summary(col("content")))
      .rdd.map(m => (m.getAs[Long]("event_id"), m.getAs[Long]("id"), m.getAs[String]("event_summary")))
    summary_data.foreachPartition(f => updateEvent(f))
    println("完成！")
  }

  def getSummary(content: String): String = {
    if (content == null) "" else {
      //  seq_words = segment.seg2sentence(content.replaceAll("\n|原标题：", " "))
      var summary = HanLP.extractSummary(content, 2, "[。？?！!；;▪]|</p>|<p>").mkString(";")
      if (summary.length > 100) summary = summary.substring(0, 100)
      summary.replaceAll("\'|\"","’")
    }
  }

  /**
    * 更新任务状态，写入数据库
    *
    * @param iter iter
    */
  def updateEvent(iter: Iterator[(Long, Long, String)]): Unit = {
    val conn = MysqlConn.connectMysql()
    val st = conn.createStatement() // t_data_info
    iter.foreach(t => {
      val sql = s"UPDATE t_event_cluster SET event_summary='${t._3}'" +
        s" where id = ${t._1}"
      try {
        st.execute(sql)
      }
      catch {
        case e: SQLException =>
          println(s"${SparkUtil.NowDate()} 更新异常! ")
          println(sql)
          e.printStackTrace()
      }
    })
  }

}
