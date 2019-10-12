package scala.yunce9.streaming

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.DataFrame
import MySpark._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import yunce9.utils.SparkUtil

/**
  * @ Author     ：许富友
  * @ Date       ：Created on 13:07 2019-04-26
  * @ Description：
  */
object KafkaStreaming extends Serializable {
  //  val url = "jdbc:mysql://10.253.50.250:3306/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
  val url = "jdbc:mysql://ycmysql.web.zz:3306/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    val interval = try args(0).toInt catch {
      case _: Exception => 10
    }
    //    val dates_ago = SparkUtil.days_ago_pre(30)
    sqlreader.option("dbtable", "t_search_task").load.createOrReplaceTempView("search_task")
    sqlreader.option("dbtable", "t_task_data").load.repartition(10).createOrReplaceTempView("t_task_data")
    //    getTaskData(1L,500L,10).createOrReplaceTempView("t_task_data")
    sqlreader.option("dbtable", "t_real_task").load.withColumnRenamed("id", "real_id").createOrReplaceTempView("t_real_task")
    //    sqlreader.option("dbtable", s"(select * from t_matchs_data_new where publish_date>='$dates_ago') as T").load().repartition(10)
    val ssc = new StreamingContext(sc, Seconds(interval))
    val topics = Array("yunce_formal") // information_security  cloud_test4  yunce_test
    val DS = getDStream(ssc, topics)
    DS.foreachRDD(dsRdd => {
      val start_time = System.currentTimeMillis()
      val count = dsRdd.count()
      if (count > 0) {
        Thread.sleep(1000)
        val offsetRanges = dsRdd.asInstanceOf[HasOffsetRanges].offsetRanges
        val rdd_count = dsRdd.count()
        println(s"${SparkUtil.NowDate()} 待处理数据:$rdd_count 条")
        IntervalAnalyze.analyze(dsRdd.map(_.value))
        DS.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        println(s"${SparkUtil.NowDate()} 用时:${(System.currentTimeMillis() - start_time) / 1000.0}S")
      }
      //      else println(s"${SparkUtil.NowDate()}")
    })
    //    ssc.checkpoint("hdfs://calcluster/yunce/streaming_checkpoint")
    ssc.remember(Duration(1800000))
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取kafka的流数据
    *
    * @param ssc    ssc
    * @param topics topics
    * @return
    *
    */
  private def getDStream(ssc: StreamingContext, topics: Array[String]): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.253.100.30:9092,10.253.100.31:9092,10.253.100.32:9092",
      //      "bootstrap.servers" -> "hadoop1:9092,hadoop2:9092,hadoop3:9092,hadoop4:9092,hadoop5:9092,hadoop6:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "yunce_formal",
      "auto.offset.reset" -> "latest", //latest, earliest, none
      "request.timeout.ms" -> "60000",
      "session.timeout.ms" -> "55000",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }

  /**
    * 分区读取t_data_nfo表数据
    *
    * @param parts askdfjaskldjfla
    * @param range adlfkjasdklfja
    * @return
    */
  def getInfoData(parts: Int = 6, range: Int = 5): DataFrame = {
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
    spark.read.jdbc(url, "t_matchs_data", predicates, prop)
  }

  /**
    * 分区读取t_data_nfo表数据
    *
    * @param lowerBound    lowerBound
    * @param upperBound    upperBound
    * @param numPartitions numPartitions
    * @return
    */
  def getTaskData(lowerBound: Long, upperBound: Long, numPartitions: Int): DataFrame = {
    //    val url = ConfigFactory.load("config.conf").getString("mysql_test.connect.url") //ycmysql.web.zz
    val tableName = "t_task_data"
    val columnName = "search_id"
    // 设置连接用户&密码
    val prop = new java.util.Properties
    prop.setProperty("user", "zzyq")
    prop.setProperty("password", "1qaz2WSX!@")
    // 取得该表数据
    val jdbcDF = spark.read.jdbc(url, tableName, columnName, lowerBound, upperBound, numPartitions, prop)
    jdbcDF
  }


}
