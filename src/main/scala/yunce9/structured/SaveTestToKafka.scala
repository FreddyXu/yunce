package scala.yunce9.structured

import java.util.Properties

import com.google.gson.Gson
import org.apache.kafka.common.serialization.StringSerializer

import scala.yunce9.utils.{KafkaSink, SparkUtil}
import scala.io._


object SaveTestToKafka {
  val url = "jdbc:mysql://10.253.50.250:3306/yunce?characterEncoding=utf8&use-SSL=true&useSSL=false&tinyInt1isBit=false"
  val kafka_toic = "yunce_formal"

  def main(args: Array[String]): Unit = {
    //    sc.setLogLevel("WARN")
    val source = Source.fromFile("D:/gitProject/yunce9/src/datas/real_ids", "UTF-8")
    val lines: Array[String] = source.getLines.toArray
    lines.foreach(f => {
      getkafka.send(kafka_toic, f)
      println("======== 发送成功！======\n" + s"$f")
      Thread.sleep(30000)
    })


    //    getMatchData(ids1).rdd.collect.foreach(a => {
    //      val gs = new Gson()
    //      getkafka.send(kafka_toic, gs.toJson(a))
    //      println("======== 发送成功！======\n" + s"${a}")
    //      Thread.sleep(500)
    //    })
    //    val matchdata = getMatchData().persist()
    //    saveData(matchdata)
    //    spark.close()
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

}
