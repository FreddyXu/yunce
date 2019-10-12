package scala.yunce9.structured

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
  * @ Author     ：许富友
  * @ Date       ：Created on 下午 1:00 2018/11/29 0029
  * @ Description：
  */
object MySpark {
//  val url = "jdbc:mysql://10.253.50.250:3306/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
  val url = "jdbc:mysql://ycmysql.web.zz:3306/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo serializer.buffer.max", "128")
    .config("spark.streaming.concurrentJobs", "3")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.debug.maxToStringFields", "100")
    .config("spark.default.parallelism", "10")
    .config("spark.speculation", "false")
    .config("spark.locality.wait.node", "3s")
    .config("spark.driver.maxResultSize", "4G")
    .appName("云测数据关键词匹配（测试环境）")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext


  val sqlreader: DataFrameReader = spark.read.format("jdbc")
  //数据库路径 mysql_test.connect.url  ycmysql.web.zz
  sqlreader.option("url", url)
    .option("driver", "com.mysql.cj.jdbc.Driver") //com.mysql.jdbc.Driver
    .option("user", "zzyq")
    .option("password", "1qaz2WSX!@")


}
