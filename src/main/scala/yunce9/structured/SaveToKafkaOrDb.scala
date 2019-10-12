package scala.yunce9.structured

import java.util.Properties

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}

import DataMatchAnalize.{getEarlyWarn, participle, source_processing}
import MySpark._
import spark.implicits._
import scala.collection.mutable.ArrayBuffer
import scala.yunce9.utils.{KafkaSink, SparkUtil}
import scala.yunce9._

object SaveToKafkaOrDb {
  //  val url = "jdbc:mysql://ycmysql.web.zz:3308/yunce?characterEncoding=utf8&useSSL=true&useSSL=false&tinyInt1isBit=false"
  val url = "jdbc:mysql://10.253.50.250:3306/yunce?characterEncoding=utf8&use-SSL=true&useSSL=false&tinyInt1isBit=false"
  //  val kafka_toic = "info_test_data"

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")
    val ids = "10,13,18,19,22,23,28,28,33,35,40,48,49,54,59,62,64,70,75,79,82,87,90,93,94,97,102,117,122,123,134,143,144,147,149,157,159,164,169,169,170,175,185,187,188,193,194,199,202,205,210,210,210,215,217,222,227,228,233,234,235,238,243,252,254,258,260,262,265,270,274,278,282,283,287,292,297,299,304,305,309,312,314,317,320,325,327,329,334,339,342,345,353,357,359,363,368,370,372,375,378,379,382,383,387,389,394,398,402,405,410,415,417,420,423,428,433,438,440,443,448,450,454,458,459,464,468,473,474,475,478,479,484,489,494,495,500,504,507,508,510,512,513,518,522,524,525,527,532,538,543,547,548,553,557,562,567,568,573,578,579,588,593,598,604,607,610,614,617,618,623,628,633,637,640,644,647,649,652,653,658,662,667,672,674,675,677,680,683,688,690,694,695,698,703,708,713,718,719,720,723,727,729,734,738,742,743,745,750,755,758,759,764,769,774,775,783,784,785,789,790,793,797,800,804,805,809,810,813,817,818,820,823,828,829,829,829,829,829,830,833,834,838,843,844,846,847,848,852,853,854,859,861,862,864,869,874,879,884,889,891,894,897,898,902,907,909,912,917,921,924,927,929,933,934,936,937,938,941,944,944,944,944,944,946,948,953,957,962,964,968,971,973,974,979,984,989,991,992,997,997,997,997,997,1002,1003,1004,1008,1012,1013,1017,1021,1026,1031,1036,1041,1042,1046,1051,1056,1059,1063,1064,1067,1069,1069,1069,1069,1069,1073,1074,1078,1083,1088,1089,1093,1096,1099,1102,1102,1102,1107,1111,1112,1116,1119,1122,1123,1127,1131,1134,1139,1144,1149,1152,1157,1161,1164,1166,1168,1172,1177,1181,1183,1193,1194,1199,1199,1199,1203,1204,1204,1204,1208,1208,1208,1208,1208,1212,1213,1216,1219,1224,1229,1229,1229,1229,1229,1232,1233,1241,1246,1251,1253,1258,1263,1268,1271,1276,1277,1279,1284,1286,1291,1293,1297,1297,1297,1297,1297,1298,1302,1304,1308,1311,1314,1316,1321,1322,1327,1329,1332,1333,1338,1343,1344,1344,1347,1351,1353,1358,1358,1358,1358,1358,1363,1368,1371,1376,1376,1376,1376,1376,1378,1381,1381,1381,1381,1381,1386,1391,1393,1398,1403,1408,1411,1413,1417,1419,1424,1429,1431,1434,1436,1439,1443,1448,1451,1456,1458,1459,1461,1464,1468,1472,1474,1479,1482,1486,1487,1492,1493,1497,1499,1503,1508,1512,1512,1512,1512,1512,1513,1514,1517,1521,1526,1528,1533,1536,1537,1542,1546,1549,1551,1553,1554,1559,1562,1572,1577,1581,1584,1588,1591,1592,1597,1598,1598,1598,1598,1598,1603,1608,1611,1612,1616,1621,1626,1628,1631,1634,1636,1639,1641,1642,1646,1646,1647,1647,1647,1647,1647,1651,1656,1661,1662,1666,1669,1671,1673,1674,1677,1678,1681,1682,1686,1689,1692,1694,1696,1698,1703,1707,1708,1713,1716,1718,1722,1726,1728,1733,1742,1743,1744,1746,1751,1754,1757,1761,1766,1768,1771,1772,1773,1774,1778,1779,1784,1787,1789,1791,1794,1797,1802,1807,1812,1813,1818,1822,1827,1831,1833,1836,1844,1845,1846,1847,1848,1849,1850,1851,1852,1853,1854,1855,1857,1863,1887,1892,1917,1918,1923,1927,1929,1932,1938,1946,1952,1953,1954,1959,1974,1977,1986,1991,1998,2006,2007,2043,2049,2053,2058,2061,2088,2097,2099,2104,2113,2117,2119,2122,2124,2126,2129,2133,2136,2136,2141,2147,2151,2156,2161,2163,2176,2182,2186,2191,2196,2202,2206,2238,2249,2256,2258,2259,2263,2274,2276,2277,2303,2304,2305,2306,2307,2308,2309,2310,2311,2312,2313,2314,2315,2316,2317,2318,2319,2320,2321,2322,2323,2324,2325,2326,2327,2328,2329,2330,2331,2332,2333,2334,2335,2335,2336,2337,2338,2339,2340,2341,2342,2343,2344,2345,2346,2347,2348,2348,2350,2350,2351,2352,2355,2356,2357,2358,2359,2360,2361,2362,2363,2364,2365,2366,2367,2368,2369,2370,2371,2372,2373,2374,2375,2376,2377,2378,2379,2382,2384,2385,2386,2387,2388,2389,2390,2391,2392,2393,2394,2396,2397,2398,2399,2400,2401,2402,2403,2405,2406,2407,2408,2409,2410,2411,2412,2413,2414,2415,2416,2417,2419,2420,2421,2422,2423,2424,2425,2425,2426,2427,2428,2432,2433,2434,2435,2436,2437,2438,2439,2440,2441,2442,2443,2444,2445,2446,2447,2448,2449,2450,2451,2452,2453,2454,2455,2456,2457,2458,2459,2460,2461,2462,2463,2464,2465,2466,2467,2468,2469,2470,2472,2473,2475,2476,2477,2478,2479,2480,2481,2482,2483,2484,2485,2486,2487,2488,2488,2489,2490,2491,2492,2493,2494,2495,2496,2497,2498,2499,2500,2501,2502,2503,2504,2505,2506,2507,2508,2509,2510,2511,2512"

    val matchdata = getMatchData().persist()
    saveData(matchdata)
    spark.close()
  }

  /**
    * 获取业务所需数据
    *
    * @param real_id real_id
    * @return
    */
  def getMatchData(real_ids: String = ""): Dataset[KafkaJson] = {
    val info = getInfoData().where("appraise in ('正面','负面')").select($"id", $"title", $"content", $"author", $"publish_date".as("publishDate"), $"type".as("types"), $"url", $"source_name".as("sourceName"), $"appraise", $"click", $"zan_total".as("zanTotal"), $"repeat_total".as("repeatTotal"), $"forward", $"create_time".as("createTime"))
    val task = getTaskData(1L, 10000000L, 20)
      //      .where(s"search_id =$real_id")
      .withColumnRenamed("search_id", "real_id")
    val resultDF = sqlreader.option("dbtable", "t_personal_risk_result").load().select($"real_id", $"nick_name", $"risk_value".as("score"))
    //case class KafkaJson(imie:String,id: Long, title: String, content: String, author: String, publishDate: String, types: String, url: String, sourceName: String, appraise: String, click: Int, zanTotal: Int, repeatTotal: Int, forward: Int, createTime:String)
    val infoDF = info.join(task, "id")
    val joinDF = if (real_ids == "") infoDF else infoDF.where(s"real_id in ($real_ids)")
    val searchDF = sqlreader.option("dbtable", "t_search_task").load().select($"id".as("search_id"))
    val getImie = udf((real_id: Long, create_time: Long, is_vip: Int) => s"$real_id-$is_vip-$create_time")
    val realdf = sqlreader.option("dbtable", "t_real_task").load().join(searchDF, "search_id")
      .withColumn("imie", getImie($"id", $"create_time", $"is_vip"))
      .select($"id".as("real_id"), $"imie")
    val allDF = joinDF.join(resultDF, "real_id").join(realdf, "real_id").drop("real_id")
    allDF.as[KafkaJson]


  }


  def saveData(ds: Dataset[KafkaJson]): Unit = {
    val getweight = udf((a: Double) => a * 0 + 1.0)
    val negative_words = sqlreader.option("dbtable", "t_negative_words").load().withColumn("word", explode(split($"cal_words", ";"))).withColumn("weight", getweight($"cal_num")).drop("cal_words", "id").cache()
    val positive_words = sqlreader.option("dbtable", "t_positive_words").load().withColumn("word", explode(split($"key_words", ";"))).drop("key_words", "key", "limit_weight", "id").cache()
    val present_words = sqlreader.option("dbtable", "t_present_words").load().withColumn("word", explode(split($"words", ";"))).drop("words").cache()
    val neg_pos_words = negative_words.join(positive_words, Seq("word", "first_level", "second_level"), "full")
    val all = present_words.join(neg_pos_words, "word").drop("id").as[Present].collect()
    //    val map_present = all.map(m => m.word -> m).toMap
    val neg_words = all.filter(_.appraise == "负面")
    val pos_words = all.filter(_.appraise == "正面")

    val source_influence = sqlreader.option("dbtable", "t_source_influence").load().map(a => (a.getAs[String]
      ("source_name"), a.getAs[Double]("source_weight"))).collect().toMap
    val dataInfo = ds.map(df => {
      if (df.createTime == null || df.createTime == "") df.createTime = SparkUtil.NowDate()
      val searchId = df.searchId.toLong
      MainBody(searchId, df.nickName, df.id, df.title, df.contentWords, df.author, df.sourceName,df.platform, df.publishDate, df.appraise, df.heat, df.createTime, df.mainBody.split(" "))
    }).as[MainBody].where("appraise in ('负面','正面')")
    //匹配与测试主体相关的业务正负面关键词
    val matchDF: Dataset[MatchPresent] = dataInfo.map(df => {
      val heat = df.heat
      val words = df.mainBody.toSet
      val topic_words: String = participle(df.contentWords, rank = true)._2.mkString(" ")
      val matchData: MatchPresent = MatchPresent(df.searchId, df.nickName, df.id, df.author,df.sourceName, df.platform,df.publishDate, 100.0, "", df.appraise, heat, df.title, topic_words, df.mainBody, null)
      if (df.appraise == "负面") {
        //计算负面数据的来源权重*时间衰减权重
        val weight = source_processing(df, source_influence)
        val fu_presents: ArrayBuffer[Present] = new ArrayBuffer[Present]()
        neg_words.foreach(n => {
          val word_set = n.word.split("+").toSet
          n.weight = weight.toString
          if (word_set.intersect(words) == word_set) fu_presents.append(n)
        })
        //负面数据匹配不为空
        if (fu_presents.nonEmpty) {
          val result = getEarlyWarn(df, fu_presents) //预警数据写入!
          matchData.label = result.type_label
          matchData.presents = fu_presents
          matchData.risk_value = result.risk_value
        }
      }
      else {
        val zm_match: ArrayBuffer[Present] = new ArrayBuffer[Present]()
        pos_words.foreach(p => {
          val word_set = p.word.split("+").toSet
          if (word_set.intersect(words) == word_set) zm_match.append(p)
        })
        //正面数据匹配不为空
        if (zm_match.nonEmpty) matchData.presents = zm_match
      }
      if (!(matchData.presents == null)) println(s"${SparkUtil.NowDate()} :$matchData")
      matchData
    }).filter(r => !(r.presents == null))
    val saveSqlDF = matchDF.withColumn("present_json", to_json($"presents")).drop("presents").as[PresentSql]
    saveSqlDF.foreachPartition(f => {
      SqlSave.saveToMacthcs(f.toSeq)
    })

  }

  /**
    * 分区读取t_data_nfo表数据
    *
    * @param parts askdfjaskldjfla
    * @param range adlfkjasdklfja
    * @return
    */
  def getInfoData(parts: Int = 10, range: Int = 50): DataFrame = {
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
    spark.read.jdbc(url, "t_matchs_data", predicates, prop)
  }

  /**
    * 分区读取t_task_data表数据
    *
    * @param lowerBound    lowerBound
    * @param upperBound    upperBound
    * @param numPartitions numPartitions
    * @return
    */
  def getTaskData(lowerBound: Long, upperBound: Long, numPartitions: Int): DataFrame = {
    //ycmysql.web.zz
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
