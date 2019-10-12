package scala.yunce9.clusters

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.mining.word2vec.{DocVectorModel, WordVectorModel}
import com.hankcs.hanlp.seg.Segment
import scala.collection.JavaConversions._
import MySpark._

object NewSummary extends App {
  val segment: Segment = HanLP.newSegment().enableCustomDictionaryForcing(true)
    .enableAllNamedEntityRecognize(true)
  val docVectorModel = new DocVectorModel(new WordVectorModel("D:/HanLP/hanlp-wiki-vec-zh.txt"))

  //  val path="file:///C:\\Users\\91BGJK2\\Desktop\\人民的名义.txt"
  val text = "朝鲜外相今抵新加坡，穿黑西装打紫色领带，将与多国外长会谈。朝鲜外相李勇浩今抵新加坡。朝中社英文版2日的报道称，李勇浩率领朝鲜代表团于当天启程，除了新加坡，他还将访问伊朗。"
  //分句拆词
  val sent: Map[Int, Seq[String]] = splitSentence(text)
  val metric = createMat(sent)
  for (i <- metric) println(i.mkString(" "))
  val ranks = Array.ofDim[Double](metric.length).map(str => 1.0)
  //循环迭代结算得分
  for (i <- 0 until 5) {
    val res: Map[Int, Double] = sent.keys.map { line =>
      val score = pageRank(metric, ranks, line)
      line -> score
    }.toMap
    res.foreach(str => println("句子" + str._1 + "==" + str._2))
    //更新第一次循环后的得分
    res.foreach(str => ranks.update(str._1, str._2))
  }


  //使用pageRand算法计算句子排名----计算单个句子的得分
  def pageRank(board: Array[Array[Double]], ranks: Array[Double], num: Int): Double = {
    val len = board.length
    val d = 0.85
    var added_score = 0.0
    for (j <- 0 until len) {
      var fraction = 0.0
      var denominator: Double = 0.0
      // 先计算分子
      fraction = board(j)(num) * ranks(j)
      // 计算分母
      for (k <- 0 until len) {
        denominator = denominator + board(j)(k)
      }
      added_score += fraction / denominator
    }
    val weighted_score = (1 - d) + d * added_score
    weighted_score
  }

  //构建句子间的相邻矩阵
  def createMat(document: Map[Int, Seq[String]]): Array[Array[Double]] = {
    val num = document.size
    // 初始化表
    val board = Array.ofDim[Double](num, num)
    for (i <- 0 until num) {
      for (j <- 0 until num) {
        if (i != j) {
          val a: Seq[String] =document(i)
          board(i)(j) = getCost(document(i), document(i))._2
        }
      }
    }
    return board
  }

  //句子间的相似度计算：共同词个数/log（len1）+log(len2）
  def similar_cal(sent1: Seq[String], sent2: Seq[String]): ((String, String), Double) = {
    val same_word = sent1.intersect(sent2)
    val sim = same_word.size / (math.log(sent1.size) + math.log(sent2.size))
    (sent1.toString(), sent2.toString()) -> sim
  }

  //中文分词
  def cutWord(sentense: String): Seq[String] = {
    val sent_split: Seq[String] = segment.seg(sentense).map(_.word)
    sent_split
  }

  //拆分句子
  def splitSentence(document: String): Map[Int, Seq[String]] = {
    val pattern = "(。|！|？|\\.|\\!|\\?)".r
    val res = pattern.split(document).toList.zipWithIndex.map(str => str._2 -> cutWord(str._1)).toMap
    res
  }

  def getCost(str1: Seq[String], str2: Seq[String]): ((String, String), Double)= {
    val vec1 = docVectorModel.addDocument(1, str1.mkString(" "))
    val vec2 = docVectorModel.addDocument(2, str2.mkString(" "))
    val sim = vec1.cosineForUnitVector(vec2).toDouble
    (str1.mkString(""),str2.mkString(" "))->sim
  }

}