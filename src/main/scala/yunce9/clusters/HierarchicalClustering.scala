package scala.yunce9.clusters

import breeze.linalg.Axis.{_0, _1}
import breeze.linalg.{DenseMatrix, DenseVector, max, norm}
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.mining.cluster.ClusterAnalyzer

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.yunce9.Present
import scala.yunce9.structured.DataMatchAnalize.{getRank, segment}
import scala.yunce9.Present

case class HcData(real_id: Long, nick_name: String, id: Long, publish_date: String, test_time: Long, risk_value: Double,label:String, heat: Double, title: String, topic_words: String, appraise: String, presents: Seq[Present])

object HierarchicalClustering extends Serializable {

  /**
    * 创建输入数据词袋
    */
  //  val word2Vec =

  def createVocabList(dataSet: Seq[Seq[String]]): Seq[String] = {
    dataSet.flatMap(a => a.toSet).toSet.toList
  }

  /**
    * 获取数据向量
    */
  def bagOfWords2Vec(vocabList: Seq[String], inputSet: Seq[String]): DenseVector[Double] = {
    val returnVec = DenseVector.zeros[Double](vocabList.size)
    inputSet.foreach(m => {
      val index = vocabList.indexOf(m)
      val value = returnVec(index) + 1
      returnVec(index) = value
    })
    returnVec
  }

  /**
    * 是计算余弦距离
    */
  def gen_sim(DV1: DenseVector[Double], DV2: DenseVector[Double]): Double = {
    val num: Double = DV1 dot DV2
    var denum: Double = norm(DV1) * norm(DV2)
    if (denum == 0.0) denum = 1.0
    val cosn = num / denum
    var sim = 0.5 + 0.500000000 * cosn // 余弦值为[-1,1],归一化为[0,1],值越大相似度越大
    sim = 1 - sim // 将其转化为值越小距离越近
    sim
  }

  /**
    * 计算两个簇的最小距离
    */

  def distMin(ds1: DenseMatrix[Double], ds2: DenseMatrix[Double]): Double = {
    var minD = 1.0
    val m = ds1.rows
    val n = ds2.rows
    ds1(1, ::).inner
    for (a <- 0.until(m)) {
      for (b <- 0.until(n)) {
        val dist = gen_sim(ds1(a, ::).inner, ds2(b, ::).inner)
        if (dist < minD) minD = dist
      }
    }
    minD
  }

  /**
    * 计算两个簇的最大距离
    */
  def distMax(ds1: DenseMatrix[Double], ds2: DenseMatrix[Double]): Double = {
    var minD = 1.0
    val m = ds1.rows
    val n = ds2.rows
    ds1(1, ::).inner
    for (a <- 0.until(m)) {
      for (b <- 0.until(n)) {
        val dist = gen_sim(ds1(a, ::).inner, ds2(b, ::).inner)
        if (dist > minD) minD = dist
      }
    }
    minD
  }

  /**
    * 计算两个簇的平均距离
    */
  def distAvg(ds1: DenseMatrix[Double], ds2: DenseMatrix[Double]): Double = {
    var avgD = 0.0
    var sumD = 0.0
    val m = ds1.rows
    val n = ds2.rows
    for (a <- 0.until(m)) {
      for (b <- 0.until(n)) {
        val dist = gen_sim(ds1(a, ::).inner, ds2(b, ::).inner)
        sumD += dist
      }
    }
    avgD = sumD / (m * n)
    avgD
  }

  def findMin(M: DenseMatrix[Double]): (Int, Int, Double) = {
    var minDist: Double = math.pow(10, 4)
    var minI = 0
    var minJ = 1
    val m = M.rows
    for (i <- 0.until(m)) {
      for (j <- 0.until(m)) {
        if (i != j && M(i, j) < minDist) {
          minDist = M(i, j)
          minI = i
          minJ = j
        }
      }
    }
    (minI, minJ, minDist)
  }


  /**
    * 函数的功能是将forclusterlist中的样本集按照labels中的标签值重新排序，得到按照类簇排列好的输出结果
    *
    * @param labels         聚类结果的标签值
    * @param forclusterlist 为聚类所使用的样本集
    * @return
    */
  def labels_to_original(labels: DenseMatrix[Int], forclusterlist: Seq[HcData]): Array[Array[HcData]] = {
    val max_label: Int = max(labels)
    val number_label = 0.to(max_label).toBuffer
    number_label.append(-1)
    val result = {
      number_label.indices.map((a: Int) => new ArrayBuffer[HcData]()).toArray
    }
    for (i <- 0.until(labels.rows)) {
      val index = number_label.indexOf(labels(i, 0))
      result(index).append(forclusterlist(i))
    }
    result.map(a => a.toArray)
  }


  //# 层次聚类算法
  def hCluster(dataSet: DenseMatrix[Double], dist: Double): DenseMatrix[Int] = {
    val m = dataSet.rows
    val clusterAssment: DenseMatrix[Int] = DenseMatrix.zeros(m, 1)
    var M: DenseMatrix[Double] = DenseMatrix.zeros(m, m) // 距离矩阵
    for (ii <- 0 until m)
      clusterAssment(ii, 0) = ii

    for (i <- 0.until(m)) {
      for (j <- (i + 1).until(m)) {
        val dataSeti: DenseMatrix[Double] = clusterAssment.findAll(f => f == i).map(m => DenseMatrix(dataSet(m._1, ::).inner))
          .reduce(DenseMatrix.vertcat(_, _))
        val dataSetj: DenseMatrix[Double] = clusterAssment.findAll(f => f == j).map(m => DenseMatrix(dataSet(m._1, ::).inner))
          .reduce(DenseMatrix.vertcat(_, _))
        M(i, j) = distAvg(dataSeti, dataSetj)
        M(j, i) = M(i, j)
      }
    }
    var q = m // 设置当前聚类个数
    var minDist = 0.0
    while (minDist < dist) {
      val min = findMin(M) // 找到距离最小的两个簇
      val i = min._1
      val j = min._2
      minDist = min._3
      //把第j个簇归并到第i个簇
      clusterAssment.findAll(f => f == j).foreach(f => clusterAssment(f._1, 0) = i)
      // 将j之后的簇重新编号
      for (l <- (j + 1).until(q)) {
        clusterAssment.findAll(f => f == l).foreach(f => clusterAssment(f._1, 0) = l - 1)
      }
      //      println(M)
      M = M.delete(j, _0)
      M = M.delete(j, _1)
      for (l <- 0 until (q - 1)) {
        val dataSeti: DenseMatrix[Double] = clusterAssment.findAll(f => f == i).map(m => DenseMatrix(dataSet(m._1, ::).inner))
          .reduce(DenseMatrix.vertcat(_, _))
        val dataSetl: DenseMatrix[Double] = clusterAssment.findAll(f => f == l).map(m => DenseMatrix(dataSet(m._1, ::).inner))
          .reduce(DenseMatrix.vertcat(_, _))
        //        println(dataSeti.rows,dataSeti.cols)
        M(i, l) = distAvg(dataSeti, dataSetl)
        M(l, i) = M(i, l)
      }
      //      performMeasure.append(Array(q - 1, minDist))
      q = q - 1
      //      println(s"当前簇的个数是：$q")
      //      println(s"距离最小的两个簇是第$i 个和第$j 个,距离是$minDist")
    }
    clusterAssment
  }

  def applyCluster(arrdata: Array[HcData], distance: Double): Array[Array[HcData]] = {
    val words = arrdata.map(_.topic_words.split(" ").toSeq)
    val vocab = createVocabList(words)
    val words2vec: Array[DenseVector[Double]] = words.map(row => bagOfWords2Vec(vocab, row))
    //    println(words2vec.size)
    val mt: DenseMatrix[Double] = DenseMatrix.zeros(words.length, vocab.size)
    //    println(mt)
    for (a <- words.indices) {
      for (b <- vocab.indices) {
        mt(a, b) = words2vec(a)(b)
      }
    }
    var result: Array[Array[HcData]] = null
    try {
      val clusters: DenseMatrix[Int] = hCluster(mt, distance)
      result = labels_to_original(clusters, arrdata)
    } catch {
      case exception: IllegalArgumentException => result = Array(arrdata)
        exception.printStackTrace()
    }
    result
  }

  def hanlpCluster(arrdata: Array[HcData], distance: Double, isTitle: Int = 1): Array[Array[HcData]] = {
    val analyzer = new ClusterAnalyzer[Long]
    val datamap: Map[Long, HcData] = arrdata.map(a => a.id -> a).toMap
    arrdata.foreach(data => {
      try {
        val document: List[String] = if(isTitle == 0) participle(data.title) else data.topic_words.split(" ").toList
        analyzer.addDocument(data.id, document)
      } catch {
        case e: Exception => println(s"数据异常: ${data.topic_words}")
          e.printStackTrace()
      }
    }) //数据加载到聚类对象中
    var clusters: mutable.Buffer[Set[Long]] = arrdata.map(a => Array(a.id).toSet).toBuffer
    try {
      clusters = analyzer.repeatedBisection(distance).map(_.toSet)
    } catch {
      case e: NullPointerException =>
        try {
          var cls = clusters.size
          if (cls > 100 && cls <= 1000) cls = cls / 30
          else if (cls > 10 && cls <= 100) cls = cls / 10
          else if (cls > 1 && cls <= 10) cls = cls
          else cls = cls / 50
          analyzer.repeatedBisection(cls)
        }
        catch {
          case e: Exception => println("该批数据无法聚类")
          //            e.printStackTrace()
        }
    }
    //    println(clusters.size)
    clusters.map(c => c.map(m => datamap(m)).toArray).toArray
  }

  /**
    * 封装分词方法
    *
    * @param sentence   文本内容
    * @param stop_words 是否启用停用词处理
    * @return
    */
  def participle(sentence: String, stop_words: Boolean = true): List[String] = {
    var str = sentence.replaceAll("尊敬的读者[.]+|网页链接}[!@#$%&*()]-|<p>|</p>|", "")
    if (sentence == null) str = ""
    var result: List[String] = null
    if (sentence.nonEmpty) {
      val segs = segment.seg(str)
      if (stop_words) CoreStopWordDictionary.apply(segs)
      result = segs.map(_.word).toList
    }
    else {
      result = List()
    }
    result
  }
}
