package scala.yunce9.clusters

import breeze.linalg.{DenseMatrix, DenseVector, max}
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.dictionary.stopword.CoreStopWordDictionary
import com.hankcs.hanlp.seg.Segment
import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.yunce9.clusters.HierarchicalClustering.{bagOfWords2Vec, createVocabList, hCluster, gen_sim}
import scala.yunce9.structured.DataMatchAnalize.{getRank}
import MySpark._
import spark.implicits._

case class LabelData(person: String, id: Long, title: String, content: String, var topic_wrods: String, appraise: String, label: String, var cluster_id: Int = 0)

case class LabelData1(labelData: LabelData,vector: DenseVector[Double])
case class LabelsCluster(input: LabelData, var cluster_id: Int = 0, var is_type: Int = 0)


object ClusteringValidation {
  val segment: Segment = HanLP.newSegment().enableCustomDictionaryForcing(true).enableAllNamedEntityRecognize(true)

  def main(args: Array[String]): Unit = {
    sc.setLogLevel("WARN")
    val fileName = "D:\\gitProject\\yunce9\\src\\data\\label.txt"
    var labelDatas = Source.fromFile(fileName).getLines().map(r => {
      val row = r.split(",")
      val topicwords = participle(row(3), rank = true)._2.mkString(" ")
      LabelData(row(0), row(1).toLong, row(2), row(3), topicwords, row(4), row(5))
    }).toArray
    val start_time = System.currentTimeMillis()
    //    val clusers = hierarchical(labelDatas, 0.4118)
    val clusers = dbScanVec(labelDatas, 0.28, 1)
//        labelDatas.map(ld => {
//          ld.topic_wrods = ld.title.replaceAll("[，。？！.]", "").split("").mkString(" ")
//          ld
//        })
//    val clusers = dbScanWords(labelDatas, 0.56, 1)
    var clusterid = 0
    val df = clusers.flatMap(a => {
      clusterid += 1
      a
    })
    val resultDS = spark.createDataset(df).sort("cluster_id").persist()
    resultDS.repartition(1).write.mode("overwrite")
      .csv("D:\\gitProject\\yunce9\\src\\data\\hiera")
    resultDS.show()
    println(s"用时：${System.currentTimeMillis() - start_time}")
  }

  /**
    *
    * @param arrdata
    * @param distance
    * @return
    */
  def hierarchical(arrdata: Array[LabelData], distance: Double): Array[Array[LabelData]] = {
    val words = arrdata.map(_.topic_wrods.split(" ").toSeq)
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
    var result: Array[Array[LabelData]] = null
    try {
      val clusters: DenseMatrix[Int] = hCluster(mt, distance)
      result = labels_to_original(clusters, arrdata)
    } catch {
      case exception: IllegalArgumentException => result = Array(arrdata)
        exception.printStackTrace()
    }
    result
  }

  /**
    * 函数的功能是将forclusterlist中的样本集按照labels中的标签值重新排序，得到按照类簇排列好的输出结果
    *
    * @param labels         聚类结果的标签值
    * @param forclusterlist 为聚类所使用的样本集
    * @return
    */
  def labels_to_original(labels: DenseMatrix[Int], forclusterlist: Seq[LabelData]): Array[Array[LabelData]] = {
    val max_label: Int = max(labels)
    val number_label = 0.to(max_label).toBuffer

    number_label.append(-1)
    val result = {
      number_label.indices.map((a: Int) => new ArrayBuffer[LabelData]()).toArray
    }
    for (i <- 0.until(labels.rows)) {
      val index = number_label.indexOf(labels(i, 0))
      forclusterlist(i).cluster_id = index
      result(index).append(forclusterlist(i))
    }
    result.map(a => a.toArray)
  }


  def dbScanVec(arrdata: Array[LabelData], ePs: Double, minPts: Int): Array[Array[LabelData]] = {
    val words = arrdata.map(m=>(m,m.topic_wrods.split(" ").toSeq))
    val vocab = createVocabList(words.map(_._2))
//    val data = arrdata
    val data: Array[LabelData1] = words.map(row => LabelData1(row._1,bagOfWords2Vec(vocab, row._2)))
    //    println(data.head.length)
    //    val types = (for (i <- data.indices) yield -1).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
    //    val types: Array[ClusterTypes] = data.map(a => ClusterTypes(a))
    val visited = (for (i <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
    var number = 1 //用于标记类
    //    val dv: DenseVector[Double] = DenseVector.zeros(data.head.length)
    var xTempPoint: LabelData1 = null
    var yTempPoint: LabelData1 = null
    var distance = new Array[(Double, Int)](1)
    var distanceTemp = new Array[(Double, Int)](1)
    val neighPoints = new ArrayBuffer[LabelData1]()
    var neighPointsTemp = new Array[LabelData1](1)
    val cluster = data.map(m => LabelsCluster(m.labelData)) //new Array[Clusters](data.length) //用于标记每个数据点所属的类别
    var index = 0
    for (i <- data.indices) {
      //对每一个点进行处理
      if (visited(i) == 0) { //表示该点未被处理
        visited(i) == 1 //标记为处理过
        xTempPoint = data(i) //取到该点
        distance = data.map(x => (gen_sim(x.vector, xTempPoint.vector), data.indexOf(x))) //取得该点到其他所有点的距离 Array{(distance,index)}
        neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点(密度相连点集合)
        if (neighPoints.nonEmpty && neighPoints.length < minPts) {
          breakable {
            for (i <- neighPoints.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
              val index: Int = data.indexOf(neighPoints(i))
              if (cluster(index).is_type == 1) {
                cluster(i).is_type = 0 //边界点
                break
              }
            }
          }
        }
        if (neighPoints.length >= minPts) { //核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
          cluster(i).is_type = 1
          cluster(i).cluster_id = number
          while (neighPoints.nonEmpty) { //对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
            yTempPoint = neighPoints.head //取集合中第一个点
            index = data.indexOf(yTempPoint)
            if (visited(index) == 0) { //若该点未被处理，则标记已处理
              visited(index) = 1
              if (cluster(index).cluster_id == 0) cluster(index).cluster_id = number //划分到与核心点一样的簇中
              distanceTemp = data.map(x =>(gen_sim(x.vector, yTempPoint.vector), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
              neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点
              if (neighPointsTemp.length >= minPts) {
                cluster(index).is_type = 1 //该点为核心点
                for (i <- neighPointsTemp.indices) { //将其领域内未分类的对象划分到簇中,然后放入neighPoints
                  if (cluster(data.indexOf(neighPointsTemp(i))).cluster_id == 0) {
                    cluster(data.indexOf(neighPointsTemp(i))).cluster_id = number //只划分簇，没有访问到
                    neighPoints += neighPointsTemp(i)
                  }
                }
              }
              if (neighPointsTemp.length > 1 && neighPointsTemp.length < minPts) {
                breakable {
                  for (i <- neighPointsTemp.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
                    val index1 = data.indexOf(neighPointsTemp(i))
                    if (cluster(index1).is_type == 1) {
                      cluster(index).is_type = 0 //边界点
                      break
                    }
                  }
                }
              }
            }
            neighPoints -= yTempPoint //将该点剔除
          } // end-while
          number += 1 //进行新的聚类
        }
      }
    }
    cluster.map(a => a.cluster_id -> a.input).groupBy(_._1).map(m => m._2.map(p => {
      p._2.cluster_id = m._1
      p._2
    })).toArray
  }


  def dbScanWords(arrdata: Array[LabelData], ePs: Double, minPts: Int): Array[Array[LabelData]] = {
    val data = arrdata
    //    println(data.head.length)
    //    val types = (for (i <- data.indices) yield -1).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
    //    val types: Array[ClusterTypes] = data.map(a => ClusterTypes(a))
    val visited = (for (i <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
    var number = 1 //用于标记类
    //    val dv: DenseVector[Double] = DenseVector.zeros(data.head.length)
    var xTempPoint: LabelData = null
    var yTempPoint: LabelData = null
    var distance = new Array[(Double, Int)](1)
    var distanceTemp = new Array[(Double, Int)](1)
    val neighPoints = new ArrayBuffer[LabelData]()
    var neighPointsTemp = new Array[LabelData](1)
    val cluster = data.map(m => LabelsCluster(m)) //new Array[Clusters](data.length) //用于标记每个数据点所属的类别
    var index = 0
    for (i <- data.indices) {
      //对每一个点进行处理
      if (visited(i) == 0) { //表示该点未被处理
        visited(i) == 1 //标记为处理过
        xTempPoint = data(i) //取到该点
        distance = data.map(x => (comparison(x.topic_wrods, xTempPoint.topic_wrods), data.indexOf(x))) //取得该点到其他所有点的距离 Array{(distance,index)}
        neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点(密度相连点集合)
        if (neighPoints.nonEmpty && neighPoints.length < minPts) {
          breakable {
            for (i <- neighPoints.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
              val index: Int = data.indexOf(neighPoints(i))
              if (cluster(index).is_type == 1) {
                cluster(i).is_type = 0 //边界点
                break
              }
            }
          }
        }
        if (neighPoints.length >= minPts) { //核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
          cluster(i).is_type = 1
          cluster(i).cluster_id = number
          while (neighPoints.nonEmpty) { //对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
            yTempPoint = neighPoints.head //取集合中第一个点
            index = data.indexOf(yTempPoint)
            if (visited(index) == 0) { //若该点未被处理，则标记已处理
              visited(index) = 1
              if (cluster(index).cluster_id == 0) cluster(index).cluster_id = number //划分到与核心点一样的簇中
              distanceTemp = data.map(x => (comparison(x.topic_wrods, yTempPoint.topic_wrods), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
              neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点
              if (neighPointsTemp.length >= minPts) {
                cluster(index).is_type = 1 //该点为核心点
                for (i <- neighPointsTemp.indices) { //将其领域内未分类的对象划分到簇中,然后放入neighPoints
                  if (cluster(data.indexOf(neighPointsTemp(i))).cluster_id == 0) {
                    cluster(data.indexOf(neighPointsTemp(i))).cluster_id = number //只划分簇，没有访问到
                    neighPoints += neighPointsTemp(i)
                  }
                }
              }
              if (neighPointsTemp.length > 1 && neighPointsTemp.length < minPts) {
                breakable {
                  for (i <- neighPointsTemp.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
                    val index1 = data.indexOf(neighPointsTemp(i))
                    if (cluster(index1).is_type == 1) {
                      cluster(index).is_type = 0 //边界点
                      break
                    }
                  }
                }
              }
            }
            neighPoints -= yTempPoint //将该点剔除
          } // end-while
          number += 1 //进行新的聚类
        }
      }
    }
    cluster.map(a => a.cluster_id -> a.input).groupBy(_._1).map(m => m._2.map(p => {
      p._2.cluster_id = m._1
      p._2
    })).toArray
  }


  /**
    * 封装分词方法
    *
    * @param sentence   文本内容
    * @param stop_words 是否启用停用词处理
    * @return
    */
  def participle(sentence: String, stop_words: Boolean = true, rank: Boolean = false): (Seq[String], Seq[String]) = {
    var str = sentence.replaceAll("尊敬的读者[.]+|网页链接}[!@#$%&*()]-|<p>|</p>|", "")
    if (sentence == null) str = ""
    var result: List[String] = null
    var topic_words: List[String] = null
    if (sentence.nonEmpty) {
      val segs = segment.seg(str)
      if (stop_words) CoreStopWordDictionary.apply(segs)
      result = segs.map(_.word).toList
      if (rank) {
        //是否只提取主题关键词
        val keys = getRank.getKeywords(segs, 20)
        val words = keys.map(m => segs.find(t => t.word == m).head).filter(a => a.nature.startsWith("v") || (a.nature.startsWith("n") && a.word.length > 1))
        topic_words = words.map(m => if (m.nature.startsWith("nr")) m.word + "/nr" else m.word).toList
      }
    }
    else {
      result = List()
      topic_words = List()
    }
    (result, topic_words)
  }

  /**
    * 计算文字复制比的距离
    *
    * @param str1 待对比的参数1
    * @param str2 待对比的参数2
    * @return
    */
  def comparison(str1: String, str2: String): Double = {
    val words1 = str1.split(" ")
    val length1 = words1.size
    val words2 = str2.split(" ")
    val length2 = words2.size
    val inter = words1.intersect(words2)
    //    println(inter)
    val length3 = inter.length.toDouble
    val max_length = if (length1 > length2) length1.toDouble else length2.toDouble
    val com: Double = length3 / max_length
    1 - com //将其转化为值越小，距离越近
  }

}
