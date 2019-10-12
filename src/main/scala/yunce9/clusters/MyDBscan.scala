package scala.yunce9.clusters

import breeze.linalg.DenseVector

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks.{break, breakable}
import scala.yunce9.clusters.TestDBscan.gen_sim
import scala.yunce9.streaming.HierarchicalClustering.{bagOfWords2Vec, createVocabList, participle}

case class Clusters(input: String, var cluster_id: Int = 0)

object MyDBscan {
  def runDBSCAN(mydata: Array[(String, DenseVector[Double])], ePs: Double, minPts: Int): (Array[Clusters], Array[Int]) = {
    val data = mydata.map(_._2)
    println(data.head.length)
    val types = (for (i <- data.indices) yield -1).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
    val visited = (for (i <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
    var number = 1 //用于标记类
    val dv: DenseVector[Double] = DenseVector.zeros(data.head.length)
    var xTempPoint = dv
    var yTempPoint = dv
    var distance = new Array[(Double, Int)](1)
    var distanceTemp = new Array[(Double, Int)](1)
    val neighPoints = new ArrayBuffer[DenseVector[Double]]()
    var neighPointsTemp = new Array[DenseVector[Double]](1)
    val cluster = mydata.map(m => Clusters(m._1)) //new Array[Clusters](data.length) //用于标记每个数据点所属的类别
    var index = 0
    for (i <- data.indices) {
      //对每一个点进行处理
      if (visited(i) == 0) { //表示该点未被处理
        visited(i) == 1 //标记为处理过
        xTempPoint = data(i) //取到该点
        distance = data.map(x => (gen_sim(new DenseVector[Double](x.toArray), new DenseVector[Double](xTempPoint.toArray)), data.indexOf(x))) //取得该点到其他所有点的距离 Array{(distance,index)}
        neighPoints ++= distance.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点(密度相连点集合)
        if (neighPoints.length > 1 && neighPoints.length < minPts) {
          breakable {
            for (i <- neighPoints.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
              val index = data.indexOf(neighPoints(i))
              if (types(index) == 1) {
                types(i) = 0 //边界点
                break
              }
            }
          }
        }
        if (neighPoints.length >= minPts) { //核心点,此时neighPoints表示以该核心点出发的密度相连点的集合
          types(i) = 1
          cluster(i).cluster_id = number
          while (neighPoints.nonEmpty) { //对该核心点领域内的点迭代寻找核心点，直到所有核心点领域半径内的点组成的集合不再扩大（每次聚类 ）
            yTempPoint = neighPoints.head //取集合中第一个点
            index = data.indexOf(yTempPoint)
            if (visited(index) == 0) { //若该点未被处理，则标记已处理
              visited(index) = 1
              if (cluster(index).cluster_id == 0) cluster(index).cluster_id = number //划分到与核心点一样的簇中
              distanceTemp = data.map(x => (gen_sim(x, yTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
              neighPointsTemp = distanceTemp.filter(x => x._1 <= ePs).map(v => data(v._2)) //找到半径ePs内的所有点
              if (neighPointsTemp.length >= minPts) {
                types(index) = 1 //该点为核心点
                for (i <- neighPointsTemp.indices) { //将其领域内未分类的对象划分到簇中,然后放入neighPoints
                  if (cluster(data.indexOf(neighPointsTemp(i))).cluster_id == 0) {
                    cluster(data.indexOf(neighPointsTemp(i))).cluster_id = number //只划分簇，没有访问到
                    neighPoints += neighPointsTemp(i)
                  }
                }
              }
              if (neighPointsTemp.length >= 1 && neighPointsTemp.length <= minPts) {
                breakable {
                  for (i <- neighPointsTemp.indices) { //此为非核心点，若其领域内有核心点，则该点为边界点
                    var index1 = data.indexOf(neighPointsTemp(i))
                    if (types(index1) == 1) {
                      types(index) = 0 //边界点
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
    (cluster, types)
  }

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val minPts = 1 //密度阈值
    val ePs = 0.2 //领域半径
//    val dim = 40 //数据集维度
    // 处理输入数据
    val fileName = "D:/gitProject/yunce9/src/data/titles.txt"
    val lines = Source.fromFile(fileName).getLines()
    //    lines.foreach(println)
    val words = lines.map(line => line.replaceAll("[\\#...【图】：]", "").split("").toSeq).toSeq
//    val words = lines.map(line => participle(line.replaceAll("[\\#...【图】]", ""))).toSeq
    //    words.foreach(println)
    val vocab = createVocabList(words)
    println(vocab.length)
    val words2vec: Array[(String, DenseVector[Double])] = words.map(row => (row.mkString(""), bagOfWords2Vec(vocab, row))).toArray
    //    words2vec.foreach(println)
    val (cluster, types) = MyDBscan.runDBSCAN(words2vec, ePs, minPts)
    cluster.map(m => m.cluster_id -> m.input).toList.sortBy(_._1).foreach(println)
    types.foreach(println)
    println(cluster.map(_.cluster_id).toSet.size)
    println(s"用时：${(System.currentTimeMillis() - start) / 1000}")
  }
}
