package scala.yunce9.clusters

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.control.Breaks.{break, breakable}


case class ClusterTypes(input: String, var cluster_id: Int = 0, var is_type: Int = 0)

case class ClusterH(input: HcData, var cluster_id: Int = 0, var is_type: Int = 0)

object CopyRatio {
  def main(args: Array[String]): Unit = {
    //    val a = "#刘强东京东股权#腾讯居然是大股东"
    //    val b = "#刘强东京东股权#突然吹出这种风，是要搞..."
    val start = System.currentTimeMillis()
    val minPts = 1 //密度阈值
    val ePs = 0.56 //领域半径
    //    val dim = 40 //数据集维度
    // 处理输入数据
    val fileName = "D:/gitProject/yunce8/src/data/titles.txt"
    val lines = Source.fromFile(fileName).getLines().map(m => m.replaceAll("[\\#...【图】：。 ]+", "")).toArray
    val cluster = ratioCluster(lines, ePs, minPts)
    cluster.map(m => m.cluster_id -> m.input).toList.sortBy(_._1).foreach(println)
    println(cluster.map(_.cluster_id).toSet.size)
    println(s"用时：${System.currentTimeMillis() - start}")
  }

  def applyClusters(arrdata: Array[HcData], ePs: Double, minPts: Int): Array[Array[HcData]] = {
    val data = arrdata
    //    println(data.head.length)
    //    val types = (for (i <- data.indices) yield -1).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
    //    val types: Array[ClusterTypes] = data.map(a => ClusterTypes(a))
    val visited = (for (i <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
    var number = 1 //用于标记类
    //    val dv: DenseVector[Double] = DenseVector.zeros(data.head.length)
    var xTempPoint: HcData = null
    var yTempPoint: HcData = null
    var distance = new Array[(Double, Int)](1)
    var distanceTemp = new Array[(Double, Int)](1)
    val neighPoints = new ArrayBuffer[HcData]()
    var neighPointsTemp = new Array[HcData](1)
    val cluster = data.map(m => ClusterH(m)) //new Array[Clusters](data.length) //用于标记每个数据点所属的类别
    var index = 0
    for (i <- data.indices) {
      //对每一个点进行处理
      if (visited(i) == 0) { //表示该点未被处理
        visited(i) == 1 //标记为处理过
        xTempPoint = data(i) //取到该点
        distance = data.map(x => (comparison(x.title, xTempPoint.title), data.indexOf(x))) //取得该点到其他所有点的距离 Array{(distance,index)}
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
              distanceTemp = data.map(x => (comparison(x.title, yTempPoint.title), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
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
    cluster.map(a=>a.cluster_id->a.input).groupBy(_._1).map(m=>m._2.map(_._2)).toArray
  }


  def ratioCluster(mydata: Array[String], ePs: Double, minPts: Int): Array[ClusterTypes] = {
    val data = mydata
    //    println(data.head.length)
    //    val types = (for (i <- data.indices) yield -1).toArray //用于区分核心点1，边界点0，和噪音点-1(即cluster中值为0的点)
    //    val types: Array[ClusterTypes] = data.map(a => ClusterTypes(a))
    val visited = (for (i <- data.indices) yield 0).toArray //用于判断该点是否处理过，0表示未处理过
    var number = 1 //用于标记类
    //    val dv: DenseVector[Double] = DenseVector.zeros(data.head.length)
    var xTempPoint = ""
    var yTempPoint = ""
    var distance = new Array[(Double, Int)](1)
    var distanceTemp = new Array[(Double, Int)](1)
    val neighPoints = new ArrayBuffer[String]()
    var neighPointsTemp = new Array[String](1)
    val cluster = data.map(m => ClusterTypes(m)) //new Array[Clusters](data.length) //用于标记每个数据点所属的类别
    var index = 0
    for (i <- data.indices) {
      //对每一个点进行处理
      if (visited(i) == 0) { //表示该点未被处理
        visited(i) == 1 //标记为处理过
        xTempPoint = data(i) //取到该点
        distance = data.map(x => (comparison(x, xTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离 Array{(distance,index)}
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
              distanceTemp = data.map(x => (comparison(x, yTempPoint), data.indexOf(x))) //取得该点到其他所有点的距离Array{(distance,index)}
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
    cluster
  }

  /**
    * 计算文字复制比的距离
    *
    * @param str1 待对比的参数1
    * @param str2 待对比的参数2
    * @return
    */
  def comparison(str1: String, str2: String): Double = {
    val words1 = str1.split("")
    val length1 = words1.size
    val words2 = str2.split("")
    val length2 = words2.size
    val inter = words1.intersect(words2)
    //    println(inter)
    val length3 = inter.length.toDouble
    val max_length = if (length1 > length2) length1.toDouble else length2.toDouble
    val com: Double = length3 / max_length
    1 - com //将其转化为值越小，距离越近
  }
}
