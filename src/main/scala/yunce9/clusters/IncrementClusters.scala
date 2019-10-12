package scala.yunce9.clusters

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.mining.word2vec
import com.hankcs.hanlp.mining.word2vec.{DocVectorModel, WordVectorModel}
import com.hankcs.hanlp.seg.Segment
import MySpark._
import spark.implicits._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.yunce9.clusters.ClusteringValidation.participle


case class Increment(id: Int, input: LabelData, var vector: word2vec.Vector,var processed: Int = 0)

//case class ClusterH(input: HcData, var cluster_id: Int = 0, var is_type: Int = 0)


object IncrementClusters {
  //  HanLP.Config.IOAdapter = new HadoopFileIoAdapter()
  val segment: Segment = HanLP.newSegment().enableCustomDictionaryForcing(true).enableAllNamedEntityRecognize(true)
  val docVectorModel = new DocVectorModel(new WordVectorModel("D:/HanLP/hanlp-wiki-vec-zh.txt"))

  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val fileName = "D:/gitProject/yunce9/src/main/scala/yunce9/datas/labels.csv"
    val documents = Source.fromFile(fileName).getLines().map(m => m.replaceAll("[/#...【图】：。 ]+", "")).toArray
    val labelDatas = Source.fromFile(fileName).getLines().map(r => {
      val row = r.split(",")
      val topicwords = participle(row(3), rank = true)._2.mkString(" ")
      LabelData(row(0), row(1).toLong, row(2), row(3), topicwords, row(4), row(5))
    }).toArray
    val arr = new ArrayBuffer[Increment]()
    for (i <- labelDatas.indices) {
      arr.append(Increment(i, labelDatas(i), docVectorModel.addDocument(i, labelDatas(i).title)))
    }
    val result = incrementClusters(new ArrayBuffer[(ArrayBuffer[Increment], word2vec.Vector)](), arr.toArray)
    var cluster_id: Int = 1
    val labels = result.flatMap(a => {
      val buff = a._1.map(p => {
        p.input.cluster_id = cluster_id
        p.input
      })
      cluster_id += 1
      buff
    })

    labels.toDS().repartition(1).write.mode("overwrite").csv("D:/gitProject/yunce9/src/main/scala/yunce9/datas/incre")

    labels.foreach(println)
    println(labels.size)
    println(s"用时：${System.currentTimeMillis() - start}")
  }

  def incrementClusters(centers: ArrayBuffer[(ArrayBuffer[Increment], word2vec.Vector)], dv: Array[Increment], distinct: Double = 0.5):
  ArrayBuffer[(ArrayBuffer[Increment], word2vec.Vector)] = {
//    var clusterid = 0
    if (centers.isEmpty) {
//      dv.head.cluster_id = clusterid
//      clusterid += 1
      centers.append((ArrayBuffer(dv.head), dv.head.vector))
      dv.head.processed = 1
      centers ++ incrementClusters(centers, dv.filter(_.processed == 0))
    }
    else {
      dv.foreach(f => {
        if (f.processed == 0) {
          if (f.vector == null) f.vector = new word2vec.Vector(300)
          centers.foreach(c => {
            if (f.processed == 0) {
              if (c._2.cosine(f.vector) >= distinct) {
                c._1.append(f)
                //c._2 =
                f.processed = 1
              }
            }
          })
          if (f.processed == 0) {
            centers.append((ArrayBuffer(f), f.vector))
            f.processed = 1
          }
        }
      })
    }
    centers
  }

}

