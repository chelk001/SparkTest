package cn.clk.day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by clk on 2019/2/24.
  */
object GroupFavTeacher3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val subjectAndteacher: RDD[((String, String), Int)] = lines.map(line => {
      val splits: Array[String] = line.split("/")
      val subject = splits(2).split("[.]")(0)
      val teacher = splits(3)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subjectAndteacher.reduceByKey(_ + _)

    //计算有多少学科
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    val subpartitioner = new SubjectPartitioner(subjects)
    //自定义一个分区器，并且按照指定的分区器进行分区
    val partiitioned: RDD[((String, String), Int)] = reduced.partitionBy(subpartitioner)
    val sorted: RDD[((String, String), Int)] = partiitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(3).toIterator
    })
    val result: Array[((String, String), Int)] = sorted.collect()
    print(result.toBuffer)
    sc.stop()
  }
}

class SubjectPartitioner(subjects: Array[String]) extends Partitioner {

  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for (sb <- subjects) {
    rules(sb) = i
    i += 1
  }

  //返回分区数量（下一个RDD有多少个分区）
  override def numPartitions: Int = subjects.length

  //根据传入的key计算分区编号
  override def getPartition(key: Any): Int = {
    //获取学科名称
    val subject = key.asInstanceOf[(String, String)]._1
    rules(subject)
  }
}