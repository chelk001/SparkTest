package cn.clk.day3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by clk on 2019/2/24.
  */
object GroupFavTeacher1 {

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
    //分组排序
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
    //将每一个组拿出来进行操作
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    val r: Array[(String, List[((String, String), Int)])] = sorted.collect()
    println(r.toBuffer)
    sc.stop()


  }

}
