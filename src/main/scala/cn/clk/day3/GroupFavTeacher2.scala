package cn.clk.day3

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Created by clk on 2019/2/24.
  */
object GroupFavTeacher2 {
  def main(args: Array[String]): Unit = {
    val subjects = Array("bigdata", "javaee", "php")
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
    //该RDD中对应的数据仅有一个学科的数据（因为过滤了）
    //    val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == "bigdata")
    //    val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(3)
    //    println(favTeacher.toBuffer)
    for (sb <- subjects) {
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)
      val favTeacher: Array[((String, String), Int)] = filtered.sortBy(_._2, false).take(3)
      println(favTeacher.toBuffer)
    }
    sc.stop()
  }
}
