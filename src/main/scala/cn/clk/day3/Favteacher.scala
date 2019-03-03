package cn.clk.day3

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by clk on 2019/2/23.
  */
object Favteacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    val teacherAndOne = lines.map(line => {
      val splits: Array[String] = line.split("/")
      //      val subject = splits(2).split("[.]")(0)
      val teacher = splits(3)
      (teacher, 1)
    })
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_ + _)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)
    val result: Array[(String, Int)] = sorted.collect()
    println(result.toBuffer)
    sc.stop()


  }
}
