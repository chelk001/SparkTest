package cn.clk.day3

/**
  * Created by clk on 2019/2/23.
  */
object TestSplit {
  def main(args: Array[String]): Unit = {
    val line = "http://bigdata.edu360.cn/laozhang"
    val splits: Array[String] = line.split("/")
    val subject = splits(2).split("[.]")(0)
    val teacher = splits(3)
    println(subject + " " + teacher)
  }
}
