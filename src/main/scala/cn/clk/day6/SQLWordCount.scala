package cn.clk.day6

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by clk on 2019/2/25. 
  */
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SQLWordCount").getOrCreate()
    //dataset只有一列，默认这列叫value
    val lines: Dataset[String] = spark.read.textFile(args(0))
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    words.createTempView("v_wc")
    val result: DataFrame = spark.sql("select value ,count(*) counts from v_wc group by value order by counts desc")

    result.show()
    spark.stop()


  }

}
