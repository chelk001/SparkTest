package cn.clk.day6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by clk on 2019/2/25. 
  */
object SQLDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SQLDemo3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val lines: RDD[String] = sc.textFile(args(0))
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id: Long = fields(0).toLong
      val name: String = fields(1)
      val age: Int = fields(2).toInt
      val fv: Double = fields(3).toDouble
      Row(id, name, age, fv)
    })
    //结果类型其实就是表头用于 描述DF
    val schema = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))
    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, schema)
    val df1: DataFrame = bdf.select("name","age","fv")
    import sqlContext.implicits._
    val df2: Dataset[Row] = df1.orderBy($"fv" desc,$"age" asc)
    df2.show()
    sc.stop()
  }
}
