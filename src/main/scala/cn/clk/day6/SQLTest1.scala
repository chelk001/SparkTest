package cn.clk.day6

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by clk on 2019/2/25. 
  */
object SQLTest1 {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("SQLTest1").master("local[*]").getOrCreate()
    val lines: RDD[String] = session.sparkContext.textFile(args(0))

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
    val df: DataFrame = session.createDataFrame(rowRDD,schema)

    df.show()
    session.stop()

  }
}
