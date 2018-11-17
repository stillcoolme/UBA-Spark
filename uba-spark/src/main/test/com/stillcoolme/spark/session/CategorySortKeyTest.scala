package com.stillcoolme.spark.session

import com.stillcoolme.spark.scala.SortKey
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CategorySortKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortKeyTest").setMaster("local")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    var array = Array(Tuple2(new SortKey(30, 35, 40), "1"),
                      Tuple2(new SortKey(35, 30, 40), "2"),
                      Tuple2(new SortKey(30, 38, 30), "3"))

    val sc = spark.sparkContext
    var rdd = sc.parallelize(array, 1)

    val sortRdd = rdd.sortByKey(false)

    for(tuple <- sortRdd.collect()){
      println(tuple._2)
    }

  }
}
