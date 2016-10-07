package com.mod.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import com.databricks.spark._
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
class TopRecipentsCalculator {
  def getRDD(sc: SparkContext, path: String) = {
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.format("com.databricks.spark.xml").option("rowTag", "Tags").load(path)
    df.rdd
  }
  def getPairs(sc: SparkContext, path: String) = {
    val rddForMap = getRDD(sc, path)
    rddForMap.map(r=>r.getList(0).toArray).flatMap(x => x.map{case Row(a,b,c,d) => (b,c)}).filter(a=> a._1 == "#To").map(a=>(a._2, 1))
  }
  def reduce(sc: SparkContext, path: String) = {
    val emailCountPairs = getPairs(sc, path)
    val summedPairs = emailCountPairs.reduceByKey(_ + _)
    val sortedPairs = summedPairs.map(item => item.swap).sortByKey(false)
    sortedPairs.map(x => x._2).take(100)
  }
}