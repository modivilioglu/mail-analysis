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
  // Uses the databricks library to read the tags
  // Reads the Row through pattern matching 
  // Filters only the pairs that are (to, emailRecipent)
  // Finally forms a pair (emailRecipient, 1) to pass it to the reduce phace
  def getPairs(sc: SparkContext, path: String) = {
    val rddForMap = getRDD(sc, path)
    rddForMap.map(r=>r.getList(0).toArray).flatMap(x => x.map{case Row(a,b,c,d) => (b,c)}).filter(a=> a._1 == "#To").map(a=>(a._2, 1))
  }
  
  // Sums up the emailRecipents, 1 by 1
  // Sorts the emailRecipents in descenting order, the most frequent being the first
  // Returns the first 100 pairs that it collected
  def reduce(sc: SparkContext, path: String) = {
    val emailCountPairs = getPairs(sc, path)
    val summedPairs = emailCountPairs.reduceByKey(_ + _)
    val sortedPairs = summedPairs.map(item => item.swap).sortByKey(false)
    sortedPairs.map(x => x._2).take(100)
  }
}