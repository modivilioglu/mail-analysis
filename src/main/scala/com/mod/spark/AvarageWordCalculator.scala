package com.mod.spark

import org.apache.spark.SparkContext

class AvarageWordCalculator extends java.io.Serializable {
  
  private def getRDD(sc: SparkContext, path: String) = sc.wholeTextFiles(path + "*.txt")
  // Maps each of the file to (1, number of words)
  // Reduce the pair summing both elements
  // Dividing the total number of words to total number of files.
  
  def preProcessToPairs(sc: SparkContext, path: String) = getRDD(sc, path).map(x => (1, x._2.split(" ").length)).reduce((a,b) => (a._1 + b._1, a._2 + b._2))

  def getAvarageWordsPerMail(sc: SparkContext, path: String): Long = {
    val (totalFiles, totalWords) = preProcessToPairs(sc, path)
    
    totalWords / totalFiles
  }
    
}
