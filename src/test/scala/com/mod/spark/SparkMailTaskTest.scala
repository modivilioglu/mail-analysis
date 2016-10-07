package com.mod.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.BeforeAndAfter
import org.scalatest.FlatSpec
import org.scalatest.Matchers
class SparkMailTaskSpec extends FlatSpec with BeforeAndAfter with Matchers {

  private val master = "local[2]"
  private val appName = "example-spark"
  private val pathForMails = "src/main/resources/text_000/"
  private val pathForXML = "src/main/resources/1.xml"
  private var sc: SparkContext = _
  
  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
  
  "The avarage number of words each mail contains" should "be correct" in {
    val awc = new AvarageWordCalculator
    awc.getAvarageWordsPerMail(sc, pathForMails) should be > (330L)
    awc.getAvarageWordsPerMail(sc, pathForMails) should be < (370L)
  }
  
  "The first 100 recipents" should "be correct" in {
    val trc = new TopRecipentsCalculator
    val result = trc.reduce(sc, pathForXML) 
    result.foreach(x => println(x))
    result.length should equal(100)
    result(0).toString().trim() should be("Whitt  Mark <Mark.Whitt@ENRON.com>")
  }

}