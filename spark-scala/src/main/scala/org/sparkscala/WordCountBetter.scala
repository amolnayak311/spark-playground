package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordCountBetter {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCountBetter")
    val lines = sc.textFile("data/book.txt")
    val lowerCaseRDD = lines.flatMap(_.split("\\W+")).map(_.toLowerCase)
    val countByValue = lowerCaseRDD.countByValue()    
    countByValue.foreach(println)
  }
}