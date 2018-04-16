package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordCount {
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCount")
    val lines = sc.textFile("data/book.txt")
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.countByValue() 
    wordCount.foreach(println)
  }
}