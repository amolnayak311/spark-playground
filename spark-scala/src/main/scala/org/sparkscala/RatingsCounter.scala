package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level


object RatingsCounter {
  
  def main(args: Array[String]): Unit = {
    
   Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "RatingsCounter")
    
    val lines = sc.textFile("./data/ml-100k/u.data")
    val ratings = lines.map(_.split("\t")(2))
    
    val results = ratings.countByValue()
    val sortedResults = results.toSeq.sortBy(_._1)
    println("")
    sortedResults.foreach(println)    
    
  }
}