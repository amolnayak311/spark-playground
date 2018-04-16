package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object FriendsByAge {
  
  
  def main(args: Array[String]) = {
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val sc = new SparkContext("local[*]", "FriendsByAge")
      val lines = sc.textFile("data/fakeFriends.csv")
      
      val rdd = lines.map(line => {
          val splits = line.split(",")
          (splits(2).toInt, splits(3).toInt)
      })
      
      val totalByAge = rdd.mapValues(numFriends => (numFriends, 1)).reduceByKey{
        case ((numFriends1, total1), (numFriends2, total2)) => 
            (numFriends1 + numFriends2, total1 + total2)
      }
      val averageByAge = totalByAge.mapValues(x => x._1 / x._2)
      
      val results = averageByAge.collect()
      
      results.sorted.foreach(println)
      
  }
}