package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object FriendsByFirstName {
  
  
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "FriendsByFirstName")
    
    val lines = sc.textFile("data/fakefriends.csv")
    
    val rdd = lines.map(line => {
      val splits = line.split(",")
      (splits(1), splits(3).toInt)
    })
    
    val totalByName = rdd.mapValues(numFriends => (numFriends, 1)).reduceByKey {
      case ((numF1, count1), (numF2, count2)) => (numF1 + numF2, count1 + count2)
    }
    
    val averageFriendsByName = totalByName.mapValues{
      case (numFriends, numInstances) => numFriends / numInstances
    }
    
    val result = averageFriendsByName.collect()
    
    result.sortBy(_._1).foreach(println)
    
  }
}