package org.sparkscala

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import scala.io.Source
import sun.security.util.Length



object MostPopularSuperhero {
  
  //We change the use case a bit in this example to find the top 15 most popular characters
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "MostPopularSuperhero")
    
    val idNamePairRDD = sc.textFile("data/Marvel-names.txt").flatMap{
      line =>        
        val splits = line.split('\"')
        if(splits.length > 1) {          
          Some(splits(0).trim.toInt, splits(1))
        } else {
            None
        }
    }
    
    val friendNumFriendsPairRDD = sc.textFile("data/Marvel-graph.txt").map {
      line => 
        val splits = line.split("\\s+")
        (splits.head.toInt, splits.tail.length)
    }.reduceByKey(_ + _)
    
    val joinedRDD = friendNumFriendsPairRDD.join(idNamePairRDD).map {
      case (_, (numFriends, name)) => (numFriends, name)
    }
    
    val friendsSortedByNumFriends = joinedRDD.sortByKey(ascending = false).map(_.swap)
    
    println("Top 15 most popular characters are ")    
    friendsSortedByNumFriends.take(15).foreach(println)                                        
    
  }
}