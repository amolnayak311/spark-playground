package org.sparkscala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object PopularMovies {
  
   def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val sc = new SparkContext("local[*]", "PopularMovies")
      
      val lines = sc.textFile("data/ml-100k/u.data")
      
      val moviePairRDD = lines.map(x => (x.split("\t")(1).toInt, 1))
      
      val numRated = moviePairRDD.reduceByKey(_ + _)
      
      val mostPopular = numRated.map(_.swap).sortByKey(ascending = false)
      
      mostPopular.collect().foreach(x => println(x.swap))
      
      
      
   }
}