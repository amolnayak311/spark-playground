package org.sparkscala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import org.apache.spark.SparkContext

object PopularMovieBetter {
  
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    val mapMovieNames = Source.fromFile("data/ml-100k/u.item").getLines().foldLeft(Map.empty[Int, String]){
      case (map, line) =>
        val splits = line.split("\\|")       
        if (splits.length > 1)
          map + (splits(0).toInt -> splits(1))
        else
          map
    }
    
    val sc = new SparkContext("local[*]", "PopularMovieBetter")
    val movieNamesBroadcast = sc.broadcast(mapMovieNames)
    
    
    val lines = sc.textFile("data/ml-100k/u.data")
    
    val movieRatings = lines.map(line => (line.split("\t")(1).toInt, 1)).reduceByKey(_ + _)
    val movieRatingsSorted = movieRatings.map(_.swap).sortByKey(ascending = false)
    
    
    val mostRatedMovied = movieRatingsSorted.map(x => (movieNamesBroadcast.value(x._2), x._1)).collect()
    
    mostRatedMovied.foreach(println)
    
    
  }
}