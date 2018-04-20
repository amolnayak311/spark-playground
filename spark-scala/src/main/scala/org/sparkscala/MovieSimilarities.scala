package org.sparkscala

import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.io.Source
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object MovieSimilarities {
  
  def loadMovies(): Map[Int, String] = {
    implicit val codec = Codec("UTF8")
              .onMalformedInput(CodingErrorAction.REPLACE)
              .onUnmappableCharacter(CodingErrorAction.REPLACE)
    
              
    Source.fromFile("data/ml-100k/u.item").getLines().foldLeft(Map.empty[Int, String]) {
      case (accumulator, line) =>
        val splits = line.split("\\|")
        if(splits.length > 1)
          accumulator + (splits(0).toInt -> splits(1))
          else
            accumulator
    }
  }
  
  type MovieRating = (Int, Double)  
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  
  /**
   * Convert a users movie, rating pair into movies and ratings pair
   */
  def makePair(userRatingPair: UserRatingPair) = {
    val (_, ((movie1, rating1), (movie2, rating2))) = userRatingPair
    ((movie1, movie2), (rating1, rating2))
  }
  
  /**
   * 
   */
  def computeCosineSimilariy(ratingPairs: RatingPairs) = {
    val (numInstances, sumxx, sumxy, sumyy) = ratingPairs.foldLeft((0, 0.0, 0.0, 0.0)) {
      case ((accNumInst, accSumXX, accSumXY, accSumYY), (x, y)) =>        
        (accNumInst + 1, accSumXX + x * x, accSumXY + x * y, accSumYY + y * y)
    }
    
    val dr = math.sqrt(sumxx * sumyy) 
    if (dr == 0)
      (0.0, numInstances)
      else
        (sumxy / dr, numInstances)    
  }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val moviesMap = loadMovies();
    
    val sc = new SparkContext("local[*]", "MovieSimilarities")
    val rdd = sc.textFile("./data/ml-100k/u.data")
    
    val userRatingPair = rdd.map{
      line => 
        val splits = line.split("\t")
        (splits(0).toInt, (splits(1).toInt, splits(2).toDouble))
    }
    
    val userRatingJoinPairs:RDD[UserRatingPair] = (userRatingPair join userRatingPair).filter {
      case (_, ((movie1, _), (movie2, _))) => movie1 < movie2
    }
    
    //Key is pair of movies and values are all the values of user ratings for tehse two movies
    val moviePairRDDGrouped = userRatingJoinPairs.map(makePair).groupByKey()
    
    val similarities = moviePairRDDGrouped.mapValues(computeCosineSimilariy).cache()
    
    val scoreThreshold = 0.97
    val cooccuranceCountThreshold = 50
    
    //Validate Command line arg
    val movieId =  args(0).toInt
     
    val filteredResults = similarities.filter{
      case ((movie1, movie2), (score, cooccurance)) => 
        (movie1 == movieId || movie2 == movieId) &&
          score > scoreThreshold && cooccurance > cooccuranceCountThreshold
    }.map(_.swap)
    
    val results = filteredResults.sortByKey(ascending = false).take(10)
    
    
     
    results.foreach{
      case ((similarity, strength), (movie1, movie2)) =>
        val similarMovie = if (movie1 == movieId) movie2 else movie1
        println(moviesMap(similarMovie) + "\tscore: " + similarity + "\tstrength: " + strength)
    }
    
  }
}