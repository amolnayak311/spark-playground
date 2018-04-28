package org.sparkscala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating


object MovieRecommendationsALS {
  
  def loadMovieNames(): Map[Int, String] = {
    
    implicit val codec = Codec("UTF8")
                      .onMalformedInput(CodingErrorAction.REPLACE)
                      .onUnmappableCharacter(CodingErrorAction.REPLACE)
                      
    Source.fromFile("data/ml-100k/u.item").getLines.foldLeft(Map.empty[Int, String]) {
      (map, line) =>
        val splits = line.split("\\|")
        map + (splits(0).toInt -> splits(1))
    }
  }
  
  def main(args: Array[String]) {
    val userId = args(0).toInt
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("MovieRecommendationsALS").master("local[*]").getOrCreate()
    
    import spark.implicits._
    
    val ratings = spark.sparkContext.textFile("data/ml-100k/u.data").map{
      line =>
        val splits = line.split("\t")
        Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
    }.cache()
    
    val userRatings = ratings.filter(_.user == userId).collect()
    val movies = loadMovieNames();
    //TODO: Fix StackOverFlowError
    val model = ALS.train(ratings, 8, 20)
    
    println("Existing ratings for the user are\n")
    userRatings.foreach(x => println(movies(x.product) + ": " + x.rating))
       
    println("\nTop 10 predictions for the user are\n")
    
    val recommendations = model.recommendProducts(userId, 10)
    recommendations.foreach(x => println(movies(x.product) + ": " + x.rating))
    
    
    
  }
}