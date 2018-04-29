package org.sparkscala

import scala.io.Codec
import scala.io.Source
import java.nio.charset.CodingErrorAction
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.recommendation.ALS
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.Row

object MovieRecALSDataframes {
  
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
   
   case class Rating(userId: Int, movieId: Int, rating: Float)
  
  def main(args: Array[String]) {
    
    val userId = args(0).toInt
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("MovieRecALSDataframes").master("local[*]").getOrCreate()
    
    import spark.implicits._
    
    val ratingsDF = spark.sparkContext.textFile("data/ml-100k/u.data").map {
          line =>
            val splits = line.split("\t")
            Rating(splits(0).toInt, splits(1).toInt, splits(2).toFloat)
        }.toDF
    
    //val Array(trainData, testData) = ratingsDF.randomSplit(Array(0.8, 0.2))
    val als = new ALS().setUserCol("userId").setRatingCol("rating").setItemCol("movieId").setMaxIter(10)
    //val alsModel = als.fit(trainData)
    val alsModel = als.fit(ratingsDF)
    
    //val transformed = alsModel.transform(testData)
    alsModel.setColdStartStrategy("drop")
    
    //Show the top 10 recommendations for the user
    val result = alsModel.recommendForAllUsers(10)
                  .where($"userId" === userId)
                  .select("recommendations").collect()
    val movies = loadMovieNames()
    val recommendations = result(0).getAs[WrappedArray[Row]](0)
    println()
    recommendations.foreach{
      e =>
        println(movies(e.getAs[Int](0)) + ": " + e.getAs[Float](1))
     }
  }
}


