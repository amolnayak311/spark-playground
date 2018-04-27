package org.sparkscala

import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PopularMovieDataset {
  
  def loadMovieNames() : Map[Int, String] = {
    implicit val codec = Codec("UTF8")
        .onMalformedInput(CodingErrorAction.REPLACE)
        .onUnmappableCharacter(CodingErrorAction.REPLACE)
    Source.fromFile("data/ml-100k/u.item").getLines().foldLeft(Map.empty[Int, String]){
      case (map, line) =>
        val splits = line.split("\\|")
        map + (splits(0).toInt -> splits(1))
    }
  }
  
  final case class Movie(movieId: Int)
  
  def main(args: Array[String]) = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
                  .appName("PopularMovieDataset")
                  .master("local[*]")
                  .getOrCreate()
                  
    import spark.implicits._
    
    val lines = spark.sparkContext.textFile("data/ml-100k/u.data")
    
    val movieDS = lines.map(line => Movie(line.split("\t")(1).toInt)).toDS()
    
    val topMovies = movieDS.groupBy($"movieId").count().orderBy(desc("count")).cache
    
    topMovies.show()
    
    // Grab the top 10
    val top10 = topMovies.take(10)
    
    val movieNames = loadMovieNames()
    println("Tot 10 movie names are\n")
    top10.foreach(row => println(movieNames(row(0).asInstanceOf[Int]) + ":"+ row(1)))    
  }
}