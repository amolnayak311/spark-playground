package org.sparkscala

import scala.io.Source
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.streaming.twitter.TwitterUtils

object PopularHashtags {
  
  def setupTwitter() {
    Source.fromFile("data/twitter.txt").getLines().foreach {
      line => 
        val splits = line.split(" ")
        if (splits.length > 1) {          
          System.setProperty("twitter4j.oauth." + splits(0), splits(1))
        }
    }
  }
  
  def main(args: Array[String]) {
    setupTwitter()
    val rootLogger = Logger.getLogger("org")
    rootLogger.setLevel(Level.ERROR)
    
    val ssc = new StreamingContext("local[*]", "PopularHashtags", Seconds(1))
    
    val dstream = TwitterUtils.createStream(ssc, None)
    
    val hashtags = dstream.flatMap(_.getText.split(" ")).filter(_.startsWith("#"))

    
    val hashtagkv = hashtags.map(h => (h, 1))
    
    val hashTagCounts = hashtagkv.reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))
    
    val sortedResults = hashTagCounts.transform(_.sortBy(_._2, ascending = false))
    
    sortedResults.print()
    ssc.checkpoint("./checkpoint")
    ssc.start()
    ssc.awaitTermination()
    
  }
}