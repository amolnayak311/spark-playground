package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordCountBetterSorted {  
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WordCountBetterSorted")
    
    val lines = sc.textFile("data/book.txt")
    
    val wordsLowerCase = lines.flatMap(_.split("\\W+")).map(_.toLowerCase)
    
    val wordsCountRDD = wordsLowerCase.map(w => (w, 1)).reduceByKey(_ + _)
    
    val sortedWords = wordsCountRDD.map(_.swap).sortByKey()
    
   //Without collecting the results, the order od the RDD is not preserved when printed  
   // for(pair <- sortedWords) {
   //   printf("%s: %d\n", pair._2, pair._1)
   // }
    
    for((count, word) <- sortedWords.collect()) {
      printf("%s: %d\n", word, count)
    }
 }
}