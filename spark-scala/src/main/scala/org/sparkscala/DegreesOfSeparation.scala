package org.sparkscala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

import org.apache.spark.util.LongAccumulator


object DegreesOfSeparation {

    val startCharacterId = 5306    //Spider Man
    val targetCharacterId = 14  //Adam 3,031
    
    var hitCounter: Option[LongAccumulator] = None
    
    type BFSData = (Array[Int], Int, String)  //All Friends, degrees of freedom, color
    type BFSNode = (Int, BFSData)    //Tuple of characterId and BFS Data for that character

    /**
     *  Converts a line from raw input to a BFS node
     */
    def convertToBFS(line: String): BFSNode = {
      val splits = line.split("\\s+")
      
      val currCharacterId = splits.head.toInt
      
      val bfsData = if(currCharacterId == startCharacterId) {
          (splits.tail.map(_.toInt), 0, "GRAY")
      } else {
        (splits.tail.map(_.toInt), 9999, "WHITE")
      }
      
      (currCharacterId, bfsData)
    }
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "DegreesOfSeparation")
    
    val network = sc.textFile("data/Marvel-graph.txt")
    
    //TODO: Finish this implementation
  }
}