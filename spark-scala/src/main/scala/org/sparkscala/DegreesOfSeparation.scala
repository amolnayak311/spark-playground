package org.sparkscala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

import org.apache.spark.util.LongAccumulator
import org.apache.spark.rdd.RDD


object DegreesOfSeparation {

    val startCharacterId = 5306    //Spider Man
    val targetCharacterId = 14  //Adam 3,031
    
    type BFSData = (Array[Int], Int, String)  //All Friends, degrees of freedom, color
    type BFSNode = (Int, BFSData)    //Tuple of characterId and BFS Data for that character

   
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "DegreesOfSeparation")
    
    val hitCounter: LongAccumulator = sc.longAccumulator("HitCounter")
    
    //Reduce the network to ensure each super hero has a unique key is there are multiple lines for same super hero
    val network = sc.textFile("data/Marvel-graph.txt").map{
      line => 
        val splits = line.split("\\s+")
        (splits.head.toInt, splits.tail.map(_.toInt))
    }.reduceByKey(_ ++ _)
    
    
     /**
     *  Converts a line from raw input to a BFS node
     */
    def convertToBFS(superHeroId: Int, connections: Array[Int]): BFSNode = {
      
      val bfsData = if(superHeroId == startCharacterId) {
          (connections, 0, "GRAY")
      } else {
        (connections, 9999, "WHITE")
      }
      
      (superHeroId, bfsData)
    }
    
    /**
     *  Expands a gray node into its connections. Increments the accumulator as soon as the target character is reached 
     */
    def flatMapper(bfsNode: BFSNode) : List[BFSNode] =  bfsNode match {
      case (id, (connections, degrees, "GRAY")) => 
       connections.foldLeft((id, (connections, degrees, "BLACK")) :: List.empty[BFSNode]) {
        case (accumulator, connection) =>
          if (connection == targetCharacterId) {
            hitCounter.add(1)
          }
          (connection, (Array.empty[Int], degrees + 1, "GRAY")) :: accumulator 
        }
         
      case _ => bfsNode :: Nil
    }
    
    /**
     * Combines different paths to a superhero into one by retaining the smallest distance and keping the darkest color 
     */
    def reduceSuperHero(bfsData1: BFSData, bfsData2: BFSData): BFSData = {      
      //Retain the one with darkest shade
      val nodeColor = (bfsData1, bfsData2) match {
        case ((_, _, "BLACK"), (_, _, _)) | ((_, _, _), (_, _, "BLACK")) => "BLACK"
        case ((_, _, "GRAY"), (_, _, _)) | ((_, _, _), (_, _, "GRAY")) => "GRAY"
        case _ => "WHITE"
      }
      
      val distance = if (bfsData1._2 < bfsData2._2) bfsData1._2 else bfsData2._2
      
      val newConnections = (bfsData1, bfsData2) match {
        case ( (Array(), _, _), (other, _, _)) => other
        case ( (other, _, _), (Array(), _, _)) => other
        case _ => bfsData1._1
      }

      (newConnections, distance, nodeColor)
    }
    
    var bfsDataNode = network.map{
      case(superHeroId, connections) =>   convertToBFS(superHeroId, connections)
    }
    
    
    for (i <- 1 to 10 if hitCounter.value == 0) {
      println(s"Iteration $i")
      val mappedRDD = bfsDataNode.flatMap(flatMapper)      
      println("Mapped RDD contains " + mappedRDD.count() + " connections")
      if (hitCounter.value > 0) {
        println(s"Reached target connection in $i iterations")        
      } else { 
        bfsDataNode = mappedRDD.reduceByKey(reduceSuperHero)
      }
    }
  }
}