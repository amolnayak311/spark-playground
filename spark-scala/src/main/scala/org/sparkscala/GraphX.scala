package org.sparkscala

import org.apache.spark.graphx.VertexId
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

object GraphX {
  
  def parseLine(line: String) : Option[(VertexId, String )]  = {
    val splits = line.split('\"')
    if(splits.length > 1) {
      val heroId = splits(0).trim().toLong
      if(heroId < 6487) Some(heroId, splits(1)) else None
    } else {
      None
    }    
  }
  
  def makeEdges(line: String): List[Edge[Int]] = {
    val splits = line.split(" ")
    val origin = splits(0).toLong
    splits.tail.foldLeft(List.empty[Edge[Int]]) { (accList, x) => 
      Edge(origin, x.toLong, 0) :: accList
    }
  }
 
  def main(args: Array[String]) {
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession.builder.appName("GraphX").master("local[*]").getOrCreate()
      
      val lines = spark.sparkContext.textFile("data/Marvel-names.txt")
      val vertexRDD = lines.flatMap(parseLine)
      val edges =  spark.sparkContext.textFile("data/Marvel-graph.txt")
      val edgeRDD = edges.flatMap(makeEdges)
      val default = "Nobody"
      val graph = Graph(vertexRDD, edgeRDD, default).cache()
      println("\nTop 10 most-connected superheroes:\n")
      // The join merges the hero names into the output; sorts by total connections on each node.
      graph.degrees.join(vertexRDD).sortBy(_._2._1, ascending = false).take(10).foreach(println)
      
      println("\nComputing degrees of separation from SpiderMan...")
    
      // Start from SpiderMan
      val root: VertexId = 5306 // SpiderMan
      
      
      //Degrees of separation using graphx
      //Map is needed to convert the Vertex[String, _] = Vertex[Double, _]
      val bfs = graph.mapVertices((_, _) =>  0.0).pregel(
                  Double.PositiveInfinity,  //Initial message 
                  10 // Max Iterations
                ) (
                //graph.map(vprog) will be initially by pregel operation internally
                vprog = (id, _, initMessage) => if(id == root) 0.0 else initMessage,
                //Triplet of source vertex value, dest vertex value (in this case a double) and the third 
                //if ignored which represents the edge value
                //In this case we simply emit the vertex where the destination is 1 more than source if
                //source is a finite value else empty
                triplet => {
                  if(triplet.srcAttr != Double.PositiveInfinity)
                    Iterator((triplet.dstId, triplet.srcAttr + 1))
                    else
                      Iterator.empty
                },
                //Merge two vertices retaining the minimum distance value to this vertex
                mergeMsg =  Math.min(_, _)
              )
      
      // Print out the first 100 results:
      bfs.vertices.join(vertexRDD).take(100).foreach(println)
      
      
      println("\n\nDegrees from SpiderMan to ADAM 3,031\n")  // ADAM 3031 is hero ID 14
      bfs.vertices.filter(x => x._1 == 14).collect.foreach(println)  
  }
}