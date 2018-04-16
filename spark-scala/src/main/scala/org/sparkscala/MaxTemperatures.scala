package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MaxTemperatures {
  
  def main(args : Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MaxTemperatures")
    
    val lines = sc.textFile("data/1800.csv")
    val rdd = lines.map(line => {
      val splits = line.split(",")
       (splits(0), splits(2), splits(3).toDouble * 0.1 * 1.8 + 32 )
    })
    
    val maxTemp = rdd.filter(_._2 == "TMAX")
    
    val stationsByTemperature = maxTemp.map(x => (x._1, x._3))
    
    val maxTempByStations = stationsByTemperature.reduceByKey((t1, t2) => math.max(t1, t2))
    
     maxTempByStations.foreach{
        case (station, temp) => printf("\nStation %s maximum temperature: %.2f F", station, temp)
      }
    
  }
}