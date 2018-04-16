package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MinTemperatures {
  
  def main(args: Array[String]) {
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      val sc = new SparkContext("local[*]", "MinTemperatures")
      val lines = sc.textFile("data/1800.csv")
      val parsedLines = lines.map {
        line => {
          val splits = line.split(",")
          (splits(0), splits(2), splits(3).toDouble * 0.1 * 1.8 + 32 )
        }
      }
      
      val minTemps = parsedLines.filter(_._2 == "TMIN")
      
      val stationTemps = minTemps.map(x => (x._1, x._3))
      
      val minTempsByStation = stationTemps.reduceByKey((t1, t2) => math.min(t1, t2))
      
      minTempsByStation.foreach{
        case (station, temp) => printf("\nStation %s minimum temperature: %.2f F", station, temp)
      }
      
  }
}