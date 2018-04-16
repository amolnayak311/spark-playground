package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
 * Find days with max precipitation for each location
 */
object MaxPrecipitation {
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "MaxPrecipitation")
    val lines = sc.textFile("data/1800.csv")
    val rdd = lines.map(line => {
        val splits = line.split(",")        
        (splits(0), splits(1), splits(2), splits(3).toInt)
    })
    
    val precipitationData = rdd.filter(_._3 == "PRCP")
    val stationWithDayAndPrecipitation = precipitationData.map(x => (x._1, (x._2, x._4)))
    
    val maxPrecipitationDayForStation = stationWithDayAndPrecipitation.reduceByKey(
            (x, y) => if (x._2 > y._2) x else y  
        )
        
    maxPrecipitationDayForStation.foreach{
      case (station, (day, prec)) =>  
         printf("\nFor station %s, max precipitation of %d was recorded on %s", station, prec, day ) 
    
    }
  }
  
  
    
}