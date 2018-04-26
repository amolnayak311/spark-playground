package org.sparkscala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkSQL {
 
    case class Person(ID:Int, name: String, age:Int, numFriends: Int)
    
    
    def main(args: Array[String]) = {
      Logger.getLogger("org").setLevel(Level.ERROR)
      val spark = SparkSession.builder().master("local[*]").appName("SparkSQL").getOrCreate()      
      import spark.implicits._
      
      val lines = spark.sparkContext.textFile("data/fakefriends.csv")
      
      val people = lines.map {
        line => 
          val splits = line.split(",")
          Person(splits(0).toInt, splits(1), splits(2).toInt, splits(3).toInt)
      }
      
      val schemaPeople = people.toDS
      
      schemaPeople.printSchema()
      
      schemaPeople.createOrReplaceTempView("people")
      
      val teenagers = spark.sql("select * from people where age between 13 and 19")
      
      teenagers.collect.foreach(println)
      
      spark.stop()
      
    }
}