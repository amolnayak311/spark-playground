package org.sparkscala

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Dataframes {
  //Why dataframes??
  
  case class Person(ID:Int, name: String, age:Int, numFriends: Int)
  
  def main(args: Array[String]) = {
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      
      val spark = SparkSession.builder.appName("Dataframes").master("local[*]").getOrCreate()
      
      import spark.implicits._
      
      val lines = spark.sparkContext.textFile("data/fakefriends.csv")
      val people = lines.map{
        line =>
          val splits = line.split(",")
          Person(splits(0).toInt, splits(1), splits(2).toInt, splits(3).toInt)
      }
      
      val peopleDF = people.toDS().cache()
      
      println("Inferred Schema")
      peopleDF.printSchema()
      
      println("Lets select name column:")
      
      peopleDF.select("name").show()
      
      println("Filter anyone over 21")
      peopleDF.filter($"age" < 21).show()  // same as peopleDF.filter(people("age") < 21).show() 
      
      println("Group by age")
      peopleDF.groupBy("age").count().show()
      
      println("Make everyone 10 years older")
      
      peopleDF.select($"name", $"age" + 10).show()
      spark.stop()
      
      
      
  }
}