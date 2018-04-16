package org.sparkscala

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PurchaseByCustomer {
  
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")
    
    val lines = sc.textFile("data/customer-orders.csv")
    val customerAmountPairRDD = lines.map{ line =>
      val splits = line.split(",")
      (splits(0).toInt, splits(2).toDouble)
    }
    
    val aggregatedCustomerPurchase = customerAmountPairRDD.reduceByKey(_ + _).collect
    aggregatedCustomerPurchase.foreach{case (custId, aggregateAmount) =>
      printf("%s : %.2f\n", custId, aggregateAmount)
    }
  }
}