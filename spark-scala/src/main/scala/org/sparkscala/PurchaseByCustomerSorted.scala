package org.sparkscala

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object PurchaseByCustomerSorted {
  
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "PurchaseByCustomerSorted")
    val lines = sc.textFile("data/customer-orders.csv")
    val customerPairRDD = lines.map{ line =>
      val splits = line.split(",")
      (splits(0).toInt, splits(2).toDouble)
    }
    
    val customerSpendOrdered =  customerPairRDD.reduceByKey(_ + _).map(_.swap).sortByKey().collect()
    
    customerSpendOrdered.foreach{
      case (amount, customerId) => printf("%s : %.2f\n", customerId, amount)
    }
    
  }
}