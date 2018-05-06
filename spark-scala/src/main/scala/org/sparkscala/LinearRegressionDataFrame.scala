package org.sparkscala

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics



object LinearRegressionDataFrame {
 
  def main(args: Array[String]) {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
                  .builder
                  .appName("LinearRegressionDataFrame")
                  .master("local[*]")
                  .getOrCreate()
                  
    import spark.implicits._
    
    val inputLines = spark.sparkContext.textFile("data/regression.txt")
    
    val colNames = Seq("label", "features")
    val data = inputLines
                  .map(_.split(","))
                  .map(x => (x(0).toDouble, Vectors.dense(x(1).toDouble)))
    
                  
     val dataFrame = data.toDF(colNames: _*)
     
     val Array(train, test) = dataFrame.randomSplit(Array(0.8, 0.2))
     
           
     val regression = new LinearRegression()
                      .setMaxIter(100)
                      .setElasticNetParam(0.8)
                      .setRegParam(0.01)
                      
     val lrModel = regression.fit(train)
     
     val modelSummary = lrModel.summary
     
     println(s"RMSE: ${modelSummary.rootMeanSquaredError}, R2: ${modelSummary.r2}\n")
     
     val results = lrModel.transform(test).cache()
     results.show()
     
     val rdd = results.rdd.map(r => (r.getAs[Double]("prediction"), r.getAs[Double]("label")))
     
     val regMetrics = new RegressionMetrics(rdd)    
     println(s"RMSE on test data is ${regMetrics.rootMeanSquaredError}, r2 is ${regMetrics.r2}\n")
                      
                      
                      
                      
  }
}