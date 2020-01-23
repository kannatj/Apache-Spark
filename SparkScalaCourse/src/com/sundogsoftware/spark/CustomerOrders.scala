package com.sundogsoftware.spark


import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark._

object CustomerOrders {
  def main(args:Array[String]){
    
        Logger.getLogger("org").setLevel(Level.ERROR)
        
        val sc = new SparkContext("local[*]","CustomerOrders")
        
        val data = sc.textFile("C:/Users/KANNA/Documents/Spark2Scala/SparkScala/customer-orders.csv")
        
        val dataTuple = data.map(f =>{
          val fields = f.split(",")
          val custId = fields(0).toInt
          val price = fields(2).toFloat
          (custId,price)
        })
    
        val aggPrice = dataTuple.reduceByKey((x,y) => x+y)
        
        val modifiedTuple = aggPrice.map(f => (f._2,f._1))
        
        val sorted = modifiedTuple.sortByKey(false)
        
        val result = sorted.collect()
        
        for(res <- result){
          val custId = res._2
          val amoutSpend = res._1
          println(s"Customer $custId total amount spend is $amoutSpend")
        }
        
        
        
  }
}