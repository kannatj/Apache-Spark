package com.sundogsoftware.spark



import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
object FriendsByName {
  def main(args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "Friends")
        
    val lines = sc.textFile("C:/Users/KANNA/Documents/Spark2Scala/SparkScala/fakefriends.csv")

    
    
    val rdd = lines.map(lines =>{
      val fields = lines.split(",")  
      val age = fields(2).toInt
      val numFriends = fields(3).toInt
      val name = fields(1).toString()
      (name,numFriends)
    })
    
//    rdd.take(10).foreach(println)
//
//    val mapval = rdd.mapValues(lines => (lines,1))
//    
//    mapval.take(10).foreach(println)
//    
//    val totalAge = mapval.reduceByKey((x,y) => (x._1+y._1,x._2+y._2))
//    
//    println("Total age")
//    
//    totalAge.take(10).foreach(println)
//    
//    val average = totalAge.mapValues(lines => (lines._1/lines._2))
//    
//    println("Result")
//    average.take(10).foreach(println)
    
    
    rdd.take(10).foreach(println)
    
    val result = rdd.mapValues(line => (line,1)).reduceByKey((x,y) =>(x._1+y._1,x._2+y._2)).mapValues(line => (line._1/line._2))
    
    println("Result:")
    
    val final_result = result.sortByKey()
    
    final_result.take(10).foreach(println)
    
    
  }
}