package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.math.min




object test {
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]", "MinTemperatures")
    
    val data = sc.textFile("C:/Users/KANNA/Documents/Spark2Scala/SparkScala/1800.csv")
    
    val datasplit = data.map(lines => lines.split(",")).filter(x => x(2) == "TMIN")
    
    val tuple = datasplit.map(lines => (lines(0),lines(3).toInt))
    
    val result = tuple.reduceByKey((x,y) => min(x,y)).collect()

    for(r <- result){
      val station = r._1
      val temp = r._2
      println(s"Minimum temperature for $station is $temp")
    }
  }
}