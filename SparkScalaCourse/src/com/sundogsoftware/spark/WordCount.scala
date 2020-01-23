package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._


object WordCount {
  def main(args: Array[String]){
    
        Logger.getLogger("org").setLevel(Level.ERROR)
        
        val sc = new SparkContext("local[*]","Word count")
        
        val file = sc.textFile("C:/Users/KANNA/Documents/Spark2Scala/SparkScala/book.txt")
        
        val words = file.flatMap(lines => lines.split(" "))
        
        val wordsBetter = file.flatMap(lines => lines.split("\\W+"))
        
        val result = words.countByValue()
        
        //val resultBetter = wordsBetter.countByValue()
        
        println("WordCount result:")
        result.take(50).foreach(println)
        
        val resultBetter = wordsBetter.map(f => (f.toLowerCase(),1)).reduceByKey((x,y) => (x+y)).map(f => (f._2,f._1)).sortByKey(false)
        
        println("WordCount result in a better way")
        resultBetter.take(50).foreach(println)    

    
  }
}