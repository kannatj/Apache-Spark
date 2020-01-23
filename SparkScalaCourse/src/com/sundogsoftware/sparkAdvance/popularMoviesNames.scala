package com.sundogsoftware.sparkAdvance

import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.io.Source

import java.nio.charset.CodingErrorAction
import scala.io.Codec

object popularMoviesNames {
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Movie Names")
        
    val file = "C:/Users/KANNA/Documents/Spark2Scala/ml-100k/u.item"
    
    var result:Map[Int,String] = Map()
        result = loadMovies(file)
    
        println(s"${result(1631)}")
  }
        
        
    def loadMovies(fileName: String):Map[Int,String] ={
    
      implicit val codec = Codec("UTF-8")
      codec.onMalformedInput(CodingErrorAction.REPLACE)
      codec.onUnmappableCharacter(CodingErrorAction.REPLACE)  
      
      var MovieNames:Map[Int,String] = Map()
      
      val file = Source.fromFile(fileName).getLines()
      for(line <- file){
        val fields = line.split("|")
        if(fields.length > 1){
          val movieId = fields(0).toInt
          val movieName = fields(1)
          MovieNames += (movieId -> movieName)
         }
       }
    
      return MovieNames
    }
    

}