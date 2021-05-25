package uk.co.newday.solutions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

import scala.collection.mutable.ListBuffer

object Exercise1Candidate {

  private case class Movie(movieId: Int, title: String, genre: String)

  private case class Rating(userId: Int, movieId: Int, rating: Int, timestamp: Int)

  def execute(sparkSession: SparkSession): (DataFrame, DataFrame) = {
    //Please load movies and ratings csv's in output dataframes.
    println("-------- Starting data loading for .dat files -------------------")

    //-------------------------------- Movies --------------------------------
    // Dataframe structure of Movies
    val movieStruct = new StructType()
      .add(StructField("movie_id", IntegerType, true))
      .add(StructField("title", StringType, true))
      .add(StructField("genres", StringType, true))

    // Creating MovieRDD by loading movie.dat file
    val moviesRDD: RDD[Row] = sparkSession.read.format("text")
      .load("C://Ashish/MyStuff/Data_files/movies.dat").rdd

    // Splitting fields of lines to get row RDD for Movies
    val mappedMovieRDD = moviesRDD.map { line =>
      //      println("Line: "+ line)
      val splitLines = line.toString()
        .replace("[", "")
        .replace("]", "")
        .split("::")
      var lineBuffer = new ListBuffer[Any]()
      var index = 0
      while (index < splitLines.length) {
        try {
          lineBuffer.append(splitLines(index).toInt)
        } catch {
          case ex: NumberFormatException => lineBuffer.append(splitLines(index))
          case ex: Exception => println("Exception occurred: "+ex)
        }
        index += 1
      }
      Row.fromSeq(lineBuffer.toList)
    }
    //Creating Move Dataframe from mapped movieRDD
    val moviesDF = sparkSession.createDataFrame(mappedMovieRDD, movieStruct)

    //-------------------------------- Ratings --------------------------------
    //DataFrame structure of Ratings
    val ratingStruct = new StructType()
      .add(StructField("user_id", IntegerType, true))
      .add(StructField("movie_id", IntegerType, true))
      .add(StructField("rating", IntegerType, true))
      .add(StructField("timestamp", IntegerType, true))

    //Creating RatingRDD by loading rating.dat file
    val ratingsRDD: RDD[Row] = sparkSession.read.format("text")
      .load("C://Ashish/MyStuff/Data_files/ratings.dat").rdd

    // Splitting fields of lines to get row RDD for Ratings
    val mappedRatingRDD = ratingsRDD.map { line =>
      //      println("Line: "+ line)
      val splitLines = line.toString()
        .replace("[", "")
        .replace("]", "")
        .split("::")
      var lineBuffer = new ListBuffer[Any]()
      var index = 0
      while (index < splitLines.length) {
        try {
          lineBuffer.append(splitLines(index).toInt)
        } catch {
          case ex: NumberFormatException => lineBuffer.append(splitLines(index))
          case ex: Exception => println("Exception occurred: " + ex)
        }
        index += 1
      }
      Row.fromSeq(lineBuffer.toList)
    }
    //Creating Rating DataFrame from mapped rating RDD
    val ratingDF = sparkSession.createDataFrame(mappedRatingRDD, ratingStruct)

    println("-------------- DataFrames created -------------")
    (moviesDF, ratingDF)
  }
}
