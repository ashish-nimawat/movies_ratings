package uk.co.newday.solutions

import org.apache.spark.sql.{DataFrame, functions => F}

object Exercise2Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    //With two dataframe apply the join as specified in the exercise.
    val movieRatingDF = movies.join(ratings, movies("movie_id") === ratings("movie_id"), "inner")
      .select(
        movies("movie_id"),
        movies("title"),
        movies("genres"),
        ratings("rating")
      )
      .groupBy("movie_id", "title", "genres")
      .agg(
        F.max("rating").alias("max_rating"),
        F.min("rating").alias("min_rating"),
        F.avg("rating").alias("avg_rating")
      )
    println("------------------------ Movie Ratings ---------------------------------")
    movieRatingDF
  }
}
