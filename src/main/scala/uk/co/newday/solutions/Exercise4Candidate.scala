package uk.co.newday.solutions

import org.apache.spark.sql.{DataFrame, SaveMode}

object Exercise4Candidate {

  def execute(movies: DataFrame, ratings: DataFrame, movieRatings:DataFrame, ratingWithRankTop3:DataFrame) = {
    //write all the output in parquet format
    movies.coalesce(1).write.mode(SaveMode.Overwrite)
      .parquet("C:/Ashish/Mystuff/Data_files/parquet/movies")

    ratings.coalesce(1).write.mode(SaveMode.Overwrite)
      .parquet("C:/Ashish/Mystuff/Data_files/parquet/ratings")

    movieRatings.coalesce(1).write.mode(SaveMode.Overwrite)
      .parquet("C:/Ashish/Mystuff/Data_files/parquet/movie_ratings")

    ratingWithRankTop3.coalesce(1).write.mode(SaveMode.Overwrite)
      .parquet("C:/Ashish/Mystuff/Data_files/parquet/ratingWithRankTop3")
  }
}
