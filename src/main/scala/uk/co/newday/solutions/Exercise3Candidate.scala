package uk.co.newday.solutions

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions => F}

object Exercise3Candidate {

  def execute(movies: DataFrame, ratings: DataFrame): (DataFrame) = {
    // Complete the exercise and show the top 3 movies per user.
    val top3MoviesDF = movies.join(ratings, movies("movie_id") === ratings("movie_id"), "inner")
      .select(
        ratings("user_id"),
        movies("title"),
        ratings("rating")
      ).withColumn(
      "row_number",
      F.row_number() over(
        Window.partitionBy("user_id")
          .orderBy(F.col("rating").desc)
        )
    ).select(
      F.col("user_id"),
      F.col("title"),
      F.col("rating")
    ).where(F.col("row_number").leq(3))
    top3MoviesDF
  }
}
