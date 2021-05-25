package uk.co.newday

import org.apache.spark.sql.SparkSession
import uk.co.newday.solutions.{Exercise1Candidate, Exercise2Candidate, Exercise3Candidate, Exercise4Candidate}

object MoviesRatingsCandidate {

  def main(args: Array[String]) {
    val sparkSession: SparkSession
    = SparkSession.builder().master("local").appName("MoviesRating").getOrCreate()

    val (movies, ratings) = Exercise1Candidate.execute(sparkSession)
    movies.cache()
    ratings.cache()
    val movieRatings = Exercise2Candidate.execute(movies, ratings)
    val ratingWithRankTop3 = Exercise3Candidate.execute(movies, ratings)
    Exercise4Candidate.execute(movies, ratings, movieRatings, ratingWithRankTop3)
    movies.unpersist()
    ratings.unpersist()
    sparkSession.stop()
  }
}
