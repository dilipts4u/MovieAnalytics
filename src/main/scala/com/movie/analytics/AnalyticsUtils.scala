package com.movie.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, col, desc, lit}


object AnalyticsUtils {

  //  Calculate Average for the Title Ratings Info
  def calculateAverageRating(title_rating_joined_df: DataFrame): DataFrame = {
    var averageNumberOfVotes = title_rating_joined_df.groupBy()
      .agg(avg(title_rating_joined_df("numVotes"))).first().get(0)
    println("averageNumberOfVotes:"+ averageNumberOfVotes)
    val title_ratings_avg_df = title_rating_joined_df.withColumn("averageNumberOfVotes", lit(averageNumberOfVotes))
    return title_ratings_avg_df
  }


  // Calculate Movie Ranking from the Title Average Ratings Info
  def calculateMovieRanking(title_ratings_avg_df: DataFrame): DataFrame = {
    title_ratings_avg_df.withColumn("ranking",
      ((col("numVotes") / col("averageNumberOfVotes"))
        * (col("averageRating"))))
      .select("titleId", "tconst", "title", "ordering",
        "ranking", "numVotes", "averageRating", "averageNumberOfVotes")
      .orderBy(desc("ranking")
      ).limit(10)
  }

  // For the Top 10 movies, list the persons who are most often credited
  def fetchMostCreditedCrew(top10_movies_crew_df: DataFrame): DataFrame = {
    top10_movies_crew_df.groupBy("primaryName").count()
      .select(col("primaryName").alias("distinctCrewName"),
        col("count").alias("crewNameCount"))
      .orderBy(desc("crewNameCount"))
  }


  // For the Top 10 movies, list the different titles of those 10 movies.
  def fetchAllTitlesForTop10Movies(top10_movies_rankings_df: DataFrame,
                                   tconst_joined_df: DataFrame): DataFrame = {
    val tconst_joined_df1 = tconst_joined_df.withColumnRenamed("tconst", "tconst1")
      .withColumnRenamed("titleId", "titleId1")
      .withColumnRenamed("title", "title1")
    top10_movies_rankings_df.drop("ordering")
          .join(tconst_joined_df1, top10_movies_rankings_df("titleId") ===  tconst_joined_df1("titleId1"), "outer")
          .select("titleId", "tconst", "title", "primaryTitle", "ranking", "ordering").na.drop()
          .orderBy(desc("ranking"), desc("ordering"))
  }


}
