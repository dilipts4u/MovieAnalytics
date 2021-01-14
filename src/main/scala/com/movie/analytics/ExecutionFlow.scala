package com.movie.analytics

import com.movie.analytics.AnalyticsUtils.{calculateAverageRating, calculateMovieRanking,
  fetchAllTitlesForTop10Movies, fetchMostCreditedCrew}
import com.movie.analytics.FileReadWriteUtils.writeDataframeAsCSV
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ExecutionFlow {

  def execute(spark: SparkSession,
              title_input_file_path: String,
              title_basics_input_file_path: String,
              title_ratings_input_file_path: String,
              principal_crew_input_file_path: String,
              all_crew_details_input_file_path: String,
              top10_movies_output_path: String,
              top10_movies_crew_output_path: String,
              all_titles_for_top10_movies_output_path: String): Unit = {

    // Fetch Title  Dataset
    val title_df = FileReadWriteUtils.readCsvFile(spark = spark, locationPath = title_input_file_path)
      .select("titleId", "title", "ordering")
    // title_df.printSchema()
    // title_df.show(false)

    // Fetch Title Basics Dataset
    val title_basics_df = FileReadWriteUtils.readCsvFile(spark = spark, locationPath = title_basics_input_file_path)
      .select("tconst", "titleType", "primaryTitle", "genres")
    // title_basics_df.printSchema()
    // title_basics_df.show(false)

    // Fetch Title Ratings Dataset
    val title_ratings_df = FileReadWriteUtils.readCsvFile(spark = spark, locationPath = title_ratings_input_file_path)
    // title_ratings_df.printSchema()
    // title_ratings_df.show(false)

    // Fetch Title Principal Crew Dataset
    val principal_crew_df = FileReadWriteUtils.readCsvFile(spark = spark, locationPath = principal_crew_input_file_path)
    // principal_crew_df.printSchema()
    // principal_crew_df.show(false)

    // Fetch All Crew Dataset
    val all_crew_details_df = FileReadWriteUtils.readCsvFile(spark = spark, locationPath = all_crew_details_input_file_path)
    // all_crew_details_df.printSchema()
    // all_crew_details_df.show(false)

    // Join Title data with TitleBasics data
    val tconst_joined_df = title_df.join(title_basics_df, title_df("titleId") === title_basics_df("tconst"), "inner")
    // tconst_joined_df.show(false)

    // Fetch Title Rating dataset for numVotes >= 500
    val filtered_title_ratings_df = title_ratings_df.filter(title_ratings_df("numVotes") >= 500)
    // filtered_title_ratings_df.show(false)

    // Join Title Details with Title Ratings data
    val tconst_joined_df1 = tconst_joined_df.withColumnRenamed("tconst", "tconst1")
    val title_rating_joined_df = tconst_joined_df1.join(filtered_title_ratings_df,
      tconst_joined_df1("tconst1") === filtered_title_ratings_df("tconst"), "inner")
      .filter(tconst_joined_df("ordering") === 1)
      .select("titleId", "tconst", "title", "ordering", "numVotes", "averageRating")
      .orderBy(asc("tconst"), asc("ordering"))
    // title_rating_joined_df.show(false)

    //  Calculate Average for the Title Ratings Info
    val title_ratings_avg_df = calculateAverageRating(title_rating_joined_df)
    //title_ratings_avg_df.show(false)

    // Section 1
    // Calculate Movie Ranking
    val top10_movies_rankings_df = calculateMovieRanking(title_ratings_avg_df)
    top10_movies_rankings_df.show()

    // Section 2
    // Join Principal Crew  with all the crew details data
    // principal_crew_df.show(false)
    // all_crew_details_df.show(false)
    val principal_df = principal_crew_df.withColumnRenamed("nconst", "nconst1")
    val principal_crew_full_details = principal_df.join(all_crew_details_df,
      principal_df("nconst1") === all_crew_details_df("nconst"), "left")
      .select("tconst", "nconst", "primaryName", "category")
    //principal_crew_full_details.show()

    // Join top10_movies_rankings_df  with principal_crew_full_details data
    val top10_movies_rankings_df1 = top10_movies_rankings_df.withColumnRenamed("tconst", "tconst1")
    val top10_movies_crew_df = top10_movies_rankings_df1.join(principal_crew_full_details,
      top10_movies_rankings_df1("tconst1") === principal_crew_full_details("tconst"), "left").drop("tconst1")
    //top10_movies_crew_df.show()
    // top10_movies_crew_df.count()

    //For the Top 10 movies, list the persons who are most often credited
    val top10_movies_distinct_crew_count_df = fetchMostCreditedCrew(top10_movies_crew_df)
    top10_movies_distinct_crew_count_df.show()

    //#  For the Top 10 movies, list the different titles of those 10 movies.
    val all_titles_for_top10_movies_df = fetchAllTitlesForTop10Movies(top10_movies_rankings_df, tconst_joined_df)
    all_titles_for_top10_movies_df.show()
    all_titles_for_top10_movies_df.count()

    // Write Output Data to Destination
    writeDataframeAsCSV(top10_movies_rankings_df, top10_movies_output_path)
    writeDataframeAsCSV(top10_movies_distinct_crew_count_df, top10_movies_crew_output_path)
    writeDataframeAsCSV(all_titles_for_top10_movies_df, all_titles_for_top10_movies_output_path)

  }

}
