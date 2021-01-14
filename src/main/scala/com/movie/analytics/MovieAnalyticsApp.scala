package com.movie.analytics

import org.apache.spark.sql.AnalysisException
import java.io.FileNotFoundException
import scala.util.Failure

object MovieAnalyticsApp extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    try {
      println("Started MovieAnalytics Spark App")
      var title_input_file_path = args(0)
      var title_basics_input_file_path = args(1)
      var title_ratings_input_file_path = args(2)
      var principal_crew_input_file_path = args(3)
      var all_crew_details_input_file_path = args(4)
      var top10_movies_output_path = args(5)
      var top10_movies_crew_output_path = args(6)
      var all_titles_for_top10_movies_output_path = args(7)

      //Start the Spark Job Execution
      ExecutionFlow.execute(spark = spark,
        title_input_file_path = title_input_file_path,
        title_basics_input_file_path = title_basics_input_file_path,
        title_ratings_input_file_path = title_ratings_input_file_path,
        principal_crew_input_file_path = principal_crew_input_file_path,
        all_crew_details_input_file_path = all_crew_details_input_file_path,
        top10_movies_output_path = top10_movies_output_path,
        top10_movies_crew_output_path = top10_movies_crew_output_path,
        all_titles_for_top10_movies_output_path = all_titles_for_top10_movies_output_path)

      println("Completed MovieAnalytics Spark App")

    } catch {
      case ex: FileNotFoundException => {
        println(s"File not found $ex.getStackTrace())")
        Failure(ex)
      }
      case ex: AnalysisException => {
        println(s"Analysis Exception $ex")
        println(s"$ex.getStackTrace()")
        Failure(ex)
      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown ")
        println(s"$unknown.getStackTrace()")
        Failure(unknown)
      }
    } finally {
      // close the spark session.
      spark.close()
    }
  }


}
