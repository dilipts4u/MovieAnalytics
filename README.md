#MovieAnalytics

## Summary
* This is Movie Analytics Project developed using the Scala.
* It demonstrates the use of Scala FunSuite to unit test Spark methods.
* In this case SparkSession is being injected to the test cases.

## Pre-Requisities:

A) The Spark Cluster is up and Running.

B) Obtain the SPARK_MASTER_URL for the Spark Cluster.

## How to clone this Repo to your local folder.
* Open a Terminal/Command Prompt at which you want to clone the repository.

        git clone https://github.com/dilipts4u/MovieAnalytics.git

## How to gather/download Raw Input Data For the Movie Analytics
* Goto the tractable-analytics Project Root Folder.

        cd MovieAnalytics/

* Create Input Director under the project root.

        mkdir -p data/movie_analytics/input/
        mkdir -p data/movie_analytics/output/

## Input Data Source

    The dataset is located at tractable-analytics/data/movie_analytics/input/

#Build Artifacts for the MovieAnalytics
    sbt clean package

##Submit Spark Job
    "spark-submit --master ${SPARK_MASTER_URL}  \
      --class ${MainClass} \
      ${ArtifactJar} ${title_input_file_path} \
      ${title_basics_input_file_path} ${title_ratings_input_file_path} \
      ${principal_crew_input_file_path} ${all_crew_details_input_file_path} \
      ${top10_movies_output_path} ${top10_movies_crew_output_path} \
      ${all_titles_for_top10_movies_output_path}"
