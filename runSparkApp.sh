#!/bin/bash
clear

chmod -R 775 ../MovieAnalytics/
PROJECT_HOME=${PWD}
MainClass=com.movie.analytics.MovieAnalyticsApp
ArtifactJar=${PROJECT_HOME}"/target/scala-2.12/movieanalytics_2.12-0.1.jar"

#Build Artifacts for the MovieAnalytics
echo "Build & Package the MovieAnalytics App Artifacts"
sbt clean package
#sbt assembly
echo "Completed Packaging the MovieAnalytics App Artifacts to "${ArtifactJar}

#Input & Output Data Files Locations
base_input_path=${PROJECT_HOME}"/data/movie_analytics/input/"
base_output_path=${PROJECT_HOME}"/data/movie_analytics/output/top10movies/"
title_input_file_path=${base_input_path}"title.akas.tsv.gz"
title_basics_input_file_path=${base_input_path}"title.basics.tsv.gz"
title_ratings_input_file_path=${base_input_path}"title.ratings.tsv.gz"
principal_crew_input_file_path=${base_input_path}"title.principals.tsv.gz"
all_crew_details_input_file_path=${base_input_path}"name.basics.tsv.gz"
top10_movies_output_path=${base_output_path}"top10_movies.csv"
top10_movies_crew_output_path=${base_output_path}"top10_movies_crew.csv"
all_titles_for_top10_movies_output_path=${base_output_path}"all_titles_for_top10_movies.csv"

#Input Arguments
SPARK_MASTER_URL=spark://dilipts-mbp.lan:7077


SPARK_JOB_COMMAND="spark-submit --master ${SPARK_MASTER_URL}  --class ${MainClass} ${ArtifactJar} ${title_input_file_path} ${title_basics_input_file_path} ${title_ratings_input_file_path} ${principal_crew_input_file_path} ${all_crew_details_input_file_path} ${top10_movies_output_path} ${top10_movies_crew_output_path} ${all_titles_for_top10_movies_output_path}"
echo "******************************************* Running the MovieAnalytics Spark Application:" ${MainClass}
echo ${SPARK_JOB_COMMAND}
${SPARK_JOB_COMMAND}

#spark-submit --master local --class ${MainClass} ${ArtifactJar}
#spark-submit --master local --class guru.learningjournal.examples.SparkTestApp target/scala-2.11/spark-test-app-2.11-0.1.jar /home/prashant/spark-data/mental-health-in-tech-survey/json-data/surveys.json