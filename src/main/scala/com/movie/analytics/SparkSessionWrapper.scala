package com.movie.analytics

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val master = "local"
  val myAppName = "MovieAnalyticsApp"

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master(master)
      .appName(myAppName)
      .getOrCreate()
  }

}
