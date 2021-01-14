package com.movie.analytics

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object FileReadWriteUtils {

  // Spark Utility Function to Load the input data from the source
  def readCsvFile(spark: SparkSession,
                  locationPath: String): DataFrame = {
    spark.read.option("header", "true")
      .option("inferSchema", "true")
      .option("sep", "\t")
      .csv(locationPath)
  }

  // Spark Utility Function to write dataframe to CSV File
  def writeDataframeAsCSV(df: DataFrame,
                          locationPath: String
                         ): Unit = {
    val oDate = java.time.LocalDate.now.toString
    // java.time.Instant.now().toString
    val delimiter = ","
    val header = "true"
    df.repartition(1).write
      .option("delimiter", delimiter)
      .option("header", header)
      .mode(SaveMode.Overwrite)
      .format("csv")
      .save(locationPath + "/" + oDate + "/")
  }

  // function to get parquet dataset from s3
  def readParquetFile(spark: SparkSession,
                      locationPath: String): DataFrame = {
    spark.read.parquet(locationPath)
  }

}
