package ch1

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvToDataFrame1 {
  def main(args: Array[String]): Unit = {
    val spark = createSparkSession()
    val filePath = "src/main/scala/ch1/data/books.csv"

    try {
      val df = readCsvFile(spark, filePath)
      df.show(5)
    } catch {
      case e: Exception => println(s"Error reading file $filePath: ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("CSV to Dataset")
      .master("local")
      .getOrCreate()
  }

  def readCsvFile(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(path)
  }
}
