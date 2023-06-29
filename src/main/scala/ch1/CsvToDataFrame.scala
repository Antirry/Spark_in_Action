package ch1

import org.apache.spark.sql.{DataFrame, SparkSession}


object CsvToDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CSV to Dataset")
      .master("local")
      .getOrCreate()

    try {
      val df = readCsvFile(spark, "src/main/scala/ch1/data/books.csv")
      df.show(5)
    } finally {
      spark.stop()
    }
  }

  def readCsvFile(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .csv(path)
  }
}

/*
    В этом коде используется Using.resource,
     чтобы автоматически закрыть SparkSession после использования,
      даже если произойдет исключение.

    Это помогает предотвратить утечку ресурсов.
     Кроме того, имена переменных были изменены на более информативные.
 */
//    Using.resource(sparkSessionBuilder.getOrCreate()) {
//      sparkSession =>
//      val booksDataFrame = sparkSession.read
//        .option("header", "true")
//        .csv("src/main/scala/ch1/data/books.csv")
//
//      booksDataFrame.show(5)
