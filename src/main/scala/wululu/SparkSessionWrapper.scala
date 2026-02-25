package wululu

import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkSessionWrapper {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("ETLApplication")
    .master("local[*]")
    .config("spark.driver.memory", "4g") // Donne 4Go au driver
    .config("spark.executor.memory", "4g")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
}