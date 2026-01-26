package wululu

import org.apache.spark.sql.{SparkSession, DataFrame}

object SparkSessionWrapper {
  lazy val spark: SparkSession = SparkSession.builder()
    .appName("ETLApplication")
    .master("local[*]")
    .getOrCreate()
  
  spark.sparkContext.setLogLevel("ERROR")
}