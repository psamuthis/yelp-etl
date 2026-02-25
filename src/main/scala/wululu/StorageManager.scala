package wululu

import org.apache.spark.sql.{SparkSession, DataFrame}
import wululu.SparkSessionWrapper.spark

object StorageManager {
  def save(df: DataFrame, name: String): Unit = {
    df.write.mode("overwrite").parquet(s"data/parquet/$name.parquet")
  }
  
  def load(name: String): DataFrame = {
    spark.read.parquet(s"data/parquet/$name.parquet")
  }
}