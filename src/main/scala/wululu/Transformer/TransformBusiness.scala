package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame}

import wululu.SparkSessionWrapper.spark

object TransformBusiness {
    def main(args: Array[String]): Unit = {
        val df: DataFrame = LoadBusiness.getDataFrame()
        df.columns.foreach(println)
    }
}