package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

import wululu.SparkSessionWrapper.spark

object TransformCalendar {
    def main(args: Array[String]): Unit = {
        val df: DataFrame = LoadCheckin.getDataFrame()
        df.columns.foreach(println)

        val calendarDF: DataFrame = createMonthlyTimeDimension(df,"date")
        calendarDF.groupBy("year").count().orderBy("year").show()
        
    }

    def createMonthlyTimeDimension(df: DataFrame, dateCol: String): DataFrame = {
        df.select(col(dateCol))
            .withColumn("date", to_date(col(dateCol)))
            .withColumn("year", year(col("date")))
            .withColumn("month", month(col("date")))
            .withColumn("quarter", quarter(col("date")))
            // ID : 202401 pour Janvier 2024
            .withColumn("ID_Temps", (col("year") * 100) + col("month")) 
            .select("ID_Temps", "year", "month", "quarter")
            .distinct()
            .orderBy(asc("date"))
    }

}