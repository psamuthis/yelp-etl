package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

import wululu.SparkSessionWrapper.spark

object TransformPerformanceFact {
    def main(args: Array[String]): Unit = {
        val businessDF:DataFrame = StorageManager.load("business")
        val calendarDF:DataFrame = StorageManager.load("calendar")
        val locationDF:DataFrame = StorageManager.load("location")

        val df:DataFrame = createPerformanceFactDF(businessDF,locationDF,calendarDF)
        df.columns.foreach(println)
    }

    def createPerformanceFactDF(businessDF: DataFrame, locationDF: DataFrame, calendarDF: DataFrame): DataFrame = {

        // Helper to transform date format only one time
        def withTimeId(df: DataFrame) = df.withColumn("ID_Temps", date_format(col("date"), "yyyyMM").cast("int"))

        val reviewAgg = withTimeId(LoadReview.getDataFrame().select("business_id", "date", "review_id", "stars"))
            .groupBy("business_id", "ID_Temps")
            .agg(
                count("review_id").as("review_count"),
                avg("stars").as("score_mean")          
            )

        val tipAgg = withTimeId(LoadTips.getDataFrame().select("business_id", "date"))
            .groupBy("business_id", "ID_Temps")
            .agg(count("*").as("tip_count"))

        val checkinAgg = withTimeId(LoadCheckin.getDataFrame().select("business_id", "date"))
            .groupBy("business_id", "ID_Temps")
            .agg(count("*").as("checkin_count"))

        val factsJoined = reviewAgg
            .join(broadcast(tipAgg), Seq("business_id", "ID_Temps"), "full")
            .join(broadcast(checkinAgg), Seq("business_id", "ID_Temps"), "full")

        factsJoined
            .join(broadcast(locationDF), "business_id") 
            .join(broadcast(calendarDF), "ID_Temps") 
            .select(
                col("business_id"),
                col("ID_Temps").as("time_id"), 
                col("location_id"),   
                coalesce(col("review_count"), lit(0)).as("review_count"),
                coalesce(col("tip_count"), lit(0)).as("tip_count"),
                coalesce(col("checkin_count"), lit(0)).as("checkin_count"),
                col("score_mean")
            )
    }
}

