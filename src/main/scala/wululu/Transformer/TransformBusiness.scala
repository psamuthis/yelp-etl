package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._

import wululu.SparkSessionWrapper.spark

object TransformBusiness {
    def main(args: Array[String]): Unit = {
        val df: DataFrame = LoadBusiness.getDataFrame().limit(1000) //limit(1000) for testing first
        df.columns.foreach(println)

        val locationDF: DataFrame = createLocationDataFrame(df)
        StorageManager.save(locationDF,"location")
        val businessDF: DataFrame = createBusinessDataFrame(df)
        StorageManager.save(businessDF,"business")
        val categoryDF: DataFrame = createCategoryDataFrame(df)
        StorageManager.save(categoryDF,"category")
        val businessCategoryLink: DataFrame = createBusinessCategoryBridgeDF(df, categoryDF)
        StorageManager.save(businessCategoryLink,"businessCategoryLink")

    }

    
    def createBusinessCategoryBridgeDF(businessDF: DataFrame, categoriesDF: DataFrame): DataFrame = {
        businessDF
            .select(
                col("business_id"),
                explode(split(col("categories"), ",\\s*")).as("business_category")
            )
            .join(broadcast(categoriesDF), col("business_category") === col("category_name"))
            .select("business_id", "category_id")
    }
    
    def createCategoryDataFrame(df: DataFrame): DataFrame = {
        df
            .withColumn("category_array", split(col("categories"), ", "))
            .withColumn("category_name", explode(col("category_array")))
            .withColumn("category_id", monotonically_increasing_id())
            .select(
                col("category_id"),
                col("category_name"),
            )
    }

    def createBusinessDataFrame(df: DataFrame): DataFrame = {
        //TODO: attributes require finer processing
        df.select(
            "name",
            "stars",
            "is_open",
            "attributes",
        )
    }

    def createLocationDataFrame(df: DataFrame): DataFrame = {
        df.select(
            "business_id", 
            "address",
            "city",
            "postal_code",
            "state",
            "latitude",
            "longitude",
        )
        .withColumn("location_id", monotonically_increasing_id())
    }
}