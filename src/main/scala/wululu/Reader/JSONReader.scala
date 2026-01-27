package wululu

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

import wululu.SparkSessionWrapper.spark
import wululu.DataConfigReader.Paths

class JSONReader(val filePath: String) {
    def readNDJSONFile(): DataFrame = {
        spark.read
            .option("multiline", "false")
            .json(filePath)
    }

    def handleConcatenatedDates(df: DataFrame): DataFrame = {
        df
            .withColumn("date_array", split(col("date"), ", "))
            .withColumn("single_date", explode(col("date_array")))
            .select(
                col("business_id"),
                trim(col("single_date")).as("date")
            )
    }
}