package wululu

import SparkSessionWrapper.spark
import org.apache.spark.sql.{SparkSession, DataFrame}

class CSVReader(val filePath: String) {
    def readFile(): DataFrame = {
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("delimiter", ",")
            .option("quote", "\"")
            .option("escape", "\\")
            .option("mode", "DROPMALFORMED")   // or PERMISSIVE, FAILFAST
            .csv(filePath)
    }

}