package wululu

import SparkSessionWrapper.spark
import org.apache.spark.sql.{DataFrame}

object ETL {
    def main(args: Array[String]): Unit = {
        //val df: DataFrame = readTipCSV()
        //println(s"${df.show(10)}")
        //println(s"Total rows: ${df.count()}")

        val df = readCheckinJSON()
        println(df.show(10, truncate = false))
        println(df.count())

        //val df = readBusinessJSON()
        //println(df.show(10))
        //println(df.count())

        spark.stop()
    }

    def readTipCSV(): DataFrame = {
        val csvReader = new CSVReader("/home/psamu/Documents/M2/S2/informatique-decisionnelle/data-integration/data/yelp_academic_dataset_tip.csv")
        csvReader.readFile()
    }

    def readBusinessJSON(): DataFrame = {
        val jsonReader = new JSONReader("/home/psamu/Documents/M2/S2/informatique-decisionnelle/data-integration/data/yelp_academic_dataset_business.json")
        jsonReader.readNDJSONFile()
    }

    def readCheckinJSON(): DataFrame = {
        val jsonReader = new JSONReader("/home/psamu/Documents/M2/S2/informatique-decisionnelle/data-integration/data/yelp_academic_dataset_checkin.json")
        val df = jsonReader.readNDJSONFile()
        jsonReader.handleConcatenatedDates(df)
    }
}