package wululu

import org.apache.spark.sql.{DataFrame}

import wululu.SparkSessionWrapper.spark
import wululu.DataConfigReader.Paths

object ETL {
    def main(args: Array[String]): Unit = {
        val tipsDF: DataFrame = readTipCSV()
        println(s"${tipsDF.show(10)}")
        println(s"Total rows: ${tipsDF.count()}")

        val checkinDF = readCheckinJSON()
        println(checkinDF.show(10, truncate = false))
        println(checkinDF.count())

        val businessDF = readBusinessJSON()
        println(businessDF.show(10))
        println(businessDF.count())

        spark.stop()
    }

    def readTipCSV(): DataFrame = {
        val csvReader = new CSVReader(Paths.CSV.tips)
        csvReader.readFile()
    }

    def readBusinessJSON(): DataFrame = {
        val jsonReader = new JSONReader(Paths.JSON.business)
        jsonReader.readNDJSONFile()
    }

    def readCheckinJSON(): DataFrame = {
        val jsonReader = new JSONReader(Paths.JSON.checkin)
        val df = jsonReader.readNDJSONFile()
        jsonReader.handleConcatenatedDates(df)
    }
}