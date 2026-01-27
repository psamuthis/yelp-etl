package wululu

import org.apache.spark.sql.{DataFrame}

import wululu.DataConfigReader.Paths

object LoadCheckin {
    def main(args: Array[String]): Unit = {
        val reader = new JSONReader(Paths.JSON.checkin)
        var df = reader.readNDJSONFile()
        df = reader.handleConcatenatedDates(df)
    }
}