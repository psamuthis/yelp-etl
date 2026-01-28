package wululu

import org.apache.spark.sql.{DataFrame}

import wululu.DataConfigReader.Paths

object LoadCheckin {
    def getDataFrame(): DataFrame = {
        val reader = new JSONReader(Paths.JSON.checkin)
        var df = reader.readNDJSONFile()
        reader.handleConcatenatedDates(df)
    }
}