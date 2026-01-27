package wululu

import org.apache.spark.sql.{DataFrame}

import wululu.DataConfigReader.Paths

object LoadBusiness {
    def getDataFrame(): DataFrame = {
        val reader = new JSONReader(Paths.JSON.business)
        reader.readNDJSONFile()
    }
}