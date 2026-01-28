package wululu

import org.apache.spark.sql.{DataFrame}
import wululu.DataConfigReader.Paths

object LoadTips {
    def getDataFrame(): DataFrame = {
        val csvReader = new CSVReader(Paths.CSV.tips)
        csvReader.readFile()
    }
}