package wululu

import org.apache.spark.sql.{DataFrame}
import wululu.CSVReader
import wululu.DataConfigReader.Paths

object LoadTips {
    def main(args: Array[String]): Unit = {
        val csvReader = new CSVReader(Paths.CSV.tips)
        val tipsDF = csvReader.readFile()

        println(tipsDF.show(10))
        println(tipsDF.count())
    }
}