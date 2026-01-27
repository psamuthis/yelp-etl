package wululu

import org.apache.spark.sql.{DataFrame}

import wululu.DataConfigReader.Paths

object LoadBusiness {
    def main(args: Array[String]): Unit = {
        val reader = new JSONReader(Paths.JSON.business)
        val df = reader.readNDJSONFile()

        println(df.show(10))
        println(df.count())
    }
}