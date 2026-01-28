package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame}

import wululu.SparkSessionWrapper.spark

object LoadElite {
    def getDataFrame(): DataFrame = {
        val config = ConfigFactory.load("pg_source.conf")
        val dbConfig = config.getConfig("pg_source")

        spark.read
            .format("jdbc")
            .option("url", dbConfig.getString("url"))
            .option("user", dbConfig.getString("user"))
            .option("password", dbConfig.getString("password"))
            .option("dbtable", dbConfig.getString("elite_table.table_name"))
            .option("driver", dbConfig.getString("driver"))
            .load()
    }
}