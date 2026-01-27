package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame}

import wululu.SparkSessionWrapper.spark

object LoadUser {
    def main(args: Array[String]): Unit = {
        val config = ConfigFactory.load("pg_source.conf")
        val dbConfig = config.getConfig("pg_source")

        val df: DataFrame = spark.read
            .format("jdbc")
            .option("url", dbConfig.getString("url"))
            .option("user", dbConfig.getString("user"))
            .option("password", dbConfig.getString("password"))
            .option("dbtable", dbConfig.getString("user_table.table_name"))
            .option("driver", dbConfig.getString("driver"))
            .load()
    }
}
