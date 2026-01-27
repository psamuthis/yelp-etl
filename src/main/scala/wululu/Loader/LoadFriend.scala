package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame}

import wululu.SparkSessionWrapper.spark

object LoadFriend {
    def main(args: Array[String]): Unit = {
        val config = ConfigFactory.load("pg_source.conf")
        val dbConfig = config.getConfig("pg_source")

        val df: DataFrame = spark.read
            .format("jdbc")
            .option("url", dbConfig.getString("url"))
            .option("user", dbConfig.getString("user"))
            .option("password", dbConfig.getString("password"))
            .option("driver", dbConfig.getString("driver"))
            .option("dbtable", dbConfig.getString("friend_table.table_name"))
            .option("partitionColumn", dbConfig.getString("friend_table.partition-column"))
            .option("numPartitions", dbConfig.getString("friend_table.partitions"))
            .option("lowerBound", "0")
            .option("upperBound", dbConfig.getString("friend_table.partitions"))
            .load()
    }
}