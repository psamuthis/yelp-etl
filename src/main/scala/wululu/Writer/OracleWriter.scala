package wululu

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.jdbc.JdbcDialects

import wululu.SparkSessionWrapper.spark

object OracleWriter {

    private val config    = ConfigFactory.load("oracle_sink.conf")
    private val dbConfig  = config.getConfig("oracle_sink")
    private val jdbcUrl   = dbConfig.getString("url")

    private val connectionProperties = {
        val props = new java.util.Properties()
        props.setProperty("user",     dbConfig.getString("user"))
        props.setProperty("password", dbConfig.getString("password"))
        props.setProperty("driver",   dbConfig.getString("driver"))
        props.setProperty("batchsize", dbConfig.getString("batch_size"))
        props
    }

    def main(args: Array[String]): Unit = {
        JdbcDialects.registerDialect(new OracleDialect())

        write(
            prepareBusiness(StorageManager.load("business")),
            dbConfig.getString("tables.business")
        )
        write(StorageManager.load("location"),            dbConfig.getString("tables.location"))
        write(StorageManager.load("category"),            dbConfig.getString("tables.category"))
        write(StorageManager.load("businessCategoryLink"),dbConfig.getString("tables.business_category"))
        write(StorageManager.load("calendar"),            dbConfig.getString("tables.calendar"))
        write(StorageManager.load("performanceFact"),     dbConfig.getString("tables.performance_fact"))

        println("Chargement Oracle terminé.")
    }

    // attributes est un struct complexe, on le sérialise en JSON string pour Oracle
    private def prepareBusiness(df: DataFrame): DataFrame = {
        df.withColumn("attributes", to_json(col("attributes")))
    }

    // Toutes les colonnes en majuscules pour compatibilité Oracle 
    private def upperColumns(df: DataFrame): DataFrame = {
        df.columns.foldLeft(df)((acc, c) => acc.withColumnRenamed(c, c.toUpperCase))
    }

    private def write(df: DataFrame, tableName: String): Unit = {
        println(s"Écriture de $tableName...")
        upperColumns(df).write
            .mode(SaveMode.Overwrite)
            .option("truncate", "true")
            .jdbc(jdbcUrl, tableName, connectionProperties)
        println(s"  => $tableName OK")
    }
}
