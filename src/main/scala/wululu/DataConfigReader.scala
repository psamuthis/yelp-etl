package wululu

import com.typesafe.config.{Config, ConfigFactory}

object DataConfigReader {
  private val config: Config = ConfigFactory.load("data.conf")
  
  object Paths {
    val dataDir: String = config.getString("paths.data_dir")
    
    object CSV {
      val tips: String = config.getString("paths.csv.tips")
    }
    
    object JSON {
      val business: String = config.getString("paths.json.business")
      val checkin: String = config.getString("paths.json.checkin")
    }
  }
}