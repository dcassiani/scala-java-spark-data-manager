package dml.config.logger

import com.typesafe.config.ConfigFactory
import dml.DataManagerLoadMain
import org.apache.log4j.{Level, Logger}

object LoggerConfig {

  private val configManager = ConfigFactory.load()
  private val loglevel = configManager.getString(s"dml.log.level")
  var log: Logger = Logger.getLogger(DataManagerLoadMain.getClass)

  def load(getClass: Class[_]): Logger = {
    log = Logger.getLogger(getClass)
    load()
  }

  def load(): Logger = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("hadoop").setLevel(Level.OFF)
    Logger.getLogger("hadoop.ParquetFileReader").setLevel(Level.OFF)
    Logger.getLogger("hadoop.InternalParquetRecordReader").setLevel(Level.OFF)
    Logger.getLogger("hadoop.ParquetOutputFormat").setLevel(Level.OFF)
    Logger.getLogger("hadoop.ParquetRecordReader").setLevel(Level.OFF)
    log.setLevel(decodeLoggerLevel(loglevel))
    log.info("[*] log.DEBUG: " + log.isDebugEnabled)
    log
  }

  private def decodeLoggerLevel(loglevel : String) : Level = {
    loglevel match {
      case "INFO" => Level.INFO
      case "DEBUG" => Level.DEBUG
      case "WARN" => Level.WARN
      case "ERROR" => Level.ERROR
      case _ => Level.INFO
    }
  }
}