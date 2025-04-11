package dml

import dml.config.logger.LoggerConfig
import dml.service._
import dml.util.DateUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object DataManagerLoadMain {
  var log: Logger = LoggerConfig.load(DataManagerLoadMain.getClass)

  def main(args: Array[String]): Unit = {
    log.debug("[*] DataManagerLoadMain INICIO")
    val ultimaAtualizacao = DateUtil.validateOrDefaultUpdateDate(args(0))

    log.info ("[*] ultimaAtualizacao = " + ultimaAtualizacao)
    println("[*] ultimaAtualizacao = " + ultimaAtualizacao)

    val sparkSession = SparkSession.builder.enableHiveSupport.getOrCreate

    val customersService = new CustomersService(sparkSession,
      sparkSession.sparkContext, ultimaAtualizacao)
    customersService.extractCustomer()

    log.debug("[*] DataManagerLoadMain finalizado")
  }
}
