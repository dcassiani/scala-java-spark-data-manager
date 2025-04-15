package dmt

import dmt.config.logger.LoggerTrait
import dmt.service.PedidoService
import dmt.util.DateUtil
import org.apache.spark.sql.SparkSession

object DataManagerTransformerMain extends LoggerTrait{
  def main(args: Array[String]): Unit = {

    val ultimaAtualizacao = DateUtil.validateUpdateDate(args(1))
    log.info ("[*] ultimaAtualizacao = " + ultimaAtualizacao)
    println("[*] ultimaAtualizacao = " + ultimaAtualizacao)

    val sparkSession = SparkSession.builder.enableHiveSupport.getOrCreate
    val pedidoService = new PedidoService(sparkSession, ultimaAtualizacao)
    pedidoService.extractAndSavePedidos

    log.info("[*] DataManagerTransformer concluido")
    println("[*]  DataManagerTransformer concluido")
  }
}
