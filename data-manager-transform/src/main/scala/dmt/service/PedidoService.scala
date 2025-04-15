package dmt.service

import dmt.config.logger.LoggerTrait
import dmt.repository.{PedidosWorkDAO, ProdutosStageDAO, VendaStageDAO}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class PedidoService(sparkSession: SparkSession, paramData: String) extends LoggerTrait {

  def extractAndSavePedidos: Unit = {
    val dfVendas = extractVendas()
      .withColumn("datavenda", when (col("datavenda")
        .equalTo(lit("00.00.0000")), lit(""))
        .otherwise(col("datavenda")))

    val dfProdutos = extractProdutos()
    val dfPedidos = findPedidosCancelados(dfVendas, dfProdutos)
    savePedidos(dfPedidos)
  }

  def extractVendas(): DataFrame = {
    val vendaRawDAO = new VendaStageDAO(sparkSession)
    vendaRawDAO.getVendas
  }

  private def extractProdutos(): DataFrame = {
    val produtosStageDAO = new ProdutosStageDAO(sparkSession)
    produtosStageDAO.getProdutos
  }

  private def savePedidos(df : DataFrame): Unit = {
    val pedidosWorkDAO = new PedidosWorkDAO(sparkSession, paramData)
    pedidosWorkDAO.persist(df)
  }


  private def findPedidosCancelados(dfVendas : DataFrame, dfProdutos : DataFrame): DataFrame = {

    val dfVendasCanceladas = dfVendas
      .where(col("typecode")
        .isin("OP05", "OP06", "OP08"))
      .where(col("vendaid")
        .notEqual(lit("")))
      .withColumnRenamed("comentario", "motivocancelamento")
      .select(col("vendaid").alias("vendacanceladaid"),
        col("clientid"),
        col("originalvendaid"),
        col("datavenda").alias("datacancelada"),
        col("motivocancelamento"))


    if (log.isDebugEnabled) {
      println("--- dfVendasCanceladas")
      dfVendasCanceladas.printSchema()
      dfVendasCanceladas.show(20, truncate = false)
    }

    val dfProdutosDetalhes =  dfProdutos
      .select(col("vendaid"),
        col("produtoid"),
        col("barcode"),
        col("modelo"))

    if (log.isDebugEnabled) {
      println("--- dfProdutosDetalhes")
      dfProdutosDetalhes.printSchema()
      dfProdutosDetalhes.show(20, truncate = false)
    }

    val dfPedidosCancelados = dfVendasCanceladas
      .join(dfProdutosDetalhes,
        dfVendasCanceladas.col("vendacanceladaid") === dfProdutosDetalhes.col("vendaid"),
        "inner")
      .select (
        col("vendacanceladaid"),
        col("originalvendaid"),
        col("datacancelada"),
        col("motivocancelamento"),
        col("produtoid"),
        col("barcode"),
        col("modelo")
    )

    if (log.isDebugEnabled) {
      println("--- dfPedidosCancelados")
      dfPedidosCancelados.printSchema()
      dfPedidosCancelados.show(20, truncate = false)
    }

    dfPedidosCancelados
  }

}
