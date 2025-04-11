package dml.repository


import dml.config.logger.LoggerTrait
import dml.model.Order
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


class OrdersDAO(sparkSession: SparkSession, param: String)
  extends LoggerTrait{

  def getOrdersDF: DataFrame = {

    log.debug("[*] processando param = " + param)

    val ordersDF = sparkSession.table(Order.TABLE)
      .select(Seq(Order.customerid,
        Order.orderid,
        Order.product,
        Order.flagorderactive,
        Order.ultimaAtualizacao)
        .map(c => col(c)): _*)
      .where(s"ultimaAtualizacao = $param and flagOrderActive = 'false' ")
      .groupBy(Order.customerid,
        Order.orderid,
        Order.product,
        Order.flagorderactive,
        Order.ultimaAtualizacao)
      .agg(concat(
        lit("{\"qtd\" : "),
        count(lit(1)),
        lit(", \"products\": \""),
        col(Order.product),
        lit("\"}")
      ).alias("pedidos"))
      .drop(Order.flagorderactive)

    if (log.isDebugEnabled) {
      println("--- ordersDF")
      ordersDF.show(10, truncate = false)
    }

    val pedidosListDF = ordersDF
      .select(Seq(Order.customerid,
        Order.ultimaAtualizacao,
        "pedidos")
        .map(c => col(c)): _*)
      .groupBy(Order.customerid,
        Order.ultimaAtualizacao)
      .agg(collect_set(col("pedidos")).alias("pedidosList"))

    if (log.isDebugEnabled) {
      println("--- OrdersDAO: pedidosListDF")
      pedidosListDF.show(11, truncate = false)
    }

    pedidosListDF
  }

}
