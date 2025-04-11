package dml.repository

import dml.config.logger.LoggerTrait
import dml.model.{Customer, Order}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class CustomerReportDAO(sparkSession: SparkSession, param: String)
  extends LoggerTrait {

  def loadCustomers(df: DataFrame): DataFrame = {

    log.debug("[*] processando param = " + param)

    val ID_TMP = "tmpid"
    val DATA_TMP = "tmpdata"

    val customersDF = sparkSession.table(Customer.TABLE)
      .select(Seq(Customer.id,
        Customer.name,
        Customer.flagEnabled,
        Customer.ultimaAtualizacao)
        .map(c => col(c)): _*)
      .where(s"ultimaAtualizacao = $param ")
      .withColumnRenamed(Customer.id, ID_TMP)
      .withColumnRenamed(Customer.ultimaAtualizacao, DATA_TMP)

    if (log.isDebugEnabled) {
      println("--- customersDF")
      customersDF.show(15, truncate = false)
    }

    val joinedCustomersDF = df.join(customersDF,
      df(Order.customerid) === customersDF(ID_TMP),
      "inner"
    ).select(Seq(Customer.id,
      Customer.name,
      Customer.flagEnabled,
      "summary",
      Customer.ultimaAtualizacao)
      .map(c => col(c)): _*)

    if (log.isDebugEnabled) {
      println("--- joinedCustomersDF")
      joinedCustomersDF.show(15, truncate = false)
    }

    joinedCustomersDF
  }

}
