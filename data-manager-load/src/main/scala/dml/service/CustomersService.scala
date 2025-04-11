package dml.service

import dml.config.logger.LoggerTrait
import dml.repository.elastic.DataManagerIndexDAO
import dml.repository.{CustomerReportDAO, OrdersDAO}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


class CustomersService(sparkSession: SparkSession, sc: SparkContext, paramData: String)
  extends LoggerTrait {

  def extractCustomer(): Unit = {
    log.debug("[*] BEGIN extractCustomer")

    val ordersDAO = new OrdersDAO(sparkSession, paramData)
    val ordersDF = ordersDAO.getOrdersDF

    val customerReportDAO = new CustomerReportDAO(sparkSession, paramData)
    val customerReportDF = customerReportDAO.loadCustomers(ordersDF)

    val dataManagerIndexDAO = new DataManagerIndexDAO(sc)
    dataManagerIndexDAO.save(customerReportDF)

    log.info("[*] END SUCESS extractCustomer")
  }

}
