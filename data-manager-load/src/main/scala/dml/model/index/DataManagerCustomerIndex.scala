package dml.model.index

import dml.model.Order

object DataManagerCustomerIndex {

  val TYPE: String = "customer"
  val ID: String = Order.customerid
  val MAKE_ID_REVERSED: Boolean = true

  val customerid = "customerId"
  val summary = "summary"
  val name = "name"
  val ultimaAtualizacao = "ultimaAtualizacao"
  val flagEnabled = "flagEnabled"

}
