package dml.config.repository

import com.typesafe.config.ConfigFactory

class ElasticIndexConfig extends ElasticIndexConfigTrait {}

object ElasticIndexConfig {
  def apply():
  ElasticIndexConfig = new ElasticIndexConfig()
}

trait ElasticIndexConfigTrait {

  private val configManager = ConfigFactory.load()

  val STR_CUSTOMERS_INDEX = configManager.getString(s"dml.elastic-search.indexes.customers")

}