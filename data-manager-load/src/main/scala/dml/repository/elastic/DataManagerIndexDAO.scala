package dml.repository.elastic

import dml.config.repository.ElasticIndexConfig
import dml.model.index.DataManagerCustomerIndex
import dml.util.DateUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class DataManagerIndexDAO(sc: SparkContext) extends AbstractElasticIndexDAO{

    def save(df: DataFrame): Unit = {
      log.debug("[*] BEGIN save")
      val elasticConf = ElasticIndexConfig.apply()
      saveIntoElastic(df, elasticConf.STR_CUSTOMERS_INDEX,
        DataManagerCustomerIndex.TYPE, DataManagerCustomerIndex.ID,
        DataManagerCustomerIndex.MAKE_ID_REVERSED, sc)
      log.debug("[*] END SUCCESS save")
    }

  override def convertDFtoIndexForElasticJson(df: DataFrame, update: Boolean): RDD[String] = {
      log.info("[*] convertDFtoIndexForElasticJson BEGIN - Flag update = " + update)
      val ultimaAtualizacao = DateUtil.data_refer
      val json = df.rdd.map(row => { //nesse bloco, nao pode colocar log, ou tera erro SparkException: Task not serializable
        var jsonFinal = ""
        var index_id_json: String = null
        val index_id = if (row.get(0) == null) { "" } else { row.get(0).toString.reverse }

        if (update) {
          index_id_json = "{\"update\": {\"_id\":\"" + index_id + "\"}}\n"
        } else {
          index_id_json = "{\"index\": {\"_id\":\"" + index_id + "\"}}\n"
        }

        val name = if (row.get(0) == null) { "" } else { row.get(0).toString }
        val customerid = if (row.get(1) == null) { "" } else { row.get(1).toString }
        val flagEnabled = if (row.get(2) == null) { "true" } else { row.get(2).toString }

        var summary = "["
        if (row.get(3) == null) {
          summary = summary.concat("]")
        } else {
          val modelArray = row.getAs[mutable.WrappedArray[String]](3)
          for {item <- modelArray} summary += item.concat(",")
          summary = summary.dropRight(1).concat("]")
        }

        jsonFinal += index_id_json
        if (update) {
          jsonFinal += String.format("{ \"doc\": " + "{\""
            + DataManagerCustomerIndex.name + "\": " + "\"" + name + "\"" +
            ",\"" + DataManagerCustomerIndex.customerid + "\": " + "\"" + customerid + "\"" +
            ",\"" + DataManagerCustomerIndex.flagEnabled + "\": " + flagEnabled +
            ",\"" + DataManagerCustomerIndex.summary + "\": " + summary +
            ",\"" + DataManagerCustomerIndex.ultimaAtualizacao + "\": " + "\"" + ultimaAtualizacao
            + "\" }} \n")
        } else {
          jsonFinal += String.format("{\""
            + DataManagerCustomerIndex.name + "\": " + "\"" + name + "\"" +
            ",\"" + DataManagerCustomerIndex.customerid + "\": " + "\"" + customerid + "\"" +
            ",\"" + DataManagerCustomerIndex.flagEnabled + "\": " + flagEnabled +
            ",\"" + DataManagerCustomerIndex.summary + "\": " + summary +
            ",\"" + DataManagerCustomerIndex.ultimaAtualizacao + "\": " + "\"" + ultimaAtualizacao
            + "\" }\n")
        }
        jsonFinal
      })

//    if (log.isDebugEnabled) {
//      println("Quantidade de elementos do json: " + json.count())
//      println("Amostra do json 15")
//      json.take(15).foreach(line => println(line))
//    }

    log.info("[*] convertDFtoIndexForElasticJson END")
    json
  }

}
