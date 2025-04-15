package dmt.repository

import dmt.config.logger.LoggerTrait
import org.apache.spark.sql.{DataFrame, SparkSession}

class PedidosWorkDAO(sparkSession: SparkSession, paramData: String)
  extends LoggerTrait{

    private val workTableName: String = "db_dml_work.pedidos"

    def persist(df: DataFrame): Unit = {
      df.createOrReplaceTempView("PedidosTemp")
      sparkSession.sql(s"ALTER TABLE $workTableName DROP IF EXISTS PARTITION(datapedido='$paramData')")
      sparkSession.sql(s"insert overwrite table $workTableName partition(datapedido='$paramData') select *, 0 from PedidosTemp")
    }

}
