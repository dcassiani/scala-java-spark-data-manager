package dmt.repository

import dmt.config.logger.LoggerTrait
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

class VendaStageDAO(sparkSession: SparkSession)
  extends LoggerTrait {

    import sparkSession.sqlContext.implicits._

    private val stageTableName: String = "db_dml_stage.vendas"

    def getVendas: DataFrame = {
      val vendasStageDF = sparkSession.table(stageTableName)

      if (log.isDebugEnabled) {
        println("--- vendasStageDF")
        vendasStageDF.printSchema()
        vendasStageDF.show(20, truncate = false)
      }

      log.info("Removendo duplicidades")
      val vendasStageOrderedDF = vendasStageDF
        .withColumn("rownum", row_number().over(Window.partitionBy("vendaid")
          .orderBy(col("datavenda").desc)))

      if (log.isDebugEnabled) {
        println("--- vendasStageOrderedDF")
        vendasStageOrderedDF.printSchema()
        vendasStageOrderedDF.show(20, truncate = false)
      }

      val vendasStageOrderedFirstRowDF = vendasStageOrderedDF
        .where(col("rownum") === lit(1))
        .drop(col("rownum"))

      if (log.isDebugEnabled) {
        println("--- vendasStageOrderedFirstRowDF")
        vendasStageOrderedFirstRowDF.printSchema()
        vendasStageOrderedFirstRowDF.show(20, truncate = false)
      }

      log.debug("[*] END SUCESS getVendas")
      vendasStageOrderedFirstRowDF
    }

}
