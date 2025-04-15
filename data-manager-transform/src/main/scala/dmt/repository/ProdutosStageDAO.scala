package dmt.repository

import dmt.config.logger.LoggerTrait
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProdutosStageDAO(sparkSession: SparkSession)
  extends LoggerTrait {

    import sparkSession.sqlContext.implicits._

    private val stageTableName: String = "db_dml_stage.produtos"

    def getProdutos: DataFrame = {
      val produtosStageDF = sparkSession.table(stageTableName)

      if (log.isDebugEnabled) {
        println("--- produtosStageDF")
        produtosStageDF.printSchema()
        produtosStageDF.show(20, truncate = false)
      }

      val produtosStageOrderedDF = produtosStageDF
        .withColumn("rownum", row_number().over(Window
          .partitionBy("vendaid", "produtoid")
          .orderBy(col("partitiondata").desc)))

      if (log.isDebugEnabled) {
        println("--- produtosStageOrderedDF")
        produtosStageOrderedDF.printSchema()
        produtosStageOrderedDF.show(20, truncate = false)
      }

      val produtosStageOrderedUniqueDF = produtosStageOrderedDF
        .where(col("rownum") === lit(1))
        .drop(col("rownum"))

      if (log.isDebugEnabled) {
        println("--- produtosStageOrderedUniqueDF")
        produtosStageOrderedUniqueDF.printSchema()
        produtosStageOrderedUniqueDF.show(20, truncate = false)
      }

      produtosStageOrderedUniqueDF
    }

}
