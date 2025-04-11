package dml.repository.elastic

import dml.config.logger.LoggerTrait
import dml.config.repository.ElasticSearchHttpClient
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer


trait AbstractElasticIndexDAO extends LoggerTrait {

    def convertDFtoIndexForElasticJson(dfImei: DataFrame, update: Boolean): RDD[String]

    def saveIntoElastic(df: DataFrame, index: String,
                        _type: String, key_id: String, isIdReversed: Boolean, sc: SparkContext): Unit = {
            log.debug("[*] updateOrInsert BEGIN")

            val _json = convertDFtoIndexForElasticJson(df, update = true)
            val index_type = index + "/" + _type

            if (log.isDebugEnabled) {
                log.debug(s"[*] index_type = $index_type")
                log.debug("[*] _json.count() UPDATE -> " + _json.count())
            }

            val elasticSearchHandler = ElasticSearchHttpClient.apply() //construtor

            val docs_sent = elasticSearchHandler.doSave(_json, index_type, sc)
//            log.debug("[*] docs_sent -> " + docs_sent) //usar apenas em desenvolvimento, produz muito log

            val insertIDsList = listIDsForElasticInsert(docs_sent, _type)
//            log.debug("[*] insertIDsList -> " + insertIDsList.toString()) //usar apenas em desenvolvimento, produz muito log
            if ( insertIDsList.nonEmpty ) {
                val insert_df = concatIDsToWhereClause(df, key_id, insertIDsList, isIdReversed)
                val _json = convertDFtoIndexForElasticJson(insert_df, update = false)
                if (log.isDebugEnabled) {
                    log.debug("[*] _json.count() INSERT -> " + _json.count())
                }
                elasticSearchHandler.doSave(_json, index_type, sc)
            }
        log.debug("[*] updateOrInsert END")
    }

    private def listIDsForElasticInsert(updateResponsePayload: String, indexName: String): List[String] = {
        log.debug("[*] listIDsForElasticInsert BEGIN")
        val insertIDs = new ListBuffer[String]
        val pattern = (s"""\\[${indexName}\\]\\[(.*?)\\]""").r
//        log.debug(updateResponsePayload) //usar apenas em desenvolvimento, produz muito log
        pattern.findAllIn(updateResponsePayload).matchData foreach {
            m => insertIDs += m.group(1)
        }
        log.debug("[*] listIDsForElasticInsert END")
        insertIDs.toList
    }

    private def concatIDsToWhereClause(df: DataFrame, key_id: String,
                insertIDsList: List[String], isIdReversed: Boolean): DataFrame = {
        log.debug("[*] concatIDsToWhereClause")
        if(log.isDebugEnabled){
            println("[*] received DF for conversion")
            df.show(20, truncate = false)
        }

        var prefix = key_id + " in ( \""
        var separator = "\",\""
        var sufix = "\" )"
        if (isIdReversed){
            if ( insertIDsList.length != 1 ) {
                separator = "\" ), REVERSE( \""
            }
            prefix = key_id + " in ( REVERSE ( \""
            sufix = "\" ) )"
        }
        if ( insertIDsList.nonEmpty ) {
            val _where = insertIDsList.distinct.mkString(prefix, separator, sufix)
            val _df = df.where(_where)
            if (log.isDebugEnabled) {
//                println("[*] WHERE_CLAUSE for DF to Insert conversion: " + _where) //usar apenas em desenvolvimento, produz muito log
                println("[*] converted DF for INSERT Elastic")
                _df.show(20, truncate = false)
            }
            _df
        } else {
            if (log.isDebugEnabled) {
                println("[*] unconverted DF")
            }
            df
        }
    }
}
