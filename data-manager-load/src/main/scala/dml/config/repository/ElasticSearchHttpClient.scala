package dml.config.repository

import com.typesafe.config.ConfigFactory
import dml.config.logger.LoggerTrait
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.config.{RegistryBuilder, SocketConfig}
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.conn.ssl.{SSLConnectionSocketFactory, SSLContexts, TrustStrategy}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{BasicCredentialsProvider, BasicResponseHandler, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods.parse
import java.security.cert.X509Certificate

//TODO substituir esse conector por um de library
class ElasticSearchHttpClient extends ElasticSearchManagerTrait {}

// construtor
object ElasticSearchHttpClient {
    def apply():
    ElasticSearchHttpClient = new ElasticSearchHttpClient()
}

//interface de configuracao geral
trait ElasticSearchManagerTrait extends LoggerTrait {
    
    private val configManager = ConfigFactory.load()
    // timeout config de 4 minutos
    private val TIMEOUT: Int = 60000 * configManager
      .getString("dml-elastic.spark.time_out_minute").toInt


    def doSave(entity: RDD[String], indexType: String, sc: SparkContext):String = {
        var entityTmp = entity

        val elasticSearchUrl = configManager
          .getString("dml-elastic.elastic-search.host") + ":" +
          configManager.getString("dml-elastic.elastic-search.port")

        val MAX_JSON_ITENS = configManager
          .getInt("dml-elastic.elastic-search.max-post-itens")

        var done = false
        var respList = parse("{}")
        while (!done) {
            val slice = entityTmp.take(MAX_JSON_ITENS)
            entityTmp = entityTmp.keyBy(x => x)
              .subtractByKey(sc.parallelize(slice).keyBy(x => x)).map(_._1)

            if (slice.isEmpty) {
                done = true
            } else {
                val post = new HttpPost(elasticSearchUrl + "/" + indexType + "/_bulk")
                val json_check = slice.mkString
                post.setEntity(new StringEntity(json_check, "UTF-8"))
                post.addHeader("Content-Type", "application/x-ndjson")
                post.addHeader("Accept-Charset", "UTF-8")
                val httpClient = getHttpClient // chama todos os outros metodos dessa classe
//                    log.debug(json_check) // produz muito log, usar apenas em desenvolvimento
                respList = respList.++(parse(new BasicResponseHandler()
                  .handleResponse(httpClient.execute(post)), useBigDecimalForDouble = false))
//                    .handleResponse(executeDebugAndBreak(post, httpClient)), useBigDecimalForDouble = false))
                log.debug("[*] CHECKPOINT ElasticHandlerAccess.doSave")
            }
        }
        log.debug("[*] END ElasticHandlerAccess.doSave")
//            log.debug(respList.values.toString);// produz muito log, usar apenas em desenvolvimento
        respList.values.toString

    }

    
    
    //----------- configuracoes do HttpClient
    
    private def getHttpClient = {
        log.debug("[*] Setting HttpClient ")
        HttpClients
          .custom()
          .setDefaultRequestConfig(getHttpClientRequestConfiguration)
          .setConnectionManager(getHttpClientConnManager)
          .setDefaultCredentialsProvider(getHttpClientProvider)
          .build()
    }

    private def getHttpClientRequestConfiguration = {
        log.debug("[*] Setting Request Configurations ")
        val requestConfig: RequestConfig = RequestConfig.custom()
          .setSocketTimeout(TIMEOUT)
          .setConnectionRequestTimeout(TIMEOUT)
          .setConnectTimeout(TIMEOUT)
          .build()
        requestConfig
    }

    private def getHttpClientConnManager = {
        log.debug("[*] Setting Connection Manager ")
        val cm = new PoolingHttpClientConnectionManager(RegistryBuilder.create[ConnectionSocketFactory]()
          .register("https", getHttpClientConnManagerSslsf)
          .register("http", PlainConnectionSocketFactory.INSTANCE).build())
        cm.setDefaultSocketConfig(getHttpClientConnManagerSocketConfig)
        cm.setMaxTotal(200)
        cm.setDefaultMaxPerRoute(20)
        cm
    }

    private def getHttpClientProvider: BasicCredentialsProvider = {
        val user = configManager.getString("equipments-elastic.elastic-search.username")
        val pass = configManager.getString("equipments-elastic.elastic-search.password")
        val provider = new BasicCredentialsProvider()
        if (user == "" && pass == "") {
            log.debug("[*] Credentials Config not found. ")
            return provider
        }
        val credentials = new UsernamePasswordCredentials(user, pass)
        provider.setCredentials(AuthScope.ANY, credentials)
        provider
    }

    private def getHttpClientConnManagerSocketConfig = {
        log.debug("[*] Setting Socket Configurations ")
        val socketConfig: SocketConfig = SocketConfig.custom()
          .setSoTimeout(TIMEOUT)
          .setTcpNoDelay(true)
          .build()
        socketConfig
    }

    private def getHttpClientConnManagerSslsf = {
        log.debug("[*] Setting SSLSF ")
        val sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy {
            override def isTrusted(chain: Array[X509Certificate], authType: String): Boolean = true
        }).build()
        new SSLConnectionSocketFactory(sslContext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
    }

}
