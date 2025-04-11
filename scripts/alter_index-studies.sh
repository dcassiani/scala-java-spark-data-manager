#!/bin/bash

APP_CONF=/scripts/application.conf

if [ ! -f ${ARQ_CONF} ]
then
        echo "[$(date +%Y-%m-%d\ %H:%M:%S) (ERROR)] Arquivo de configuracao nao encontrado ($ARQ_CONF)"
        exit 1
fi

source ${ARQ_CONF}

LOGFILE=$LOGPATH/`date +\%Y\%m\%d`/alter_index-studies_`date +\%Y\%m\%d\%H\%M\%S`.log
mkdir -p $LOGPATH/`date +\%Y\%m\%d`

info(){ 
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (INFO)] $1" 2>&1 | tee --append $LOGFILE 
}
error(){ 
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (ERROR)] $1" 2>&1 | tee --append $LOGFILE 
} 

#----------------------------------------------------------------------------------
info "$0: Inicio"

info "LOGFILE=${LOGFILE}"

USER=$(grep -A2 ElasticCredential  ${APP_CONF} | grep '"username":' | cut -d'"' -f4)
info "USER=${USER}"

PASSWD=$(grep -A2 ElasticCredential  ${APP_CONF}  | grep '"password":' | cut -d'"' -f4)


ELASTIC_SERVER=$(grep -A3 ElasticURL  ${APP_CONF} | grep '"host":' | cut -d'"' -f4)
info "ELASTIC_SERVER=${ELASTIC_SERVER}"

ELASTIC_PORT=$(grep -A3 ElasticURL  ${APP_CONF} | grep '"port":' | cut -d'"' -f4)
info "ELASTIC_PORT=${ELASTIC_PORT}"

INDEX_SRC=studies_dat
INDEX_DEST=studies_dat-20231010
ALIAS_BACKEND=studies
ALIAS_BACK_OFFICE=studies_back_office

info "INDEX_SRC=${INDEX_SRC}"
info "INDEX_DEST=${INDEX_DEST}"

info "Criando o indice ${INDEX_DEST} com novo mapping"
curl -H "Content-Type: application/json" -u ${USER}:${PASSWD} -XPUT "${ELASTIC_SERVER}:${ELASTIC_PORT}/${INDEX_DEST}" --noproxy '*' --insecure -d '{
"settings" : {"index" : {"number_of_shards" : 10, "number_of_replicas" : 2 }}, 
    "mappings" : {
      "study" : {
        "properties" : {
          "nome" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            },
            "fielddata" : true
          },
          "quantidade" : {
            "type" : "long"
          },
          "ultimaAtualizacao" : {
            "type" : "text",
            "fields" : {
              "keyword" : {
                "type" : "keyword",
                "ignore_above" : 256
              }
            }
          },
        }
      }
    }    
}'

info "Fazendo a copia do indice ${INDEX_SRC} para o indice de backup ${INDEX_DEST}"

curl -H "Content-Type: application/json" -u ${USER}:${PASSWD} -XPOST "${ELASTIC_SERVER}:${ELASTIC_PORT}/_reindex" --noproxy '*' --insecure -d "{ \"source\": { \"index\": \"${INDEX_SRC}\", \"_source\": [ \"nome\", \"quantidade\", \"ultimaAtualizacao\" ] }, \"dest\": { \"index\": \"${INDEX_DEST}\", \"type\": \"study\" } }"

info " "
info "---------------------------------"

info "Removendo o alias ${ALIAS_BACKEND} do indice ${INDEX_SRC}"

curl -H "Content-Type: application/json" -u ${USER}:${PASSWD} -XPOST "${ELASTIC_SERVER}:${ELASTIC_PORT}/_aliases?pretty" --noproxy '*' --insecure -d "{
  \"actions\": [
    {
      \"remove\": {
        \"index\": \"${INDEX_SRC}\",
        \"alias\": \"${ALIAS_BACKEND}\"
      }
    }
  ]
}" 2>&1 | tee --append $LOGFILE

info " "
info "---------------------------------"

info "Removendo o alias ${ALIAS_BACK_OFFICE} do indice ${INDEX_SRC}"

curl -H "Content-Type: application/json" -u ${USER}:${PASSWD} -XPOST "${ELASTIC_SERVER}:${ELASTIC_PORT}/_aliases?pretty" --noproxy '*' --insecure -d "{
  \"actions\": [
    {
      \"remove\": {
        \"index\": \"${INDEX_SRC}\",
        \"alias\": \"${ALIAS_BACK_OFFICE}\"
      }
    }
  ]
}" 2>&1 | tee --append $LOGFILE



info " "
info "---------------------------------"
info "Criando o alias ${ALIAS_BACKEND} para o indice ${INDEX_DEST}"
curl  -H "Content-Type: application/json" -u ${USER}:${PASSWD} -XPOST "${ELASTIC_SERVER}:${ELASTIC_PORT}/_aliases?pretty" --noproxy '*' --insecure  -d "
{
  \"actions\": [
    {
      \"add\": {
        \"index\": \"${INDEX_DEST}\",
        \"alias\": \"${ALIAS_BACKEND}\"
      }
    }
  ]
}" 2>&1 | tee --append $LOGFILE


info " "
info "---------------------------------"
info "Criando o alias ${ALIAS_BACK_OFFICE} para o indice ${INDEX_DEST}"
curl  -H "Content-Type: application/json" -u ${USER}:${PASSWD} -XPOST "${ELASTIC_SERVER}:${ELASTIC_PORT}/_aliases?pretty" --noproxy '*' --insecure  -d "
{
  \"actions\": [
    {
      \"add\": {
        \"index\": \"${INDEX_DEST}\",
        \"alias\": \"${ALIAS_BACK_OFFICE}\"
      }
    }
  ]
}" 2>&1 | tee --append $LOGFILE



info "$0: SUCCEEDED"
exit 0
