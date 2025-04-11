#!/bin/bash

SCRIPT=$(readlink -f "$0")
SCRIPTPATH=$(dirname "$SCRIPT")
DIRPARENT=$(dirname "$SCRIPTPATH")

source $DIRPARENT/conf

mkdir -p $LOGPATH/`date +\%Y\%m\%d`
LOGFILE=$LOGPATH/`date +\%Y\%m\%d`/expurgo_study_`date +\%Y\%m\%d\%H\%M`.log

info(){
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (INFO)] $1" 2>&1 | tee --append $LOGFILE
}

error(){
    echo "[$(date +%Y-%m-%d\ %H:%M:%S) (ERROR)] $1" 2>&1 | tee --append $LOGFILE
}


DIA=$(date --date="${NUMDAYSRAW} days ago" +%Y%m%d)


info "limpando arquivos anteriores a ${DIA}"
kinit -R -kt ${KERBEROS_KEYTAB} ${KERBEROS_PRINCIPAL}@${KERBEROS_REALM} >> $LOGFILE 2>&1 && 
{
    info "criando pasta de backup"
    for i in $(hdfs dfs -ls ${ROOT_DIR}/datawarehouse/study/*.csv | awk '{printf $6"\n"}' | sort | uniq | sed 's/-//g'); do hdfs dfs -mkdir -p ${ROOT_DIR}/datawarehouse/study_bkp/$i; done

    for j in $(hdfs dfs -ls ${ROOT_DIR}/datawarehouse/study/*.csv | awk '{printf $6 $8"\n"}');
    do
        info "j: $j"

        ultima_atualizacao=$(echo "$j" | cut -c1-10 | sed s'/-//g');
        info "ultima_atualizacao: ${ultima_atualizacao}"

        arquivo=$(echo "$j" |  cut -c11-150)
        info "Arquivo: ${arquivo}"


        if [ $ultima_atualizacao -lt $DIA ]
        then
            info "Movendo pro backup:  $arquivo -> ${ROOT_DIR}/datawarehouse/study_bkp/${ultima_atualizacao}"
            hdfs dfs -mv $arquivo ${ROOT_DIR}/datawarehouse/study_bkp/${ultima_atualizacao} 2>&1 | tee --append $LOGFILE &&
            {
                info "Done!"
            } ||
            {
                error "Fail!"
                exit 1
            }
        else
            info "arquivos movidos para backup"
            break
        fi
    done

    info "--------------------------------------------------------------"
    info "Expurgando ${ROOT_DIR}/datawarehouse/study_bkp"

    DIA_BACKUP=$(date --date="${NUMDAYSRAW} days ago" +%Y%m%d)
    info "Removendo diretorios anteriores a $DIA_BACKUP, maximo de 100 diretorios por execucao"

    for j in $(hdfs dfs -ls ${ROOT_DIR}/datawarehouse/study_bkp/ | head -100 | grep "^d" | awk '{printf $8"\n"}');
    do
        info "item: $j"

        ultima_atualizacao=$(basename $j);
        info "ultima_atualizacao: ${ultima_atualizacao}"


        if [ $ultima_atualizacao -lt $DIA_BACKUP ]
        then
            info "Remove diretorio de backup:  ${ROOT_DIR}/datawarehouse/study_bkp/${ultima_atualizacao}"
            info "Executando: hdfs dfs -rm -r ${ROOT_DIR}/datawarehouse/study_bkp/${ultima_atualizacao} "
            hdfs dfs -rm -r ${ROOT_DIR}/datawarehouse/study_bkp/${ultima_atualizacao}  2>&1 | tee --append $LOGFILE &&
            {
                info "Sucesso"
            } ||
            {
                error "Fim do processo - Abortando por Falha"
                exit 1
            }
        else
            info "todos diretorios expurgados"
            break
        fi
    done


    info "Fim do processo - Sucesso"
    exit 0
} || {
    error "Fim do processo - Falha do Kerberos"
    exit 2
}
