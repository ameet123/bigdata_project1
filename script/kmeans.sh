#!/usr/bin/env bash

SPARK_SUBMIT=/usr/bin/spark2-submit
APP_JAR=../target/scala-2.10/project_2.10-0.1.jar
VM_OPTIONS="-Dlog4j.configuration=file://`pwd`/log4j.properties"
KMEANS_CLASS="edu.gatech.cse8803.TopicKmeans"

usage(){
  echo -e "Usage: $0 \n\t-s <mode:local/yarn>\n\t-t <subject-topic file>\n\t-m <subject-mortality file>\n\t-k <keytab>\n\t-p <principal>\n"
  exit 2
}
null_check(){
  arg=$1
  if [ "$1" == "" ]
  then
    echo "Invalid option"
    usage
  fi
}
keytab_check(){
  file=$1
  if [ ! -s ${file} ]
  then
    echo "Invalid keytab:$file"
    exit 4
  fi
}

if [ "$#" -lt 4 ]
then
  usage
fi
while getopts ":s:t:k:p:m:" x; do
    case "${x}" in
        s)
            SPARK_SVR=${OPTARG}
            (("$SPARK_SVR" == "local" || "$SPARK_SVR" == "yarn")) || usage
            ;;
        t)
            TOPIC_MAP_FILE=${OPTARG}
            null_check ${TOPIC_MAP_FILE}
            ;;
        m)
            MORT_FILE=${OPTARG}
            null_check ${MORT_FILE}
            ;;
        k)
            KEYTAB=${OPTARG}
            keytab_check ${KEYTAB}
            ;;
        p)
            PRINCIPAL=${OPTARG}
            null_check PRINCIPAL
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

echo -e "Running LDA :\n{\n\tMode=${SPARK_SVR}\n\tPrincipal=${PRINCIPAL}\n\tKeytab=$KEYTAB\n\tTopicFile=${TOPIC_MAP_FILE}\n\tMortality file=${MORT_FILE}\n}"

if [ "${SPARK_SVR}" == "local" ]
then
    echo ">> Kmeans:Running in LOCAL Mode..."

    ${SPARK_SUBMIT} \
    --master ${SPARK_SVR} \
    --keytab ${KEYTAB} \
    --principal ${PRINCIPAL} \
    --conf "spark.driver.port=39200" \
    --executor-cores 4 \
    --num-executors 2 \
    --driver-memory 1g --executor-memory 1g \
    --driver-java-options "${VM_OPTIONS}" \
    --conf "spark.executor.extraJavaOptions=${VM_OPTIONS}" \
    --conf spark.network.timeout=10000000 \
    --conf spark.ui.port=24100 \
    --class ${KMEANS_CLASS} \
    ${APP_JAR} ${TOPIC_MAP_FILE} ${MORT_FILE}
else
    echo ">> Kmeans:Running in Yarn Mode..."

    ${SPARK_SUBMIT} \
    --queue bdf_yarn \
    --master ${SPARK_SVR} \
    --keytab ${KEYTAB} \
    --principal ${PRINCIPAL} \
    --conf "spark.driver.port=39200" \
    --executor-cores 4 \
    --num-executors 100 \
    --driver-memory 4g --executor-memory 4g \
    --driver-java-options "${VM_OPTIONS}" \
    --conf "spark.executor.extraJavaOptions=${VM_OPTIONS}" \
    --conf spark.network.timeout=10000000 \
    --conf spark.ui.port=24100 \
    --class ${KMEANS_CLASS} \
    ${APP_JAR} ${TOPIC_MAP_FILE} ${MORT_FILE}
fi