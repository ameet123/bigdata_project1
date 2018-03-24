#!/usr/bin/env bash

# For SSL DEBUG add -Djavax.net.debug=all to driver and executor java options.
# Sample invocation
#  bash runner.sh   -s yarn -t 3 -o /user/af55267/project/lda_out -i 10 \
#                   -w /user/af55267/project/stopwords2 -k ameet.keytab -p AF55267@DEVAD.WELLPOINT.COM -d

SPARK_SUBMIT=/usr/bin/spark2-submit
APP_JAR=../target/scala-2.10/project_2.10-0.1.jar
VM_OPTIONS="-Dlog4j.properties=./log4j.properties"
CLASS="edu.gatech.cse8803.LdaProcessing"

usage(){
  echo -e "Usage: $0 \n\t-s <mode:local/yarn>\n\t-t <numTopics>\n\t-i <maxIterations>\n\t-k <keytab>\n\t-p
  <principal>\n\t-o <output HDFS dir>\n\t-w <stopwords>"
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

if [ "$#" -lt 6 ]
then
  usage
fi
while getopts ":s:t:i:o:k:p:w:d" x; do
    case "${x}" in
        s)
            SPARK_SVR=${OPTARG}
            (("$SPARK_SVR" == "local" || "$SPARK_SVR" == "yarn")) || usage
            ;;
        t)
            TOPICS=${OPTARG}
            null_check ${TOPICS}
            ;;
        i)
            ITERATIONS=${OPTARG}
            null_check ${ITERATIONS}
            ;;
        k)
            KEYTAB=${OPTARG}
            keytab_check ${KEYTAB}
            ;;
        p)
            PRINCIPAL=${OPTARG}
            null_check PRINCIPAL
            ;;
        o)
            OUTPUT_DIR=${OPTARG}
            null_check ${OUTPUT_DIR}
            ;;
        w)
            STOPWORDS=${OPTARG}
            null_check ${STOPWORDS}
            ;;
        d)
            PURGE="Y"
            ;;
        *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

echo -e "Running LDA :\n{\n\tMode=${SPARK_SVR}\n\tPrincipal=${PRINCIPAL}\n\tKeytab=$KEYTAB\n\tTopics=${TOPICS}\n\tIterations=${ITERATIONS}\n\tOutput=${OUTPUT_DIR}\n\tStopwords=${STOPWORDS}\n}"

if [ "$PURGE" == "Y" ]
then
  echo "Deleting output dir: $OUTPUT_DIR"
  hdfs dfs -rm -r ${OUTPUT_DIR}
fi

if [ "${SPARK_SVR}" == "local" ]
then
    echo ">> Running in LOCAL Mode..."

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
    --class ${CLASS} \
    ${APP_JAR} ${TOPICS} ${ITERATIONS} ${OUTPUT_DIR} $STOPWORDS
else
    echo ">> Running in Yarn Mode..."

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
    --class ${CLASS} \
    ${APP_JAR} ${TOPICS} ${ITERATIONS} ${OUTPUT_DIR} ${STOPWORDS}
fi