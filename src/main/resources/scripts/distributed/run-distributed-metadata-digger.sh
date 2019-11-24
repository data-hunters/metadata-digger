#!/bin/sh
# Starting script for Metadata Digger Basic Extractor Job
# Only one argument is needed - path to Metadata Digger configuration file.
# Sample execution: sh run-distributed-metadata-digger.sh csv.config.properties

MD_VERSION=0.1.2

DH="

                                               ||
                                             |||||
                                           ||||||||
                                         ||||||||||
                                       ||||||||||||
                             |||||||||||||||||||||
                           ||||||   |||||||||||||
                         ||||||    |||||||||||||||||||
                       ||||||||||||||||||||||||||||||||
             ||||||||||||||||||||||||||||||||||||||||
       ||||||||||||||||||||||||||||||||||||||||||||
           ||||||||||||||||||||||||||||||||||||||
              |||||||||||||||||||||||||||||||||
             ||||||||||||||||||||||||||||||||||
           ||||||||||||||||||||||||||||||||||||
         ||||||||||||||||||||||||||||||||||||||
                    |||||||||||||||||||||||||||
                    |||||||||||||||||||||||||||
                    |||||||||||||||||||||||||||
                    |||||||| ||||||||||    ||||
                    ||||||     ||||||||
                    ||||         |||||
                    ||             ||

                DataHunters.ai - Big Data & OSINT
                   Contact: dev@datahunters.ai

                         Metadata Digger
                             v.$MD_VERSION
                        [Standalone Mode]
                       Apache License 2.0

"

echo "$DH"

MD_ENV_FILE="metadata-digger-env.sh"

if [ ! -f "$MD_ENV_FILE" ]
then
  echo "Error: File $MD_ENV_FILE does not exist!"
  exit 1
fi
. "./$MD_ENV_FILE"

MD_CONFIG_PATH=$1
MD_JAR=metadata-digger-$MD_VERSION.jar
MD_ACTION=$2
BASE_IMG_FILE=""

if [ ! -f "$MD_CONFIG_PATH" ]
then
  echo "Error: First argument has to be path to existing configuration file!"
  exit 1
fi

CLUSTER_MODE=$(grep "DEPLOY_MODE=cluster" $MD_ENV_FILE)
if [ -z $CLUSTER_MODE ] # If empty - it means we have client mode
then
  echo "Client mode."
  MD_CONFIG_FNAME=$MD_CONFIG_PATH
else
  echo "Cluster mode."
  MD_CONFIG_FNAME=$(basename $MD_CONFIG_PATH)
fi

case "$MD_ACTION" in
    "extract")
        MAIN_CLASS=ai.datahunters.md.launcher.BasicExtractorLauncher ;;
    "describe")
        MAIN_CLASS=ai.datahunters.md.launcher.MetadataPrinterLauncher ;;
    "find_similar")
        MAIN_CLASS=ai.datahunters.md.launcher.SimilarMetadataExtractionLauncher
        BASE_IMG_FILE=$3
        if [ ! -f "$BASE_IMG_FILE" ] ; then
            echo "Error: Third argument has to be path to existing image file for action $MD_ACTION!"
            exit 1
        fi
        ;;
    *)
        echo "Action $MD_ACTION not supported!"
        exit 1
esac

echo "Action: $MD_ACTION"

MD_STANDALONE=0

spark-submit --class $MAIN_CLASS \
    --master $SPARK_MASTER \
    --deploy-mode $DEPLOY_MODE \
    --conf spark.driver.userClassPathFirst=true \
    --conf spark.executor.userClassPathFirst=true \
    --driver-memory $DRIVER_MEMORY \
    --executor-memory $EXECUTOR_MEMORY \
    --executor-cores $EXECUTOR_CORES \
    --queue $QUEUE \
    --files $MD_CONFIG_PATH \
    $MD_JAR \
    $MD_CONFIG_FNAME \
    $MD_STANDALONE \
    $BASE_IMG_FILE
