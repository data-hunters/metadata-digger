#!/bin/bash
# Starting script for Metadata Digger Basic Extractor Job
# Only one argument is needed - path to Metadata Digger configuration file.
# Sample execution: sh run-distributed-metadata-digger.sh csv.config.properties

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
                       [Distributed Mode]
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

MD_VERSION=0.1.1
MD_CONFIG_PATH=$1
MD_JAR=metadata-digger-$MD_VERSION.jar

if [ ! -f "$MD_CONFIG_PATH" ]
then
  echo "Error: First argument has to be path to existing configuration file!"
  exit 1
fi

spark-submit --class ai.datahunters.md.launcher.BasicExtractorLauncher \
    --master $SPARK_MASTER \
    --deploy-mode $DEPLOY_MODE \
    --conf spark.driver.userClassPathFirst=true \
    --conf spark.executor.userClassPathFirst=true \
    --driver-memory $DRIVER_MEMORY \
    --executor-memory $EXECUTOR_MEMORY \
    --executor-cores $EXECUTOR_CORES \
    --queue $QUEUE \
    --conf spark.files=$MD_CONFIG \
    $MD_JAR \
    $MD_CONFIG
