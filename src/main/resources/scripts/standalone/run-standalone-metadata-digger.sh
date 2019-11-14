#!/bin/bash

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

MD_JAR=metadata-digger-$MD_VERSION.jar
MD_LIBS=libs/*
MD_ACTION=$1
MD_CONFIG_PATH=$2

if [ ! -f "$MD_CONFIG_PATH" ] ; then
  echo "Error: Second argument has to be path to existing configuration file!"
  exit 1
fi

for arg in "$@"
do
  if [ "$arg" = "--includeAWS" ]; then
    echo "Including AWS libs..."
    MD_LIBS=aws_libs/*:$MD_LIBS
  fi
done


echo "Configuration file path: $MD_CONFIG_PATH"

case "$MD_ACTION" in
    "extract")
        MAIN_CLASS=ai.datahunters.md.launcher.BasicExtractorLauncher ;;
    "describe")
        MAIN_CLASS=ai.datahunters.md.launcher.MetadataPrinterLauncher ;;
    *)
        echo "Action $MD_ACTION not supported!"
        exit 1
esac

echo "Action: $MD_ACTION"

java -cp $MD_JAR:$MD_LIBS $MAIN_CLASS $MD_CONFIG_PATH 1