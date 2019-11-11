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

MD_CONFIG_PATH=$1
MD_JAR=metadata-digger-$MD_VERSION.jar
MD_LIBS=libs/*

if [ ! -f "$MD_CONFIG_PATH" ]
then
  echo "Error: First argument has to be path to existing configuration file!"
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
java -cp $MD_JAR:$MD_LIBS ai.datahunters.md.launcher.BasicExtractorLauncher $MD_CONFIG_PATH 1
