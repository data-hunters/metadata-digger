#!/bin/bash

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
                        [Standalone Mode]
                       Apache License 2.0

"

echo "$DH"

MD_CONFIG_PATH=$1

if [ ! -f "$MD_CONFIG_PATH" ]
then
  echo "Error: First argument has to be path to existing configuration file!"
  exit 1
fi

echo "Configuration file path: $MD_CONFIG_PATH"
java -cp metadata-digger-0.1.1.jar:lib/* ai.datahunters.md.launcher.BasicExtractorLauncher $MD_CONFIG_PATH 1
