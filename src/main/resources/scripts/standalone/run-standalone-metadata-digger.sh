#!/bin/sh

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
RUN_ON_SPARK=1
BASE_IMG_FILE=""

echo "Action: $MD_ACTION"
case "$MD_ACTION" in
    "extract")
        MAIN_CLASS=ai.datahunters.md.launcher.BasicExtractorLauncher ;;
    "describe")
        MAIN_CLASS=ai.datahunters.md.launcher.MetadataPrinterLauncher ;;
    "detect_categories")
        MAIN_CLASS=ai.datahunters.md.launcher.MetadataEnrichmentLauncher ;;
    "full")
        MAIN_CLASS=ai.datahunters.md.launcher.FullMDLauncher ;;
    "find_similar")
        MAIN_CLASS=ai.datahunters.md.launcher.SimilarMetadataExtractionLauncher
        BASE_IMG_FILE=$3
        echo "Base file for comparison: $BASE_IMG_FILE"
        if [ ! -f "$BASE_IMG_FILE" ] ; then
            echo "Error: Third argument has to be path to existing image file for action $MD_ACTION!"
            exit 1
        fi
        ;;
    "extract_single")
        MAIN_CLASS=ai.datahunters.md.launcher.LocalBasicExtractorLauncher
        RUN_ON_SPARK=0
        ;;
    *)
        echo "Action $MD_ACTION not supported!"
        exit 1
esac




if [ "$RUN_ON_SPARK" = "1" ] ; then
    MD_CONFIG_PATH=$2
    MEMORY=$(grep -iR "^processing.maxMemoryGB" $MD_CONFIG_PATH | awk -F "=" '{print $2}')
    if [ ! -z "$MEMORY" ] ; then
        MEMORY_OPT="-Xmx${MEMORY}g"
        echo "Setting memory to value passed in $MD_CONFIG_PATH: $MEMORY"
    fi

    for arg in "$@"
    do
      if [ "$arg" = "--includeAWS" ]; then
        echo "Including AWS libs..."
        MD_LIBS=aws_libs/*:$MD_LIBS
      fi
    done
    echo "Configuration file path: $MD_CONFIG_PATH"

    if [ ! -f "$MD_CONFIG_PATH" ] ; then
      echo "Error: Second argument has to be path to existing configuration file!"
      exit 1
    fi
    # BASE_IMG_FILE exists only for comparison actions
    java $MEMORY_OPT -cp $MD_JAR:$MD_LIBS $MAIN_CLASS $MD_CONFIG_PATH 1 $BASE_IMG_FILE
else
    LOCAL_FILE_PATH=$2
    if [ ! -f "$LOCAL_FILE_PATH" ] ; then
      echo "Error: Second argument has to be path to existing image file!"
      exit 1
    fi
    java -cp $MD_JAR:$MD_LIBS $MAIN_CLASS $2
fi