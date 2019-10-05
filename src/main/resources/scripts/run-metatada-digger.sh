#!/bin/bash

echo "Configuration file path: $1"
java -cp metadata-digger-0.1.1.jar:spark-2.3.4-bin-hadoop2.7/jars/* ai.datahunters.md.launcher.StandaloneBasicExtractor $1
