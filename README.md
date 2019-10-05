# Metadata Digger
Main goal of this project is to provide better insights into Metadata extracted from binary files (like images).
MD is built on top of [Apache Spark](https://spark.apache.org/) - one of the most popular Big Data processing engine - to take advantage of distributed computing.

Currently MD is under development but basic functionality is available:

* Extracting Metadata from files located in multiple directories locally or on HDFS
* Basic filtering - you can provide list of allowed groups/directories of tags (e.g.: ExifIFD0, ExifSubIFD, JPEG, GPS) or particular tags.
* Scaling extraction process to multiple machines and cores, so you can work with huge volumes of data
* Saving output in CSV and JSON formats

To provide easy start for OSINT researchers who do not know details of Apache Spark, special Standalone version has been prepared that can utilize many cores of processor on single machine.

## Requirements
Minimal requirements:

* Linux OS
* Java 8
* Memory - at least 2GB

For distributed mode, you should use Spark 2.4.3.

## Output formats
Currently two output formats are supported:
* CSV file
* JSON file - each line has separated JSON object, so it is easy to load data in stream line by line

## Running in Standalone mode
To get current distribution, please go to releases tab and download zipped 0.1.1 version and unpack it. There you will have run-metadata-digger.sh script and two sample configuration files (`json.config.properties` and `csv.config.properties`) with examples for JSON and CSV output format. Pick one, open it and change two settings:

* `input.paths` - paths to directories with files you want to process. You can set multiple paths delimited by comma
* `output.directoryPath` - path to output directory where files with metadata will be written. *Make sure this directory does not exist before you start processing*. Metadata Digger will create it and write there files.

Optional settings:

* `output.format` - currently you can set `csv` or `json`
* `processing.maxMemoryGB` - memory in GB that will be used by Metadata Digger.
* `output.filesNumber` - number of files where Metadata Digger will save results.
* `processing.cores` - number of cores that will be used to parallel processing. If you do not set it, MD will automatically use max cores/threads your machine has - 1.
* `filter.allowedMetadataDirectories` - comma delimited list of allowed directories/groups of tags, e.g. ExifIFD0,ExifSubIFD,JPEG,GPS. If you do not set it, MD will retrieve all existing
* `processing.partitions` - advance property, leave it if you do not know Spark.

When you adjust your config, run the following command (where `<path_to_config>` is path to adjusted configuration file):
```
sh run-metadata-digger.sh <path_to_config>
```

## Running in distributed mode
See above information about running in standalone mode to download release and adjust configuration, just go to `ditributed` directory, not `standalone`.
Currently there is not script that runs Metadata Digger on Spark cluster, so please use spark-submit command with provided JAR and main class - `ai.datahunters.md.launcher.DistributedBasicExtractor`. There is only one argument - path to configuration file.

<br />
<br />
<br />

![DataHunters](http://datahunters.ai/assets/images/logo_full_small.png)
