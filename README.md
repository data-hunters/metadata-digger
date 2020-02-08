![metadata-digger logo](http://datahunters.ai/assets/images/metadata-digger-logo.png)

[![metadata-digger build status](https://github.com/data-hunters/metadata-digger/workflows/build/badge.svg)](https://github.com/data-hunters/metadata-digger/actions?query=workflow%3Abuild)

<!-- toc -->

- [Overview](#overview)
- [Getting Started](#getting-started)
  * [Requirements](#requirements)
  * [Output formats](#output-formats)
  * [Running in Standalone mode](#running-in-standalone-mode)
  * [Running in Distributed mode](#running-in-distributed-mode)
  * [Displaying available Tags](#displaying-available-tags)
  * [Setting up mandatory tags](#setting-up-mandatory-tags)
  * [Finding similar images based on metadata](#finding-similar-images-based-on-metadata)
  * [Displaying Metadata of single file](#displaying-metadata-of-single-file)
  * [Hash generation](#hash-generation)
- [Advanced settings](#advanced-settings)
  * [Reader configuration](#reader-configuration)
    + [Common Reader properties](#common-reader-properties)
    + [Local File System](#local-file-system)
    + [Hadoop Distributed File System](#hadoop-distributed-file-system)
    + [Amazon S3](#amazon-s3)
    + [Digital Ocean Spaces](#digital-ocean-spaces)
  * [Processing configuration](#processing-configuration)
  * [Writer configuration](#writer-configuration)
    + [Common Files Writer properties](#common-files-writer-properties)
    + [Local File System](#local-file-system-1)
    + [Hadoop Distributed File System](#hadoop-distributed-file-system-1)
    + [Amazon S3](#amazon-s3-1)
    + [Digital Ocean Spaces](#digital-ocean-spaces-1)
    + [Apache Solr](#apache-solr)
- [External dependencies](#external-dependencies)
- [Logo](#logo)

<!-- tocstop -->

## Overview

Main goal of Metadata Digger is to provide better insights into Metadata extracted from binary files (currently images).
MD is built on top of <a href="https://spark.apache.org/" target="_blank">Apache Spark</a> - one of the most popular Big Data processing engine - to take advantage of distributed computing.

Features:

* Extracting Metadata from files located in multiple directories, from the following sources:

    * Local File System
    * Hadoop Distributed File System
    * Amazon Simple Storage Service (S3)
    * Digital Ocean Spaces (Spaces Object Storage)

* Basic filtering - you can provide list of allowed groups/directories of tags (e.g.: Exif IFD0, Exif SubIFD, JPEG, GPS).
* Displaying all detected Metatags in provided dataset of files.
* Setting up mandatory tags.
* Finding similar images based on specified set of Metatags.
* Displaying Metadata for single file without running Spark.
* Hash generation with most common algorithms.
* Scaling extraction process to multiple machines and cores, so you can work with huge volumes of data
* Saving output in CSV and JSON formats
* Indexing results to <a href="http://lucene.apache.org/solr/" target="_blank">Apache Solr</a> (Full-Text Search Engine)

To provide easy start for OSINT researchers who do not know details of Apache Spark, special Standalone version has been prepared that can utilize many cores of processor on single machine.
**If you want to try Metadata Digger without going into Big Data/Spark technical details**, read *Getting Started* section, especially *Runing in Standalone mode*. More complex configuration is covered in *Advanced settings*.

Metadata Digger is under development. We are also working on Web Appliction that will allow searching and analysing processed metadata.

<br/>





## Getting Started

### Requirements
Minimal requirements:

* Linux OS
* Java 8
* Memory - at least 1GB

For distributed mode, you should use Spark 2.4.3, however it should also work with other 2.x versions.

### Output formats
Currently two files output formats are supported:

* CSV file
* JSON file - each line has separated JSON object, so it is easy to load data in stream line by line

Additionally it is possible to index metadata directly to  <a href="http://lucene.apache.org/solr/" target="_blank">Apache Solr</a> (one of the most popular Full-Text Search Engine), instead of writing results to file.

### Running in Standalone mode
To get current distribution, please go to releases tab, download `metadata-digger-0.1.2_standalone.zip` file and unpack it. There you will have run-metadata-digger.sh script and two sample configuration files (`json.config.properties` and `csv.config.properties`) with examples for JSON and CSV output format. Pick one, open it and change two settings:

* `input.paths` - paths to directories with files you want to process. You can set multiple paths delimited by comma
* `output.directoryPath` - path to output directory where files with metadata will be written. *Make sure this directory does not exist before you start processing*. Metadata Digger will create it and write there files.

Optional settings:

* `output.format` - currently you can set `csv`, `json` or `solr`
* `processing.maxMemoryGB` - memory in GB that will be used by Metadata Digger.
* `output.filesNumber` [optional] - number of files where Metadata Digger will save results.
* `processing.cores` [optional] - number of cores that will be used to parallel processing. If you do not set it, MD will automatically use max cores/threads your machine has - 1.
* `filter.allowedMetadataDirectories` [optional] - comma delimited list of allowed directories/groups of tags, e.g. Exif IFD0,Exif Sub IFD,JPEG,GPS. If you do not set it, MD will retrieve all existing. See section *Displaying available Tags* for instruction how to check exac names of directories.

When you adjust your config, run the following command (where `<path_to_config>` is path to adjusted configuration file):
```
sh run-standalone-metadata-digger.sh extract <path_to_config> [--includeAWS]
```
Third argument `--includeAWS` is optional but it has to be used if you specified in your configuration that MD loads data from or write to Amazon S3/Digital Ocean Spaces.

### Running in Distributed mode
See above information about running in standalone mode to download release and adjust configuration (just choose `metadata-digger-0.1.2_distributed.zip` file).
To run Metadata Digger in Distribute mode you need a cluster. It could be one of systems [supported by Spark](https://spark.apache.org/docs/latest/cluster-overview.html#cluster-manager-types). After adjusting your config.properties file you will have to set right values in `metadata-digger-env.sh` file. When you do this, you can just run the following script:

```
sh run-distributed-metadata-digger.sh extract <path_to_config>
```

Above script has been tested on YARN cluster (HDP 3.1.4) which is probably the most common case but this script is just `spark-submit` command with appropriate parameters, so if you have different cluster and you know some Spark basics, it will be easy to adjust it.
On most Hadoop Clusters Amazon AWS libraries (for connecting to S3) are available, so we are not including them in our package. However, we know that there are many differences between particular distributions of Hadoop, so in case of lack of AWS SDK, we put it in aws_libs directory.

### Displaying available Tags
If you have many different files, there are chances that not all of tags you want to have in output. Currently you can decide which directories you want to include in output (property `filter.allowedMetadataDirectories`) but to know what is exact name you can run Metadata Digger with special command `describe`.
Standalone:
```
sh run-standalone-metadata-digger.sh describe <path_to_config>
```
Distributed:
```
sh run-distributed-metadata-digger.sh describe <path_to_config>
```
In case of Distributed mode you should use default value for `DEPLOY_MODE` variable (`client`). This variable is set in `metadata-digger-env.sh` file. If you have experience with Spark, you probably know the difference (if not, read more [here](https://spark.apache.org/docs/latest/cluster-overview.html#glossary)) but basically in this case `client` deploy mode is better because you will have logs (including whole list of tags) visible in output of above script.

Sample output of describe action:
```
+--------------------------------------+
|Metadata Directory and Tag Name       |
+--------------------------------------+
|File Type.Detected File Type Long Name|
|File Type.Detected File Type Name     |
|File Type.Detected MIME Type          |
|File Type.Expected File Name Extension|
|Huffman.Number of Tables              |
|JFIF.Resolution Units                 |
|JFIF.Thumbnail Height Pixels          |
|JFIF.Thumbnail Width Pixels           |
|JFIF.Version                          |
|JFIF.X Resolution                     |
|JFIF.Y Resolution                     |
|JPEG.Component 1                      |
|JPEG.Component 2                      |
|JPEG.Component 3                      |
|JPEG.Compression Type                 |
|JPEG.Data Precision                   |
|JPEG.Image Height                     |
|JPEG.Image Width                      |
|JPEG.Number of Components             |
+--------------------------------------+
```

**Important!** - Names of tags could be a bit different than other tools (like Exiftool) provide. We use the same names as [Metadata-Extractor](https://drewnoakes.com/code/exif/) but additionally remove special characters like `,.":;'()`. In some cases it is useful to have tag's values in two (or three) different formats (e.g. for filtering purposes). In such case we add custom tag with `MD` prefix, for instance: `GPS.MD Location Long F` has float format like: 11.884539. The same value for original tag `GPS.GPS Longitude` will be: 11° 53' 4.34". First value could be easily compared during filtering process but second is better for human.

### Setting up mandatory tags
After getting all available tags you can decide to set mandatory fields with not empty value. To set it up you can add them
 in specific format(same as printed above e.g. 'JPEG.Component 3') in property named 'filter.mandatoryTags'.
 In case of require more than one tag, you can split them using ','.

### Finding similar images based on metadata
Let's suppose you have image with some specific set of tags that is for some reason interesting for you. You noticed that specific device or application adds those tags with such values in specific circumstances, so we can say it is kind of fingerprint of device, photo's author or author of modifications applied on image, etc. In short - you can identify someone or something by those set of tag values. 
This functionality allows you to find all images that are similar. In the simplest scenario to determine if photo A is similar to B all tags from list specified in configuration have to have the same values for A and B. However, in real life there could be some small differences between values caused by many things (e.g. geolocation - very rare situation where you have exactly the same values even if someone is in the same place). To add toleration to algorithm for such cases we have added two things:

* Converting values to types for further filtering. Currently supported types:
    * `STRING` - just text
    * `INT` - integer value between -2147483648 and 2147483647. Metadata Digger before making any comparison, removes all characters that are not numbers, `-` and `.` (to be able to treat values like `640 pixels` as integer `640`).
    * `LONG` - integer value between -9223372036854775808 and 9223372036854775807. The same cleansing process as for INT.
    * `FLOAT` - float value (e.g. for GPS locations comparison) between -3.4028235E38 and 3.4028235E38. The same cleansing process as for INT.
* Difference value for every tag. Default is 0 which means that values has to be equal to be treated as similar. However you can increase this value to make comparison algorithm more tolerant:
    * `INT`, `LONG`, `FLOAT` - here situation is very simple. Let's suppose we have a tag with number of GPS Sattelites equals `5` and we want to find also `4` and `6`. Just set difference to 1.
    * `STRING` - here you have the most complicated case because difference between two strings is calculating with [Levenshtein Distance](https://en.wikipedia.org/wiki/Levenshtein_distance). This measure shows how many changes have to be made for string A to make it B. Let's suppose you have metatag with Device Model name and it is "NIKON D500" and you want to find images made with the same model but also similar like: "NIKON D610". In such case you can set difference to 2 because you have to change two characters `50` to `61` to change value from "NIKON D500" to "NIKON D610". Keep in mind that it will also match other cases like "**R**IKON D500", "NIKO**D** D500", which in this situation are rare but still possible.

To specify list of tags that have to be compared, you have to define them with key `analytics.similar.tags` in the following format:
```
analytics.similar.tags=DIR1_NAME:TAG1_NAME:TYPE:MAX_DIFFERENCE,DIR2_NAME:TAG2_NAME:TYPE:MAX_DIFFERENCE,DIR3_NAME:TAG3_NAME:TYPE:MAX_DIFFERENCE
```
Example for Camera model and GPS location:
```
analytics.similar.tags=Exif IFD0.Model:STRING,GPS.MD Location Lat F:FLOAT:0.1,GPS.MD Location Long F:FLOAT:0.1
```
Above configuration will inform Metadata Digger to find all images where:

* Tag `Exif IFD0.Model` has string that cannot be different (default MAX_DIFFERENCE value is 0)
* GPS Location (latitude and longitude) could be different max by 0.1

There is also one additional config property: `analytics.similar.minPassingConditions`. It determines how many conditions described in `analytics.similar.tag` have to be passed to classify image as similar. By default all conditions have to be fulfilled.

When you set properties, it's time to find similar images by running one of the following commands (depends on your needs):

Standalone:
```
sh run-standalone-metadata-digger.sh find_similar <path_to_config> <path_to_base_image_file>
```
Distributed:
```
sh run-distributed-metadata-digger.sh find_similar <path_to_config> <path_to_base_image_file>
```
where <path_to_base_image_file> **has to point to image on Local File System**.

### Displaying Metadata of single file
Sometimes you just need to display Metatags for single file from Local File System. Metadata Digger provides such utility without starting Spark, so it is faster way than running `extract` action on one file. The main difference is that this action just display tags on the screen without writing to file. The action is supported only in script for Standalone mode and you do not need here configuration file:
```
sh run-standalone-metadata-digger.sh extract_single <path_to_image_file>
```

### Hash generation

There are some moments when you need to verify if different data sets contains same content. 
As a first step hashes can be generated based on a file using several algorithm.
Actually Metadata Digger supports: CRC32, MD5, SHA-1, SHA-224, SHA-256, SHA-384, SHA-512.

## Advanced settings

Metadata Digger is built as Processing Pipeline with configurable blocks called Processors. Input to the Pipeline is handled by Reader/Source and output by Writer/Sink. All things related to different input formats and types of storage are responisibility of Reader. The same is for Writer. To configure Metadata Digger with another source than Local File System, S3 for instance, you have to know which properties of Reader have to be set. When we talk about properties, parameters, configs, we mean key-value pairs placed in config.properties file which path has to be passed as an argument of Metadata Digger starting script.

### Reader configuration
First thing we have to decide before we start configuring Reader is type of storage. Currently we support the following:

* Local File System - your local disk, avoid using it in Distributed mode on multiple machines because every machine has to have access to the data and output will be misleading (multiple files spread across all servers).
* <a href="https://en.wikipedia.org/wiki/Apache_Hadoop#Hadoop_distributed_file_system" target="_blank">Hadoop Distributed File System</a> (HDFS)
* <a href="https://aws.amazon.com/s3/" target="_blank">Amazon Simple Storage Service</a> (S3)
* <a href="https://www.digitalocean.com/products/spaces/" target="_blank">Digital Ocean Spaces</a> (Spaces Object Storage)



#### Common Reader properties
The following table presents properties that are common for each type of storage.

| Property | Default | Description |
| -------- | ------- | ----------- |
| `input.paths` | | Paths to directories delimited by comma |
| `input.storage.name` | file | Name of storage |
| `input.partitions` | -1 |  Number of Spark partitions. Default value will let Spark decide how many partitions should be used and for most cases, it is recommended. To understand what is partition, you should dive a little bit more into Spark technical details but basically, partition is some part of data (e.g. content of X files loaded on input) that can be processed in parallel with other parts. Let's suppose we have 500 input files with images, 10 CPU cores reserved for calculations and we decided to use 10 partitions. Spark will divide all those files into 10 packages (they will not be ideally equal). All those partitions will be calculated in parallel, each partition per one core. It is of course simplification but generally it is how Spark works with partitions. |

#### Local File System
This is default storage and to force it for some reason you can just set `file` value to `input.storage.name` property.

#### Hadoop Distributed File System
Spark works pretty well with HDFS by default, so if you run Metadata Digger in Distributed mode on your cluster, you can set `hdfs` value to `input.storage.name` property and all your input paths will be treated as HDFS paths.
For now we do not support passing custom HDFS configuration for external Hadoop cluster. However, you can do this manually if you know Spark.

#### Amazon S3
One of the most popular Service providing Storage in Cloud. If you keep your files on S3, you can easily configure Metadata Digger to load them. First thing you should do is setting `input.storage.name` to `s3`. Read below table for other properties.


| Property | Default | Description |
| -------- | ------- | ----------- |
| `storage.s3.accessKey` |  | S3 Access Key |
| `storage.s3.secretKey` |  | S3 Secret Key |
| `storage.s3.endpoint`  |  | Endpoint including Region. In most cases it will have the following formt: *s3.[REGION].amazonaws.com*, e.g. *s3.eu-central-1.amazonaws.com*. Currently it is possible to load only from one region. |
| `storage.s3.credsProvided` | true | Flag determining if credentials (Access Key and Secret Key) are provided in main Metadata Digger config file. This is default and the easiest way. However, it is not the most secure because it can happen that credentials will be visible in internal Metadata Digger logs (on your machines of course, we are not sending anything on external servers). To configure S3 in the most secure way, you should follow Hadoop instruction (e.g. <a href="https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/spark_s3.html" target="_blank">Cloudera Guide</a>). |

Paths to particular directories on S3 should be set in the following format: *s3a://[BUCKET_NAME]/[PATH_TO_DIRECTORY]* or just: *[BUCKET_NAME]/[PATH_TO_DIRECTORY]*. **Do not use `s3://` prefix because it is the old and not supported format**.

#### Digital Ocean Spaces
Less popular but similar to S3 service providing Storage in Cloud. It is young but general idea of Digital Ocean - "*Developer Cloud - We make it simple to launch in the cloud and scale up as you grow – with an intuitive control panel, predictable pricing, team accounts, and more*" makes this service quite good place for individual OSINT researchers (if you can upload your files on cloud of course...). Setup is very quick, simple and they have API compatible with Amazon S3, so people can <a href="https://developers.digitalocean.com/documentation/spaces/" target="_blank">offically use Amazon S3 client libraries</a> to connect to Digital Ocean Spaces. If  you want to load your files from this storage, use S3 properties as follows:


| Property | Default | Description |
| -------- | ------- | ----------- |
| `storage.s3.accessKey` |  | Digital Ocean Spaces Access Key |
| `storage.s3.secretKey` |  | Digital Ocean Spaces Secret Key |
| `storage.s3.endpoint`  |  | Endpoint including Region. In most cases it will have the following formt: *https://[REGION].digitaloceanspaces.com*, e.g. *https://nyc3.digitaloceanspaces.com*. Currently it is possible to load only from one region. |
| `storage.s3.credsProvided` | true | Flag determining if credentials (Access Key and Secret Key) are provided in main Metadata Digger config file. This is default and the easiest way. However, it is not the most secure because it can happen that credentials will be visible in internal Metadata Digger logs (on your machines of course, we are not sending anything on external servers). To configure Digital Ocean Spaces in the most secure way, you should follow Hadoop instruction for S3 (e.g. <a href="https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/spark_s3.html" target="_blank">Cloudera Guide</a>). |

Paths to particular directories on Spaces should be set in the following format: *s3a://[BUCKET_NAME]/[PATH_TO_DIRECTORY]* or just: *[BUCKET_NAME]/[PATH_TO_DIRECTORY]*. **Do not use `s3://` prefix because it is the old and not supported format**.

### Processing configuration
Processing part contains all actions between Reader (loading data) and Writer (saving). Below table explains possible properties that allow you to adjust final result.

| Property | Default | Description |
| -------- | ------- | ----------- |
| `filter.allowedMetadataDirectories` | * | Comma delimited list of Tags' Directories (Groups), e.g. GPS, JPEG that will be included in output. **Metadata are categorized into groups called Directories** and using this property you can specify which one you want to have. By default all of them will be included. |
| `output.columns.includeDirsInTags` | true | Applicable only for flat output structures like CSV or Solr. Flag determining if final tag name should include Directory name at the beginning or not. In most cases it is recommended to set this value to `true` because there are chances that two different Directories contains tag with the same name and in such case Metadata Digger will stop working with error: TheSameTagNamesException. |
| `output.columns.metadataPrefix` |  | Prefix that will be added to all tag names. Final output contains some additional fields like file path, so adding prefix to tag colums will be helpful in selecting only metadata from output in your system. **Prefix will be added only in case of flat structures like CSV or Solr**. JSON output contains nested structure where metadata fields have separated object so it does not make sense to add prefix in such case. |
| `output.columns.namingConvention` | camelCase | Naming convention that will be applied on all output field/column names. Possible values: `camelCase` (e.g. "GPS Latitude" field will be converted to "GPSLatitute"), `snakeCase` (e.g. "GPS Latitude" to "gps_latitude"). |
| `output.columns.includeMetadataContent` | false | If true, add column containing concatenation of all tag values. It is useful when you want to have separated field for index/search purposes. |
| `processing.cores` | [available cores - 1] | Number detrmining how many cores will be used for whole processing. If you do not set it, Metadata Digger will retrieve how many cores your machine has and left one core free. **This property is used only in Standalone mode**. |
| `processing.maxMemoryGB` | 2 | How many memory should be reserved for processing (in GB). If you receive errors in logs like this: *"OutOfMemory: Java heap space"* or *"GC Overhead Limit Exceeded Error"*, you should try to increase this value but remember to left some memory. You should check your total RAM before you set this property. **This property is used only in Standalone mode**. |
| `processing.hash.types` |  | Comma delimited list of hashes which will be generated based on file. Actually supporting methods: `crc32`, `md5`, `sha1`, `sha224`, `sha256`, `sha384`, `sha512`. |


### Writer configuration
Writer contains similar properties to Reader. The first one we have to set is `output.storage.name`. Currently we support the following:

* Local File System - your local disk, avoid using it in Distributed mode on multiple machines because every machine has to have access to the data and output will be misleading (multiple files spread across all servers).
* <a href="https://en.wikipedia.org/wiki/Apache_Hadoop#Hadoop_distributed_file_system" target="_blank">Hadoop Distributed File System</a> (HDFS)
* <a href="https://aws.amazon.com/s3/" target="_blank">Amazon Simple Storage Service</a> (S3)
* <a href="https://www.digitalocean.com/products/spaces/" target="_blank">Digital Ocean Spaces</a> (Spaces Object Storage)
* [Apache Solr](http://lucene.apache.org/solr/) - Full Text Search engine.

#### Common Files Writer properties
The following table presents properties that are common for all files' types of storage.

| Property | Default | Description |
| -------- | ------- | ----------- |
| `output.directoryPath` | | Path to directory where all output files will be written. Spark uses this directory also to write temporary files (like status of operations), so to avoid potential issues **Spark will not start processing if the directory exist**.  |
| `output.storage.name` | file | Name of storage |
| `output.format` |  | Format of output file(s). Available values: `json`, `csv`. |
| `output.filesNumber` | -1 |  Number of output files. Default value will be equal to the number of Spark partitions (see *Reader configuration* section for more information about partitions) because each partition is processing separately by Spark and writes output to file. If you want to have specific number of output files, you can set this value to some number more than 0. However, keep in mind that in case of huge volumes of data there are chances that application **fails due to memory issues**. Let's suppose we set `output.filesNumber` to 1. Spark has to move results of processing from all servers to one machine and then write result to file. This is memory expensive operation and can also slow down whole process if network in your cluster is not very fast. **For demo purposes** we have provided configuration in Standalone mode that instructs application to write result to single file by default. |

Spark writes empty _SUCCESS file to output directory after successfull completion of job. 
Names of actual output files consists of numbers. It is not very human readable, so we decided to add mechanism (**currently only for Local File System in Standalone mode**) that will change those names to use name of output directory, so if you set `output.directoryPath` to */some/path/to/my_output*, `output.filesNumber` to *2* and `output.format` to *csv*, Metadata Digger will remove all temporary files and produce the following:

* `/some/path/to/my_output/my_output_1.csv`
* `/some/path/to/my_output/my_output_2.csv`


#### Local File System
This is default storage and to force it for some reason you can just set `file` value to `output.storage.name` property.

#### Hadoop Distributed File System
Spark works pretty well with HDFS by default, so if you run Metadata Digger in Distributed mode on your cluster, you can set `hdfs` value to `output.storage.name` property and all your output paths will be treated as HDFS paths.
For now we do not support passing custom HDFS configuration for external Hadoop cluster. However, you can do this manually if you know Spark.

#### Amazon S3
Currently we support only case when Reader and Writer use the same S3 credentials and endpoint. It does not mean you have to use S3 for Reader and Writer but you cannot use different S3 configurtion to load data and to write. Please read section: *Reader configuration/Amazon S3* because it is almost the same for Writer. One thing different is that you should set to `s3` property `output.storage.name` instead of `input.storage.name`.

#### Digital Ocean Spaces
Currently we support only case when Reader and Writer use the same Digital Ocean Spaces credentials and endpoint. It does not mean you have to use Digital Ocean Spaces for Reader and Writer but you cannot use different configurtion to load data and to write. Please read section: *Reader configuration/Digital Ocean Spaces* because it is almost the same for Writer. One thing different is that you should set to `s3` property `output.storage.name` instead of `input.storage.name`.

#### Apache Solr
According to [official Solr site](http://lucene.apache.org/solr/): *"Solr is highly reliable, scalable and fault tolerant, providing distributed indexing, replication and load-balanced querying, automated failover and recovery, centralized configuration and more. Solr powers the search and navigation features of many of the world's largest internet sites. "*. In simple words, it is system that allows for effective text search. Solr has many builtin mechanisms like searching by synonyms, returning similar documents, advanced filtering and grouping, graph queries etc. It is stable solution (over 15 years) and currently it is common practice to use it with Hadoop ecosystem. We have added support for Solr mostly because we are developing our own Web Application that uses Solr as main backend Full-Text Search engine. If you want to use our Solr Schema, you can get it from [metadata-digger-deployment repository](https://github.com/data-hunters/metadata-digger-deployment/tree/master/dev/solrcloud/solr-conf/conf).
If you want to write result to Solr, you have to set `output.storage.name` to `solr` and configure the following properties:

| Property | Default | Description |
| -------- | ------- | ----------- |
| `output.collection` |  | Name of Solr collection when results will be written. |
| `output.zk.servers` |  | List of Solr ZooKeeper servers, e.g. *host1.com:2181,host2.com:2181*. |
| `output.format` |  | Use `solr` |
| `output.zk.znode` |  | ZooKeeper ZNode that keeps Solr configuration. Leave empty if you keep Solr data in ZooKeeper root. |
| `output.solr.conversion.integerTags` |  | List of tag names that has to be converted into integers to adjust output to Solr Schema. If you use Metadata Digger Solr Schema, use the following value: `md_jpeg_image_width,md_jpeg_image_height,md_exif_subifd_exif_image_width,md_exif_subifd_exif_image_height,md_gps_gps_satellites`. |
| `output.solr.conversion.dateTimeTags` |  | List of tag names that has to be converted into Solr date time format. If you use Metadata Digger Solr Schema use the following: `md_exif_ifd0_datetime,md_icc_profile_profile_datetime,md_gps_datetime,md_exif_subifd_datetime_original`. |

Metadata Digger sends commit request immediately after indexing all results to Solr.

## External dependencies
We use the following libraries in our application:

* <a href="https://spark.apache.org/" target="_blank">Apache Spark</a> - Apache License 2.0
* <a href="https://drewnoakes.com/code/exif/" target="_blank">Metadata Extractor</a> - Apache License 2.0
* <a href="https://github.com/apache/lucene-solr/" target="_blank">SolrJ</a> - Apache License 2.0
* <a href="https://github.com/apache/hadoop/" target="_blank">Apache Hadoop</a> - Apache License 2.0
* <a href="https://github.com/aws/aws-sdk-java/" target="_blank">AWS SDK for Java</a> - Apache License 2.0
* Other common libraries for Scala, see built.sbt for details

Please read documentation of particular dependencies to check details about licenses and used libraries.


## Logo
Metadata Digger logo has been created by Dominik Koślin. Thanks Domin!
<br />
<br />
<br />
[![DataHunters](http://datahunters.ai/assets/images/logo_full_small.png)](http://datahunters.ai)
