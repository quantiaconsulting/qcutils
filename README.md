# qcutils
Python utils for educational project

## Build the distribution

`python3 setup.py sdist bdist_wheel`

## Available utils

### `read_config_value`

**Signature**: `read_config_value(key,cf_path)`

**Description**: Read, from a yaml-style config file, the value related to the `key`

**Args**:

* `key` - `String` : the key of the configuration you nned to extract
	* Required: yes 
* `cf_path` - `String` : absolute path of the configuration file 
	* Required: no  
	* default value: `/home/jovyan/utils/config.yaml`

### init_spark_shell

**Signature**: `init_spark_shell(java_sdk_vrs, hadoop_aws_vrs)`

**Description**: Initialize the pyspark shell at startup in order to import libs to interact with S3 using the s3a filesystem

**Args**:

* `java_sdk_vrs`  - `String`: the version of `aws-java-sdk` from `com.amazonaws` repository
	* Required: yes 
* `hadoop_aws_vrs` - `String`: the version of `hadoop-aws` from `org.apache.hadoop` repository
	* Required: yes  

### `init_spark_session`

**Signature**: `init_spark_session(spark_session, cf_path)`

**Description**: Initialize an already existing SparkSession with the information to read from S3 using the s3a filesystem

**Args**:

* `spark_session`  - `SparkSession`: the key of the configuration you nned to extract
	* Required: yes 
* `cf_path` - `String`: absolute path of the configuration file 
	* Required: no  
	* default value: `/home/jovyan/utils/config.yaml`

### `kafka_srv_description`

**Signature**: `kafka_srv_description(cf_path)`

**Description**: Show a table with all the kafka services available in the environment

**Args**:

* `cf_path` - `String`: absolute path of the configuration file 
	* Required: no  
	* default value: `/home/jovyan//utils/config.yaml`

### `create_kafka_topic`

**Signature**: `create_kafka_topic(topic,cf_path,partitions=4,replication=3)`

**Description**: Create a new kafka topic on a kafka broker. The information related to kafka is stored in the config file.

**Args**:

* `topic` - `String`: name of the new kafka topic you want to create
	* Required: yes 
* `security` - `boolean`: flag: if `True`, the server is secured via SASL protocol and you need to specify `sasl.username` and `sasl.password` in the configuration file.
	* Required: no  
	* default value: `False`
* `cf_path` - `String`: absolute path of the configuration file 
	* Required: no  
	* default value: `/home/jovyan/utils/config.yaml`
* `partitions` - `Integer`: number of partitions of the new topic
	* Required: no  
	* default value: 3
* `replication` - `Integer`: number of replicas for the new topic
	* Required: no  
	* default value: 4


### `list_s3_bucket_objects`

**Signature**: `list_s3_bucket_objects(bucket_name='quantia-master', prefix='training', limit=10)`

**Description**: List objects in a S3 bucket and folder

**Args**:

* `bucket_name` - `String`: name of the S3 bucket
	* Required: no  
	* default value: `quantia-master` 
* `prefix` - `String`: prefix of the object to list. It can be as complex as you want. 
	* Required: no  
	* default value: `training `
* `limit` - `Integer`: max number of object to show
	* Required: no  
	* default value: 10

### `print_s3_bucket_object`

**Signature**: `print_s3_bucket_object(key, bucket_name='quantia-master', size=1000, decode=True)`

**Description**: Print an objects in a S3 bucket

**Args**:

* `key` - `String`: path of the object
	* Required: yes  
* `bucket_name` - `String`: name of the S3 bucket
	* Required: no  
	* default value: `quantia-master` 
* `size` - `Integer`: number of bytes to be printed 
	* Required: no  
	* default value: 1000
* `decode` - `Integer`: decode the streaming object using UTF-8
	* Required: no  
	* default value: true
