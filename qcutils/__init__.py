import yaml
import boto3
import io
import s3fs
from IPython.display import Markdown, display
import os
import config_with_yaml as config

#Generic utils

def read_config_value(key,cf_path = "/home/jovyan/utils/config.yaml"):
    cfg = config.load(cf_path)
    return cfg.getProperty(key)

# Spark utils

def init_spark_shell(java_sdk_vrs, hadoop_aws_vrs):
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--packages com.amazonaws:aws-java-sdk:{},org.apache.hadoop:hadoop-aws:{} pyspark-shell'
        .format(java_sdk_vrs, hadoop_aws_vrs))
    display(Markdown("**PySpark-Shell Up and Running**"))
   
    return

def init_spark_session(spark_session, cf_path = "/home/jovyan/utils/config.yaml"):

    with open(cf_path, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
    #Read the AWS key and secret from cofiguration file
    aws_key = cfg['aws']['access']['key']
    aws_secret = cfg['aws']['access']['secret']
    
    #Set-up the hadoop configuration to enable s3a filesystem
    hadoop_conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", aws_key)
    hadoop_conf.set("fs.s3a.secret.key", aws_secret)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    #Enable better dataframe visualiztion in jupyter
    spark_session.conf.set("spark.sql.repl.eagerEval.enabled", True)
    
    return

# Kafka utils

def kafka_srv_description(cf_path = "/home/jovyan/utils/config.yaml"):

    from tabulate import tabulate
    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError

    try:
        kconf_bkey = 'kafka'

        ksrv = read_config_value(key="{}.server".format(kconf_bkey), cf_path=cf_path)
        ksrvp = str(read_config_value(key="{}.port".format(kconf_bkey), cf_path=cf_path))

        z = read_config_value(key="{}.zookeper".format(kconf_bkey), cf_path=cf_path)

        sr_temp=read_config_value("{}.schema_registry.url".format(kconf_bkey), cf_path=cf_path).split(':')
        sr=sr_temp[0]+':'+sr_temp[1]
        srp=sr_temp[2]

        l = [["Kafka", ksrv, ksrvp], ["Zookeeper", z, "-"], ["Schema Registry", sr, srp]]
        table = tabulate(l, headers=['Service', 'Address', 'Port'], tablefmt='pretty')
        print(table)
    except:
        print("No kafka service available")

def create_kafka_topic(topic, security=False, cf_path = "/home/jovyan/utils/config.yaml", partitions=1,replication=1):

    from confluent_kafka.admin import AdminClient, NewTopic
    from confluent_kafka import KafkaError
    kconf_bkey = 'kafka'
    servers=read_config_value(key="{}.server".format(kconf_bkey), cf_path=cf_path) + ":" + str(read_config_value(key="{}.port".format(kconf_bkey), cf_path=cf_path))
    
    if security:
        username=read_config_value(key="{}.access.key".format(kconf_bkey), cf_path=cf_path)
        password=read_config_value(key="{}.access.secret".format(kconf_bkey), cf_path=cf_path)

        adminconf = {
                'bootstrap.servers': servers,
                'sasl.mechanisms': 'PLAIN',
                'security.protocol': 'SASL_SSL',
                'sasl.username': username,
                'sasl.password': password 
                }
    else:
        adminconf = {
                'bootstrap.servers': servers
                }

    a = AdminClient(adminconf)
    fs = a.create_topics([NewTopic(
         topic,
         num_partitions=partitions,
         replication_factor=replication
    )])

    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            # Continue if error code TOPIC_ALREADY_EXISTS, which may be true
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                print("Failed to create topic {}: {}".format(topic, e))
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print("{}".format(e))


# S3 utils

def list_s3_bucket_objects(bucket_name='quantia-master', prefix='training', limit=10):
    s3 = boto3.client('s3')
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix =prefix)

    for obj in objects.get('Contents')[:limit]:
        print(obj.get('Key')) 

def print_s3_bucket_object(key, bucket_name='quantia-master', size=1000, decode=True):
    s3 = boto3.client('s3')
    obj = s3.get_object(
        Bucket=bucket_name, 
        Key=key)

    if decode:
        body = obj.get('Body').read(size).decode(encoding="utf-8",errors="ignore")
    else:
        body = obj.get('Body').read(size)
    
    print(body)
