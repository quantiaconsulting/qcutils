import yaml
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError
import boto3
import io
import s3fs

def init_session(spark_session, cf_path = "/home/jovyan/materials/utils/config.yaml"):

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

def init_spark_session(spark_session, cf_path = "/home/jovyan/materials/utils/config.yaml"):

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

def create_kafka_topic(conf, topic, partitions=4,replication=3):
    
    a = AdminClient(conf)
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
