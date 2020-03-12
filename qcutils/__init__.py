import yaml
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError

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

def create_kafka_topic(conf, topic, partitions=4,replication=1):
    
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